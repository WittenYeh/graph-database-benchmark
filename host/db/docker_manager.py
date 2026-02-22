"""
DockerManager - Manages Docker containers for benchmark execution
"""
import docker
import json
import time
import requests
from pathlib import Path
from typing import Dict, Any, Optional


class DockerManager:
    def __init__(self, database_config: Dict[str, Any], rebuild: bool = False):
        self.database_config = database_config
        self.rebuild = rebuild
        self.client = docker.from_env()

    def prepare_image(self, database_name: str, rebuild: bool = False):
        """Build or pull Docker image"""
        db_config = self.database_config[database_name]
        image_name = db_config['docker_image']
        dockerfile_path = Path(db_config['dockerfile_path'])

        # Check if image exists
        try:
            self.client.images.get(image_name)
            if not rebuild:
                print(f"✓ Image {image_name} already exists")
                return
            print(f"🔨 Rebuilding image {image_name}...")
        except docker.errors.ImageNotFound:
            print(f"🔨 Building image {image_name}...")

        # Build image
        # Build from project root to access common-java
        project_root = Path(__file__).parent.parent.parent
        self.client.images.build(
            path=str(project_root),
            dockerfile=str(dockerfile_path),
            tag=image_name,
            rm=True,
            forcerm=True
        )
        print(f"✓ Image {image_name} built successfully")

    def start_container(
        self,
        database_name: str,
        dataset_path: Path,
        compiled_workload_dir: Path,
        progress_callback_url: str = None,
        mode: str = 'structural'
    ):
        """Start Docker container with mounted volumes"""
        db_config = self.database_config[database_name]
        image_name = db_config['docker_image']
        container_name = db_config['container_name']
        api_port = db_config['api_port']

        # Stop and remove existing container if exists
        try:
            old_container = self.client.containers.get(container_name)
            old_container.stop()
            old_container.remove()
        except docker.errors.NotFound:
            pass

        # Get project root directory (parent of host directory)
        project_root = Path(__file__).parent.parent.parent.absolute()

        # Determine DB_TYPE based on mode
        db_type = database_name if mode == 'structural' else f"{database_name}-property"

        # Build environment variables
        env_vars = {
            'DATASET_FILE': f'/workspace/{dataset_path.relative_to(project_root)}',
            'WORKLOAD_DIR': '/data/workloads',
            'HEAP_SIZE': db_config['config'].get('heap_size', '4G'),
            'API_PORT': str(api_port),
            'DB_TYPE': db_type
        }

        # Add progress callback URL if provided
        if progress_callback_url:
            env_vars['PROGRESS_CALLBACK_URL'] = progress_callback_url

        # Start new container with host network mode
        # Using host network allows container to access localhost:8888 directly
        container = self.client.containers.run(
            image_name,
            name=container_name,
            detach=True,
            network_mode='host',  # Use host network to access progress server
            volumes={
                str(project_root): {'bind': '/workspace', 'mode': 'ro'},
                str(compiled_workload_dir.absolute()): {'bind': '/data/workloads', 'mode': 'ro'}
            },
            environment=env_vars
        )

        # Wait for container to be ready
        print(f"⏳ Waiting for container to be ready...")
        self._wait_for_container_ready(api_port)
        print(f"✓ Container ready")

        return container

    def _wait_for_container_ready(self, port: int, timeout: int = 60):
        """Wait for container API to be ready"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f'http://localhost:{port}/health', timeout=2)
                if response.status_code == 200:
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(1)
        raise TimeoutError(f"Container did not become ready within {timeout} seconds")

    def execute_benchmark(
        self,
        container,
        workload_config: Dict[str, Any],
        dataset_name: str,
        dataset_path: Path,
        callback_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute benchmark and collect results"""
        db_config = self.database_config[container.name.replace('-benchmark', '')]
        api_port = db_config['api_port']

        # Get project root directory
        project_root = Path(__file__).parent.parent.parent.absolute()

        # Convert dataset_path to container path
        container_dataset_path = f'/workspace/{dataset_path.relative_to(project_root)}'

        # Prepare request payload
        payload = {
            'dataset_name': dataset_name,
            'dataset_path': container_dataset_path,
            'server_threads': workload_config.get('server_config', {}).get('threads', 8)
        }

        # Add callback URL if provided
        if callback_url:
            payload['callback_url'] = callback_url

        # Trigger benchmark execution
        response = requests.post(
            f'http://localhost:{api_port}/execute',
            json=payload,
            timeout=3600  # 1 hour timeout
        )

        if response.status_code != 200:
            raise RuntimeError(f"Benchmark execution failed: {response.text}")

        results = response.json()
        return results

    def stop_container(self, container):
        """Stop and remove container"""
        try:
            container.stop(timeout=10)
            container.remove()
        except Exception as e:
            print(f"⚠️  Warning: Failed to stop container: {e}")
