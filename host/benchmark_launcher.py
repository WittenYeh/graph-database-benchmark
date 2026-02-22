#!/usr/bin/env python3
"""
BenchmarkLauncher - Main entry point for graph database benchmarking
"""
import argparse
import json
import os
import sys
import requests
from pathlib import Path
from typing import List, Dict, Any, Optional

from compiler.workload_compiler import WorkloadCompiler
from db.docker_manager import DockerManager
from report.report_generator import ReportGenerator
from dataset.dataset_loader import DatasetLoader
from progress_server import ProgressServer


class BenchmarkLauncher:
    def __init__(self, args):
        self.args = args
        self.database_name = args.database_name
        self.database_config = self._load_json(args.database_config)
        self.dataset_config = self._load_json(args.dataset_config)
        self.workload_config = self._load_json(args.workload_config)
        self.output_dir = Path(args.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.docker_manager = DockerManager(self.database_config, args.rebuild)
        self.workload_compiler = WorkloadCompiler(self.database_config)
        self.dataset_loader = DatasetLoader(self.dataset_config)

        # Store current container for timeout handling
        self.current_container = None

    def _load_json(self, filepath: str) -> Dict[str, Any]:
        """Load JSON configuration file"""
        with open(filepath, 'r') as f:
            return json.load(f)

    def _get_datasets_to_test(self) -> List[str]:
        """Get list of datasets to test"""
        if self.args.dataset_name:
            return self.args.dataset_name if isinstance(self.args.dataset_name, list) else [self.args.dataset_name]

        # Dataset must be specified via CLI
        print("❌ Error: No dataset specified. Use --dataset-name to specify dataset(s)")
        sys.exit(1)

    def _get_workloads_to_test(self) -> List[str]:
        """Get list of workload configs to test"""
        if self.args.workload_name:
            return [self.args.workload_name]
        return [self.args.workload_config]

    def run(self):
        """Main execution flow"""
        print(f"🚀 Starting benchmark for database: {self.database_name}")

        # Get database configuration
        if self.database_name not in self.database_config:
            print(f"❌ Error: Database '{self.database_name}' not found in configuration")
            sys.exit(1)

        db_config = self.database_config[self.database_name]

        # Get workload name and mode from config
        workload_name = self.workload_config.get('name', 'unnamed_workload')
        mode = self.workload_config.get('mode', 'structural')
        print(f"📋 Workload: {workload_name} (mode: {mode})")

        # Define timeout callback
        def handle_timeout(subtask_data: Dict[str, Any]):
            """Handle subtask timeout - stop container to abort execution"""
            print(f"    ❌ Subtask '{subtask_data.get('task_name')}' TIMEOUT!")
            print(f"    🛑 Stopping container to abort execution...")

            # Stop the container immediately to abort the hanging HTTP request
            if self.current_container:
                try:
                    self.current_container.stop(timeout=5)
                    print(f"    ✓ Container stopped due to timeout")
                except Exception as e:
                    print(f"    ⚠️  Failed to stop container: {e}")

        # Start progress server with timeout callback
        progress_server = ProgressServer(port=8888, timeout_callback=handle_timeout)
        progress_server.start()
        callback_url = progress_server.get_callback_url()

        try:
            # Build/rebuild Docker image if needed
            print(f"\n📦 Preparing Docker image: {db_config['docker_image']}")
            self.docker_manager.prepare_image(self.database_name, self.args.rebuild)

            # Get datasets to test
            datasets = self._get_datasets_to_test()

            for dataset_name in datasets:
                print(f"\n📊 Testing dataset: {dataset_name}")

                # Get dataset path
                dataset_path = self.dataset_loader.get_dataset_path(dataset_name)
                if not dataset_path:
                    print(f"⚠️  Warning: Dataset '{dataset_name}' not found, skipping...")
                    continue

                # Compile workload to database-specific queries
                print(f"⚙️  Compiling workload...")
                compiled_dir = self.workload_compiler.compile_workload(
                    self.workload_config,
                    self.database_name,
                    dataset_name,
                    self.args.seed,
                    dataset_path
                )

                # Start Docker container and run benchmark
                print(f"🐳 Starting Docker container: {db_config['container_name']}")
                container = self.docker_manager.start_container(
                    self.database_name,
                    dataset_path,
                    compiled_dir,
                    callback_url,
                    mode=mode
                )

                # Store container for timeout handling
                self.current_container = container

                try:
                    # Execute benchmark tasks
                    print(f"⏱️  Executing benchmark tasks...")
                    results = self.docker_manager.execute_benchmark(
                        container,
                        self.workload_config,
                        dataset_name,
                        dataset_path,
                        callback_url
                    )

                    # Add workload name to metadata
                    if 'metadata' not in results:
                        results['metadata'] = {}
                    results['metadata']['workload'] = workload_name

                    # Save results
                    output_file = self.output_dir / f"bench_{self.database_name}_{dataset_name}_{workload_name}.json"
                    with open(output_file, 'w') as f:
                        json.dump(results, f, indent=2)

                    print(f"✅ Results saved to: {output_file}")

                except (requests.exceptions.RequestException, Exception) as e:
                    # Handle timeout or other errors
                    error_type = type(e).__name__
                    print(f"❌ Benchmark execution failed: {error_type}")
                    if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                        print(f"⚠️  This was likely caused by a subtask timeout.")
                        print(f"⚠️  Partial results may have been lost.")
                    else:
                        print(f"⚠️  Error details: {e}")

                finally:
                    # Clear current container reference
                    self.current_container = None

                    # Stop and remove container
                    print(f"🛑 Stopping container...")
                    self.docker_manager.stop_container(container)

            print(f"\n🎉 Benchmark completed!")

        finally:
            # Stop progress server
            progress_server.stop()


def main():
    parser = argparse.ArgumentParser(
        description='Graph Database Benchmark Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--database-name', required=True,
                        choices=['neo4j', 'janusgraph', 'arangodb', 'orientdb', 'aster', 'sqlg'],
                        help='Database to benchmark')

    parser.add_argument('--database-config', default='config/database-config.json',
                        help='Path to database configuration file')

    parser.add_argument('--dataset-config', default='config/datasets.json',
                        help='Path to dataset configuration file')

    parser.add_argument('--dataset-name', nargs='+', required=True,
                        help='Dataset name(s) to test')

    parser.add_argument('--workload-config', default='workloads/templates/example_workload.json',
                        help='Path to workload configuration file')

    parser.add_argument('--workload-name',
                        help='Specific workload name to run')

    parser.add_argument('--output-dir', default='reports',
                        help='Output directory for benchmark reports')

    parser.add_argument('--seed', type=int,
                        help='Random seed for reproducibility')

    parser.add_argument('--rebuild', action='store_true',
                        help='Force rebuild the Docker image')

    args = parser.parse_args()

    # Create and run launcher
    launcher = BenchmarkLauncher(args)
    launcher.run()


if __name__ == '__main__':
    main()
