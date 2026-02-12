"""Docker utility functions for benchmark orchestration."""

import subprocess
from pathlib import Path
from typing import Dict, List, Optional


def image_exists(image: str) -> bool:
    """Check if a Docker image exists locally."""
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
        text=True
    )
    return result.returncode == 0


def build_image(
    image: str,
    dockerfile: Path,
    context: Path,
    force: bool = False
) -> bool:
    """
    Build Docker image if needed.

    Args:
        image: Image name/tag
        dockerfile: Path to Dockerfile
        context: Build context directory
        force: Force rebuild even if image exists

    Returns:
        True if image was built, False if skipped
    """
    if not force and image_exists(image):
        print(f"Image {image} already exists (use --build to force rebuild)")
        return False

    print(f"Building Docker image: {image}")
    print(f"  Dockerfile: {dockerfile}")
    print(f"  Context: {context}")

    result = subprocess.run(
        ["docker", "build", "-f", str(dockerfile), "-t", image, str(context)],
        check=True
    )

    print(f"✓ Image {image} built successfully")
    return True


def run_container(
    image: str,
    args: List[str],
    volumes: Dict[str, str],
    name: Optional[str] = None,
    detach: bool = False
) -> str:
    """
    Run a command in a Docker container.

    Args:
        image: Image name/tag
        args: Command arguments
        volumes: Dict mapping host paths to container paths
        name: Optional container name
        detach: Run in detached mode

    Returns:
        Container ID if detached, empty string otherwise
    """
    cmd = ["docker", "run", "--rm"]

    if name:
        cmd.extend(["--name", name])

    if detach:
        cmd.append("-d")

    for host_path, container_path in volumes.items():
        cmd.extend(["-v", f"{host_path}:{container_path}"])

    cmd.append(image)
    cmd.extend(args)

    result = subprocess.run(cmd, capture_output=detach, text=True, check=True)

    if detach:
        return result.stdout.strip()
    return ""


def copy_from_container(container_id: str, src: str, dst: Path) -> None:
    """
    Copy files from container to host.

    Args:
        container_id: Container ID or name
        src: Source path in container
        dst: Destination path on host
    """
    subprocess.run(
        ["docker", "cp", f"{container_id}:{src}", str(dst)],
        check=True
    )


def get_container_logs(container_id: str) -> str:
    """Get logs from a container."""
    result = subprocess.run(
        ["docker", "logs", container_id],
        capture_output=True,
        text=True,
        check=True
    )
    return result.stdout


def wait_for_container(container_id: str) -> int:
    """Wait for container to finish and return exit code."""
    result = subprocess.run(
        ["docker", "wait", container_id],
        capture_output=True,
        text=True,
        check=True
    )
    return int(result.stdout.strip())
