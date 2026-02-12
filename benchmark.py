#!/usr/bin/env python3
"""
Simplified benchmark orchestrator using the unified CLI.

This script builds Docker images and runs benchmarks for specified databases.
Results are automatically copied from the container to the host.
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

import docker_utils

PROJECT_ROOT = Path(__file__).resolve().parent
SUPPORTED_DATABASES = ["neo4j", "janusgraph"]


def log(msg: str):
    """Print a timestamped log message."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def run_benchmark(args):
    """Run unified benchmark for a specific database."""
    database = args.database_name

    if database not in SUPPORTED_DATABASES:
        log(f"ERROR: Unsupported database '{database}'. Supported: {SUPPORTED_DATABASES}")
        sys.exit(1)

    # Determine image name and Dockerfile
    image_name = f"graphdb-benchmark-{database}"
    dockerfile = PROJECT_ROOT / f"{database}-benchmark" / f"Dockerfile.{database}"

    if not dockerfile.exists():
        log(f"ERROR: Dockerfile not found: {dockerfile}")
        sys.exit(1)

    log("=" * 60)
    log(f"Graph Database Benchmark - {database}")
    log("=" * 60)
    log(f"  Database: {database}")
    log(f"  Database Config: {args.database_config}")
    log(f"  Workload Config: {args.workload_config}")
    log(f"  Dataset Config: {args.dataset_config}")
    log(f"  Dataset Name: {args.dataset_name or '(from config)'}")
    log(f"  Workload Name: {args.workload_name or '(all workloads)'}")
    log(f"  Output: {args.output_dir}")
    log("=" * 60)

    # Build Docker image
    log(f"\n[1/3] Building Docker image: {image_name}")
    docker_utils.build_image(
        image=image_name,
        dockerfile=dockerfile,
        context=PROJECT_ROOT,
        force=args.rebuild
    )

    # Prepare volume mounts
    graph_datasets = PROJECT_ROOT / "graph-datasets"
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Resolve config file paths
    database_config = Path(args.database_config).resolve()
    dataset_config = Path(args.dataset_config).resolve()
    workload_config = Path(args.workload_config).resolve()

    if not workload_config.exists():
        log(f"ERROR: Workload config not found: {workload_config}")
        sys.exit(1)

    volumes = {
        str(graph_datasets): "/app/graph-datasets",
        str(database_config): "/app/database-config.json",
        str(dataset_config): "/app/datasets.json",
        str(PROJECT_ROOT / "workloads"): "/app/workloads",
        str(output_dir): "/app/reports",
    }

    # Build benchmark command
    cmd = [
        "benchmark",
        "--database-config", "/app/database-config.json",
        "--database-name", database,
        "--workload-config", f"/app/workloads/{workload_config.name}",
        "--dataset-config", "/app/datasets.json",
        "--db-path", f"/data/db/{database}",
        "--result-dir", "/app/reports",
    ]

    if args.dataset_name:
        cmd.extend(["--dataset-name", args.dataset_name])
    if args.workload_name:
        cmd.extend(["--workload-name", args.workload_name])
    if args.seed is not None:
        cmd.extend(["--seed", str(args.seed)])

    # Run benchmark
    log(f"\n[2/3] Running benchmark in Docker container")
    log(f"  Command: {' '.join(cmd)}")

    try:
        docker_utils.run_container(
            image=image_name,
            args=cmd,
            volumes=volumes
        )
    except Exception as e:
        log(f"ERROR: Benchmark failed: {e}")
        sys.exit(1)

    # Results are already in the mounted volume, no need to copy
    log(f"\n[3/3] Benchmark complete")
    log(f"  Results written to: {output_dir}")

    # List generated reports
    reports = sorted(output_dir.glob(f"bench_{database}_*.json"), reverse=True)
    if reports:
        log(f"  Latest report: {reports[0].name}")

    log("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Graph Database Benchmark - Docker Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build and run Neo4j benchmark
  python benchmark.py --database-name neo4j --rebuild

  # Run JanusGraph benchmark with custom config
  python benchmark.py --database-name janusgraph --workload-config workloads/my_config.json

  # Run with specific dataset and seed
  python benchmark.py --database-name neo4j --dataset-name coAuthorsDBLP --seed 42

Docker Best Practice:
  This benchmark is designed to run in Docker for reproducibility and isolation.
  Each database runs in its own container with isolated storage.
        """,
    )

    parser.add_argument(
        "--database-config",
        default="database-config.json",
        help="Path to database-config.json (default: database-config.json)",
    )
    parser.add_argument(
        "--database-name",
        required=True,
        choices=SUPPORTED_DATABASES,
        help="Database to benchmark (neo4j or janusgraph)",
    )
    parser.add_argument(
        "--dataset-config",
        default="datasets.json",
        help="Path to datasets.json (default: datasets.json)",
    )
    parser.add_argument(
        "--dataset-name",
        help="Dataset name (overrides config file)",
    )
    parser.add_argument(
        "--workload-config",
        default="workloads/workload_config.json",
        help="Path to workload_config.json (default: workloads/workload_config.json)",
    )
    parser.add_argument(
        "--workload-name",
        help="Workload name (optional, for filtering specific workloads)",
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Output directory for benchmark reports (default: reports/)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducible workload generation",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Force rebuild the Docker image",
    )

    args = parser.parse_args()
    run_benchmark(args)


if __name__ == "__main__":
    main()
