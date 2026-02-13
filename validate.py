#!/usr/bin/env python3
"""
Validation script to check project structure and dependencies
"""
import sys
import os
from pathlib import Path


def check_file_exists(filepath, description):
    """Check if a file exists"""
    if Path(filepath).exists():
        print(f"✓ {description}: {filepath}")
        return True
    else:
        print(f"✗ {description}: {filepath} NOT FOUND")
        return False


def check_directory_exists(dirpath, description):
    """Check if a directory exists"""
    if Path(dirpath).is_dir():
        print(f"✓ {description}: {dirpath}")
        return True
    else:
        print(f"✗ {description}: {dirpath} NOT FOUND")
        return False


def validate_project_structure():
    """Validate the project structure"""
    print("=" * 60)
    print("Validating Graph Database Benchmark Project Structure")
    print("=" * 60)
    print()

    all_valid = True

    # Check configuration files
    print("Configuration Files:")
    all_valid &= check_file_exists("config/database-config.json", "Database config")
    all_valid &= check_file_exists("config/datasets.json", "Dataset config")
    all_valid &= check_file_exists("workloads/templates/example_workload.json", "Example workload")
    all_valid &= check_file_exists("workloads/templates/quick_test.json", "Quick test workload")
    print()

    # Check Python host files
    print("Python Host Implementation:")
    all_valid &= check_file_exists("host/benchmark_launcher.py", "Benchmark launcher")
    all_valid &= check_file_exists("host/compiler/workload_compiler.py", "Workload compiler")
    all_valid &= check_file_exists("host/dataset/dataset_loader.py", "Dataset loader")
    all_valid &= check_file_exists("host/db/docker_manager.py", "Docker manager")
    all_valid &= check_file_exists("host/report/report_generator.py", "Report generator")
    print()

    # Check Java Docker implementations
    print("Java Docker Implementations:")
    all_valid &= check_file_exists("docker/neo4j/pom.xml", "Neo4j pom.xml")
    all_valid &= check_file_exists("docker/neo4j/Dockerfile", "Neo4j Dockerfile")
    all_valid &= check_file_exists("docker/neo4j/src/main/java/com/graphbench/neo4j/BenchmarkServer.java", "Neo4j server")
    all_valid &= check_file_exists("docker/neo4j/src/main/java/com/graphbench/neo4j/Neo4jBenchmarkExecutor.java", "Neo4j executor")
    all_valid &= check_file_exists("docker/janusgraph/pom.xml", "JanusGraph pom.xml")
    all_valid &= check_file_exists("docker/janusgraph/Dockerfile", "JanusGraph Dockerfile")
    all_valid &= check_file_exists("docker/janusgraph/src/main/java/com/graphbench/janusgraph/BenchmarkServer.java", "JanusGraph server")
    all_valid &= check_file_exists("docker/janusgraph/src/main/java/com/graphbench/janusgraph/JanusGraphBenchmarkExecutor.java", "JanusGraph executor")
    print()

    # Check utility scripts
    print("Utility Scripts:")
    all_valid &= check_file_exists("build.sh", "Build script")
    all_valid &= check_file_exists("run_benchmark.sh", "Run benchmark script")
    all_valid &= check_file_exists("visualize.py", "Visualization script")
    print()

    # Check documentation
    print("Documentation:")
    all_valid &= check_file_exists("README.md", "README")
    all_valid &= check_file_exists("CONTRIBUTING.md", "Contributing guide")
    all_valid &= check_file_exists("requirements.txt", "Python requirements")
    print()

    # Check Python dependencies
    print("Checking Python Dependencies:")
    try:
        import docker
        print("✓ docker package installed")
    except ImportError:
        print("✗ docker package NOT installed (run: pip install -r requirements.txt)")
        all_valid = False

    try:
        import requests
        print("✓ requests package installed")
    except ImportError:
        print("✗ requests package NOT installed (run: pip install -r requirements.txt)")
        all_valid = False

    try:
        import matplotlib
        print("✓ matplotlib package installed")
    except ImportError:
        print("✗ matplotlib package NOT installed (run: pip install -r requirements.txt)")
        all_valid = False

    try:
        import seaborn
        print("✓ seaborn package installed")
    except ImportError:
        print("✗ seaborn package NOT installed (run: pip install -r requirements.txt)")
        all_valid = False

    try:
        import pandas
        print("✓ pandas package installed")
    except ImportError:
        print("✗ pandas package NOT installed (run: pip install -r requirements.txt)")
        all_valid = False

    print()

    # Check dataset directory
    print("Dataset Directory:")
    check_directory_exists("graph-datasets", "Graph datasets directory")
    print()

    # Final result
    print("=" * 60)
    if all_valid:
        print("✓ Project structure validation PASSED")
        print()
        print("Next steps:")
        print("  1. Install dependencies: pip install -r requirements.txt")
        print("  2. Build Docker images: ./build.sh")
        print("  3. Run benchmark: ./run_benchmark.sh neo4j coAuthorsDBLP")
    else:
        print("✗ Project structure validation FAILED")
        print("Please fix the issues above before proceeding.")
    print("=" * 60)

    return all_valid


if __name__ == '__main__':
    success = validate_project_structure()
    sys.exit(0 if success else 1)
