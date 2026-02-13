#!/bin/bash

# Quick start script for running benchmarks
#
# Usage with named arguments (recommended):
#   ./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP
#   ./run_benchmark.sh --database neo4j,janusgraph --dataset coAuthorsDBLP,delaunay_n13
#   ./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP --workload workloads/templates/quick_test.json
#
# Usage with positional arguments (legacy):
#   ./run_benchmark.sh neo4j coAuthorsDBLP
#   ./run_benchmark.sh neo4j,janusgraph coAuthorsDBLP,delaunay_n13
#
# Arguments:
#   --database, -d    Comma-separated list of databases (e.g., "neo4j,janusgraph")
#   --dataset, -s     Comma-separated list of datasets (e.g., "coAuthorsDBLP,delaunay_n13")
#   --workload, -w    Path to workload config (default: workloads/templates/example_workload.json)
#   --help, -h        Show this help message

set -e

# Default values
DATABASES=""
DATASETS=""
WORKLOAD_CONFIG="workloads/templates/example_workload.json"

# Parse named arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --database|-d)
            DATABASES="$2"
            shift 2
            ;;
        --dataset|-s)
            DATASETS="$2"
            shift 2
            ;;
        --workload|-w)
            WORKLOAD_CONFIG="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: ./run_benchmark.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --database, -d    Comma-separated list of databases (e.g., 'neo4j,janusgraph')"
            echo "  --dataset, -s     Comma-separated list of datasets (e.g., 'coAuthorsDBLP,delaunay_n13')"
            echo "  --workload, -w    Path to workload config (default: workloads/templates/example_workload.json)"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP"
            echo "  ./run_benchmark.sh -d neo4j,janusgraph -s coAuthorsDBLP,delaunay_n13"
            echo "  ./run_benchmark.sh -d neo4j -s coAuthorsDBLP -w workloads/templates/quick_test.json"
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
        *)
            # Positional arguments (legacy support)
            if [ -z "$DATABASES" ]; then
                DATABASES="$1"
            elif [ -z "$DATASETS" ]; then
                DATASETS="$1"
            elif [ "$WORKLOAD_CONFIG" = "workloads/templates/example_workload.json" ]; then
                WORKLOAD_CONFIG="$1"
            fi
            shift
            ;;
    esac
done

# Apply defaults if not specified
DATABASES=${DATABASES:-neo4j}
DATASETS=${DATASETS:-coAuthorsDBLP}

# Convert comma-separated strings to arrays
IFS=',' read -ra DB_ARRAY <<< "$DATABASES"
IFS=',' read -ra DS_ARRAY <<< "$DATASETS"

echo "=========================================="
echo "Benchmark Configuration"
echo "=========================================="
echo "Databases: ${DB_ARRAY[*]}"
echo "Datasets: ${DS_ARRAY[*]}"
echo "Workload: $WORKLOAD_CONFIG"
echo "=========================================="
echo

# Build Docker images for all databases
for DATABASE in "${DB_ARRAY[@]}"; do
    if ! docker images | grep -q "bench-$DATABASE"; then
        echo "Docker image bench-$DATABASE not found. Building..."
        ./build.sh
    fi
done

# Run benchmarks for all combinations
TOTAL_RUNS=$((${#DB_ARRAY[@]} * ${#DS_ARRAY[@]}))
CURRENT_RUN=0

for DATABASE in "${DB_ARRAY[@]}"; do
    for DATASET in "${DS_ARRAY[@]}"; do
        CURRENT_RUN=$((CURRENT_RUN + 1))
        echo
        echo "=========================================="
        echo "Run $CURRENT_RUN/$TOTAL_RUNS: $DATABASE on $DATASET"
        echo "=========================================="

        python host/benchmark_launcher.py \
            --database-name "$DATABASE" \
            --dataset-name "$DATASET" \
            --workload-config "$WORKLOAD_CONFIG" \
            --output-dir reports

        echo "✓ Results saved to reports/ directory."
    done
done

echo
echo "=========================================="
echo "All benchmarks complete!"
echo "=========================================="
echo "Total runs: $TOTAL_RUNS"
echo "Results directory: reports/"
echo
echo "To visualize results, run:"
echo "  python visualize.py reports/bench_*.json --output-dir visualizations"
