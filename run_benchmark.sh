#!/bin/bash

# Quick start script for running benchmarks
#
# Usage with named arguments (recommended):
#   ./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP
#   ./run_benchmark.sh --database neo4j,janusgraph,arangodb --dataset coAuthorsDBLP,delaunay_n13
#   ./run_benchmark.sh --database all --dataset coAuthorsDBLP
#   ./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP --workload workloads/templates/quick_test.json
#
# Usage with positional arguments (legacy):
#   ./run_benchmark.sh neo4j coAuthorsDBLP
#   ./run_benchmark.sh neo4j,janusgraph,arangodb coAuthorsDBLP,delaunay_n13
#
# Arguments:
#   --database, -d    Comma-separated list of databases or "all" (e.g., "neo4j,janusgraph,arangodb" or "all")
#   --dataset, -s     Comma-separated list of datasets (e.g., "coAuthorsDBLP,delaunay_n13")
#   --workload, -w    Path to workload config (default: workloads/templates/example_workload.json)
#   --help, -h        Show this help message
#
# Note: The workload is compiled once per dataset and reused across all databases for efficiency.

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
            echo "  --database, -d    Comma-separated list of databases or 'all' (e.g., 'neo4j,janusgraph,arangodb' or 'all')"
            echo "  --dataset, -s     Comma-separated list of datasets (e.g., 'coAuthorsDBLP,delaunay_n13')"
            echo "  --workload, -w    Path to workload config (default: workloads/templates/example_workload.json)"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP"
            echo "  ./run_benchmark.sh -d neo4j,janusgraph,arangodb -s coAuthorsDBLP,delaunay_n13"
            echo "  ./run_benchmark.sh -d all -s coAuthorsDBLP"
            echo "  ./run_benchmark.sh -d arangodb -s coAuthorsDBLP -w workloads/templates/quick_test.json"
            echo ""
            echo "Note: The workload is compiled once per dataset and reused across all databases."
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

# Handle 'all' keyword for databases
if [ "$DATABASES" = "all" ]; then
    # Extract all database names from config file
    DATABASES=$(python3 -c "import json; config=json.load(open('config/database-config.json')); print(','.join(config.keys()))")
    echo "🔍 Detected 'all' keyword - running benchmarks for all databases: $DATABASES"
fi

# Convert comma-separated strings to space-separated for Python args
DB_ARGS=$(echo "$DATABASES" | tr ',' ' ')
DS_ARGS=$(echo "$DATASETS" | tr ',' ' ')

# Convert to arrays for display
IFS=',' read -ra DB_ARRAY <<< "$DATABASES"
IFS=',' read -ra DS_ARRAY <<< "$DATASETS"

echo "=========================================="
echo "Benchmark Configuration"
echo "=========================================="
echo "Databases: ${DB_ARRAY[*]}"
echo "Datasets: ${DS_ARRAY[*]}"
echo "Workload: $WORKLOAD_CONFIG"
echo "=========================================="
echo "Optimization: Workload compiled once per dataset, reused across all databases"
echo "=========================================="
echo

# Build Docker images for all databases
for DATABASE in "${DB_ARRAY[@]}"; do
    if ! docker images | grep -q "bench-$DATABASE"; then
        echo "Docker image bench-$DATABASE not found. Building..."
        ./build.sh
    fi
done

# Run benchmark with optimized approach:
# - For each dataset: compile workload once
# - Test all databases with the same compiled workload
# - Clean up compiled workload after testing all databases
echo
echo "=========================================="
echo "Running Optimized Benchmark"
echo "=========================================="
echo "Total databases: ${#DB_ARRAY[@]}"
echo "Total datasets: ${#DS_ARRAY[@]}"
echo "Total benchmark runs: $((${#DB_ARRAY[@]} * ${#DS_ARRAY[@]}))"
echo "=========================================="
echo

python host/benchmark_launcher.py \
    --database-name $DB_ARGS \
    --dataset-name $DS_ARGS \
    --workload-config "$WORKLOAD_CONFIG" \
    --output-dir reports

echo
echo "=========================================="
echo "All benchmarks complete!"
echo "=========================================="
echo "Total runs: $((${#DB_ARRAY[@]} * ${#DS_ARRAY[@]}))"
echo "Results directory: reports/"
echo
echo "To visualize results, run:"
echo "  python visualize.py reports/bench_*.json --output-dir visualizations"
