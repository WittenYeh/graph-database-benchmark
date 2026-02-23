#!/bin/bash
# Visualization wrapper script
# Calls visualization scripts in the visualize/ directory

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VISUALIZE_DIR="$SCRIPT_DIR"

# Default output directory
OUTPUT_DIR="plots"

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [COMMAND] [OPTIONS]

Commands:
  batchsize       Generate batch size comparison plots
  performance     Generate performance comparison plots
  help            Show this help message

Batch Size Comparison:
  bash $0 batchsize --database DB1,DB2,... --workload WORKLOAD --dataset DATASET [--output-dir DIR]

Performance Comparison:
  bash $0 performance --database DB1,DB2,... --workload WORKLOAD --dataset DS1,DS2,... [--output-dir DIR]

Examples:
  # Batch size comparison for a single dataset
  bash $0 batchsize --database neo4j,janusgraph --workload example_workload --dataset delaunay_n13

  # Performance comparison across multiple datasets
  bash $0 performance --database neo4j,janusgraph --workload example_workload --dataset delaunay_n13,movielens-small

Examples:
  # Batch size comparison for a single dataset
  bash $0 batchsize \\
    --database neo4j,janusgraph,orientdb,sqlg,arangodb \\
    --workload batchsize_comparison \\
    --dataset coAuthorsDBLP
  # Performance comparison across multiple datasets
  bash $0 performance \\
    --database neo4j,janusgraph,orientdb,sqlg,arangodb,aster \\
    --workload small_structural_workload \\
    --dataset delaunay_n13,movielens-small
  bash $0 performance \\
    --database neo4j,janusgraph,orientdb,sqlg,arangodb,aster \\
    --workload medium_structural_workload \\
    --dataset delaunay_n21,cit-Patents,coAuthorsDBLP
  bash $0 performance \\
    --database neo4j,janusgraph,orientdb,sqlg,arangodb,aster \\
    --workload large_structural_workload \\
    --dataset hollywood-2009,soc-twitter,soc-orkut,soc-livejournal

EOF
    exit 0
}

# Check if no arguments provided
if [ $# -eq 0 ]; then
    usage
fi

# Parse command
COMMAND="$1"
shift

case "$COMMAND" in
    batchsize)
        python3 "$VISUALIZE_DIR/plot_batchsize_comparison.py" "$@"
        ;;
    performance)
        python3 "$VISUALIZE_DIR/plot_performance_comparison.py" "$@"
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        echo "Error: Unknown command '$COMMAND'"
        echo ""
        usage
        ;;
esac