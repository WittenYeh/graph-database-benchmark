#!/bin/bash
# Example script demonstrating visualization usage

echo "=========================================="
echo "Visualization Examples"
echo "=========================================="
echo ""

# Example 1: Batch size comparison
echo "Example 1: Batch Size Comparison"
echo "Compare how different databases perform with varying batch sizes"
echo ""
echo "Command:"
echo "  python3 visualize/plot_batchsize_comparison.py \\"
echo "      --database neo4j janusgraph arangodb aster \\"
echo "      --workload example_workload \\"
echo "      --dataset delaunay_n13 \\"
echo "      --output-dir plots"
echo ""
echo "Output: One HTML file per task (ADD_VERTEX, ADD_EDGE, etc.)"
echo "  - plots/batchsize_delaunay_n13_ADD_VERTEX.html"
echo "  - plots/batchsize_delaunay_n13_ADD_EDGE.html"
echo "  - ..."
echo ""
echo "=========================================="
echo ""

# Example 2: Performance comparison
echo "Example 2: Performance Comparison"
echo "Compare overall performance across databases for multiple datasets"
echo ""
echo "Command:"
echo "  python3 visualize/plot_performance_comparison.py \\"
echo "      --database neo4j janusgraph arangodb aster \\"
echo "      --workload example_workload \\"
echo "      --dataset delaunay_n13 movielens-small \\"
echo "      --output-dir plots"
echo ""
echo "Output: One HTML file per dataset"
echo "  - plots/performance_delaunay_n13_example_workload.html"
echo "  - plots/performance_movielens-small_example_workload.html"
echo ""
echo "=========================================="
echo ""

# Run examples if requested
if [ "$1" = "--run" ]; then
    echo "Running Example 1..."
    python3 visualize/plot_batchsize_comparison.py \
        --database neo4j janusgraph arangodb aster \
        --workload example_workload \
        --dataset delaunay_n13 \
        --output-dir plots

    echo ""
    echo "Running Example 2..."
    python3 visualize/plot_performance_comparison.py \
        --database neo4j janusgraph arangodb aster \
        --workload example_workload \
        --dataset delaunay_n13 movielens-small \
        --output-dir plots

    echo ""
    echo "=========================================="
    echo "✓ Examples completed!"
    echo "Open plots/*.html in your browser to view the plots"
else
    echo "To run these examples, use: $0 --run"
fi