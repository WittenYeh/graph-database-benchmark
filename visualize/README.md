# Visualization Tools

This directory contains interactive visualization tools for benchmark results using Plotly.

## Scripts

### 1. plot_batchsize_comparison.py

Plots performance vs batch size for multiple databases on the same task.
Generates one interactive plot per task showing how latency changes with batch size.

**Usage:**
```bash
python visualize/plot_batchsize_comparison.py \
    --database neo4j janusgraph arangodb \
    --workload example_workload \
    --dataset delaunay_n13 \
    --output-dir visualizations
```

**Parameters:**
- `--database`: Database name(s) to compare (multiple allowed)
- `--workload`: Workload configuration name (e.g., `example_workload`)
- `--dataset`: Dataset name (only one dataset allowed)
- `--reports-dir`: Directory containing benchmark reports (default: `reports`)
- `--output-dir`: Output directory for plots (default: `visualizations`)

**Output:**
- One HTML file per task: `batchsize_{dataset}_{task_type}.html`
- Each plot shows multiple databases as different colored lines
- X-axis: Batch size (log scale)
- Y-axis: Latency (μs)

### 2. plot_performance_comparison.py

Plots performance comparison across databases for each dataset.
Generates one interactive grouped bar chart per dataset, with tasks grouped
and databases compared within each group.

**Usage:**
```bash
python visualize/plot_performance_comparison.py \
    --database neo4j janusgraph arangodb \
    --workload example_workload \
    --dataset delaunay_n13 coAuthorsDBLP \
    --output-dir visualizations
```

**Parameters:**
- `--database`: Database name(s) to compare (multiple allowed)
- `--workload`: Workload configuration name (e.g., `example_workload`)
- `--dataset`: Dataset name(s) to plot (multiple allowed)
- `--reports-dir`: Directory containing benchmark reports (default: `reports`)
- `--output-dir`: Output directory for plots (default: `visualizations`)

**Output:**
- One HTML file per dataset: `performance_{dataset}_{workload}.html`
- Each plot shows grouped bars: tasks on X-axis, databases as different colored bars
- Y-axis: Average latency across all batch sizes (μs)

## Requirements

Install Plotly:
```bash
pip install plotly
```

## Examples

### Example 1: Compare batch size performance for 3 databases on one dataset
```bash
python visualize/plot_batchsize_comparison.py \
    --database neo4j janusgraph arangodb \
    --workload example_workload \
    --dataset delaunay_n13
```

This generates multiple plots (one per task):
- `visualizations/batchsize_delaunay_n13_ADD_VERTEX.html`
- `visualizations/batchsize_delaunay_n13_ADD_EDGE.html`
- `visualizations/batchsize_delaunay_n13_REMOVE_VERTEX.html`
- etc.

### Example 2: Compare overall performance across multiple datasets
```bash
python visualize/plot_performance_comparison.py \
    --database neo4j janusgraph arangodb aster \
    --workload example_workload \
    --dataset delaunay_n13 coAuthorsDBLP movielens-small
```

This generates:
- `visualizations/performance_delaunay_n13_example_workload.html`
- `visualizations/performance_coAuthorsDBLP_example_workload.html`
- `visualizations/performance_movielens-small_example_workload.html`

## Features

- **Interactive plots**: Hover to see exact values, zoom, pan, export as PNG
- **Professional styling**: Clean design suitable for papers and presentations
- **Web-ready**: HTML files can be embedded in web pages
- **Automatic color coding**: Consistent colors for each database across plots
- **Responsive layout**: Plots adapt to different screen sizes