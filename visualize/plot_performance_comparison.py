#!/usr/bin/env python3
"""
Performance Comparison Visualization

Plots performance comparison across databases for each dataset.
Generates one interactive grouped bar chart per dataset, with tasks grouped
and databases compared within each group.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def load_report(filepath: Path) -> Dict[str, Any]:
    """Load a benchmark report JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def find_matching_reports(reports_dir: Path, databases: List[str],
                          workload: str, datasets: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Find all report files matching the criteria.

    Returns:
        {dataset: {database: report_data}}
    """
    matching_reports = {}

    for dataset in datasets:
        matching_reports[dataset] = {}
        for db in databases:
            pattern = f"bench_{db}_{dataset}_{workload}.json"
            filepath = reports_dir / pattern

            if filepath.exists():
                report = load_report(filepath)
                matching_reports[dataset][db] = report
            else:
                print(f"⚠️  Warning: Report not found: {filepath}")

    return matching_reports


def extract_average_latency(report: Dict[str, Any]) -> Dict[str, float]:
    """Extract average latency for each task type.

    For tasks with batch_results, compute the average across all batch sizes.
    """
    task_latencies = {}

    for result in report.get('results', []):
        task_type = result.get('task_type')

        # Skip LOAD_GRAPH as it doesn't have batch results
        if task_type == 'LOAD_GRAPH':
            continue

        batch_results = result.get('batch_results', [])

        if batch_results:
            # Average latency across all batch sizes
            avg_latency = sum(br['latency_us'] for br in batch_results) / len(batch_results)
            task_latencies[task_type] = avg_latency

    return task_latencies


def create_performance_comparison_plot(dataset: str, db_data: Dict[str, Dict[str, float]],
                                       workload: str) -> go.Figure:
    """Create an interactive grouped bar chart for a single dataset.

    Args:
        dataset: Dataset name
        db_data: {database: {task_type: avg_latency}}
        workload: Workload name
    """
    # Get all unique task types across all databases
    all_tasks = set()
    for task_latencies in db_data.values():
        all_tasks.update(task_latencies.keys())
    all_tasks = sorted(list(all_tasks))

    # Color palette for databases
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    fig = go.Figure()

    # Add a bar trace for each database
    for idx, (db_name, task_latencies) in enumerate(db_data.items()):
        latencies = [task_latencies.get(task, 0) for task in all_tasks]

        fig.add_trace(go.Bar(
            name=db_name,
            x=all_tasks,
            y=latencies,
            marker=dict(
                color=colors[idx % len(colors)],
                line=dict(color='white', width=1)
            ),
            hovertemplate=(
                f'<b>{db_name}</b><br>' +
                'Task: %{x}<br>' +
                'Avg Latency: %{y:.2f} μs<br>' +
                '<extra></extra>'
            )
        ))

    # Update layout
    fig.update_layout(
        title=dict(
            text=f'<b>Performance Comparison</b><br><sub>Dataset: {dataset} | Workload: {workload}</sub>',
            x=0.5,
            xanchor='center',
            font=dict(size=20)
        ),
        xaxis=dict(
            title='<b>Task Type</b>',
            tickangle=-45,
            gridcolor='lightgray',
            showgrid=False
        ),
        yaxis=dict(
            title='<b>Average Latency (μs)</b>',
            gridcolor='lightgray',
            showgrid=True,
            zeroline=False
        ),
        barmode='group',
        hovermode='closest',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(family='Arial, sans-serif', size=12),
        legend=dict(
            title='<b>Database</b>',
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02,
            bgcolor='rgba(255, 255, 255, 0.8)',
            bordercolor='lightgray',
            borderwidth=1
        ),
        margin=dict(l=80, r=150, t=100, b=120),
        width=1000,
        height=600
    )

    return fig


def main():
    parser = argparse.ArgumentParser(
        description='Plot performance comparison across databases for each dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--database', nargs='+', required=True,
                        help='Database name(s) to compare')

    parser.add_argument('--workload', required=True,
                        help='Workload configuration name (e.g., example_workload)')

    parser.add_argument('--dataset', nargs='+', required=True,
                        help='Dataset name(s) to plot')

    parser.add_argument('--reports-dir', default='reports',
                        help='Directory containing benchmark reports')

    parser.add_argument('--output-dir', default='plots',
                        help='Output directory for plots')

    args = parser.parse_args()

    # Setup paths
    reports_dir = Path(args.reports_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"📊 Generating performance comparison plots...")
    print(f"   Databases: {', '.join(args.database)}")
    print(f"   Datasets: {', '.join(args.dataset)}")
    print(f"   Workload: {args.workload}")
    print()

    # Find matching reports
    reports = find_matching_reports(reports_dir, args.database, args.workload, args.dataset)

    # Generate one plot per dataset
    plot_count = 0
    for dataset, db_reports in reports.items():
        if not db_reports:
            print(f"⚠️  No reports found for dataset: {dataset}")
            continue

        print(f"  📈 Generating plot for dataset: {dataset}...")

        # Extract average latencies for each database
        db_data = {}
        for db_name, report in db_reports.items():
            task_latencies = extract_average_latency(report)
            if task_latencies:
                db_data[db_name] = task_latencies

        if not db_data:
            print(f"     ⚠️  No valid data for dataset: {dataset}")
            continue

        # Create plot
        fig = create_performance_comparison_plot(dataset, db_data, args.workload)

        # Save as HTML
        output_file = output_dir / f"performance_{dataset}_{args.workload}.html"
        fig.write_html(str(output_file))
        print(f"     ✓ Saved to {output_file}")

        plot_count += 1

    print()
    if plot_count > 0:
        print(f"🎉 Generated {plot_count} plot(s) in {output_dir}/")
        print(f"   Open the HTML files in a browser to view interactive plots")
    else:
        print("❌ No plots generated. Check your input parameters and report files.")
        sys.exit(1)


if __name__ == '__main__':
    main()