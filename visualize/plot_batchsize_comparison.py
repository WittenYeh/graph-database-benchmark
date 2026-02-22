#!/usr/bin/env python3
"""
Batch Size Comparison Visualization

Plots performance vs batch size for multiple databases on the same task.
Generates one interactive plot per task showing how latency changes with batch size.
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
                          workload: str, dataset: str) -> Dict[str, Dict[str, Any]]:
    """Find all report files matching the criteria."""
    matching_reports = {}

    for db in databases:
        pattern = f"bench_{db}_{dataset}_{workload}.json"
        filepath = reports_dir / pattern

        if filepath.exists():
            report = load_report(filepath)
            matching_reports[db] = report
        else:
            print(f"⚠️  Warning: Report not found: {filepath}")

    return matching_reports


def extract_batch_data(report: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Extract batch size data grouped by task type."""
    task_data = {}

    for result in report.get('results', []):
        task_type = result.get('task_type')
        batch_results = result.get('batch_results', [])

        if batch_results:
            task_data[task_type] = batch_results

    return task_data


def create_batch_size_plot(task_type: str, db_data: Dict[str, List[Dict[str, Any]]],
                           dataset: str, workload: str) -> go.Figure:
    """Create an interactive plot for a single task comparing databases."""
    fig = go.Figure()

    # Color palette for databases
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']

    for idx, (db_name, batch_results) in enumerate(db_data.items()):
        batch_sizes = []
        latencies = []

        for batch_result in batch_results:
            batch_sizes.append(batch_result['batch_size'])
            latencies.append(batch_result['latency_us'])

        # Add trace for this database
        fig.add_trace(go.Scatter(
            x=batch_sizes,
            y=latencies,
            mode='lines+markers',
            name=db_name,
            line=dict(width=2, color=colors[idx % len(colors)]),
            marker=dict(size=10, symbol='circle'),
            hovertemplate=(
                f'<b>{db_name}</b><br>' +
                'Batch Size: %{x}<br>' +
                'Latency: %{y:.2f} μs<br>' +
                '<extra></extra>'
            )
        ))

    # Update layout
    fig.update_layout(
        title=dict(
            text=f'<b>{task_type}</b><br><sub>Dataset: {dataset} | Workload: {workload}</sub>',
            x=0.5,
            xanchor='center',
            font=dict(size=20)
        ),
        xaxis=dict(
            title='<b>Batch Size</b>',
            type='log',
            tickmode='array',
            tickvals=batch_sizes,
            ticktext=[str(x) for x in batch_sizes],
            gridcolor='lightgray',
            showgrid=True,
            zeroline=False
        ),
        yaxis=dict(
            title='<b>Latency (μs)</b>',
            gridcolor='lightgray',
            showgrid=True,
            zeroline=False
        ),
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
        margin=dict(l=80, r=150, t=100, b=80),
        width=1000,
        height=600
    )

    return fig


def main():
    parser = argparse.ArgumentParser(
        description='Plot batch size comparison across databases for each task',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--database', nargs='+', required=True,
                        help='Database name(s) to compare')

    parser.add_argument('--workload', required=True,
                        help='Workload configuration name (e.g., example_workload)')

    parser.add_argument('--dataset', required=True,
                        help='Dataset name (only one dataset allowed)')

    parser.add_argument('--reports-dir', default='reports',
                        help='Directory containing benchmark reports')

    parser.add_argument('--output-dir', default='plots',
                        help='Output directory for plots')

    args = parser.parse_args()

    # Setup paths
    reports_dir = Path(args.reports_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"📊 Generating batch size comparison plots...")
    print(f"   Databases: {', '.join(args.database)}")
    print(f"   Dataset: {args.dataset}")
    print(f"   Workload: {args.workload}")
    print()

    # Find matching reports
    reports = find_matching_reports(reports_dir, args.database, args.workload, args.dataset)

    if not reports:
        print("❌ No matching reports found!")
        sys.exit(1)

    print(f"✓ Found {len(reports)} report(s)")

    # Extract batch data for each database
    all_task_data = {}
    for db_name, report in reports.items():
        task_data = extract_batch_data(report)
        for task_type, batch_results in task_data.items():
            if task_type not in all_task_data:
                all_task_data[task_type] = {}
            all_task_data[task_type][db_name] = batch_results

    # Generate one plot per task
    plot_count = 0
    for task_type, db_data in all_task_data.items():
        print(f"  📈 Generating plot for {task_type}...")

        fig = create_batch_size_plot(task_type, db_data, args.dataset, args.workload)

        # Save as HTML
        output_file = output_dir / f"batchsize_{args.dataset}_{task_type}.html"
        fig.write_html(str(output_file))
        print(f"     ✓ Saved to {output_file}")

        plot_count += 1

    print()
    print(f"🎉 Generated {plot_count} plot(s) in {output_dir}/")
    print(f"   Open the HTML files in a browser to view interactive plots")


if __name__ == '__main__':
    main()