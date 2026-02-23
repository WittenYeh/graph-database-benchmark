#!/usr/bin/env python3
"""
Generate a beautiful index page for benchmark visualization plots.
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


def parse_filename(filename: str) -> Dict[str, str]:
    """Parse plot filename to extract metadata."""
    # Pattern: performance_{dataset}_{workload}.html or batchsize_{dataset}_{task}.html
    if filename.startswith('performance_'):
        match = re.match(r'performance_(.+)_(.+)\.html', filename)
        if match:
            return {
                'type': 'performance',
                'dataset': match.group(1),
                'workload': match.group(2),
                'task': 'All Tasks'
            }
    elif filename.startswith('batchsize_'):
        match = re.match(r'batchsize_(.+)_(.+)\.html', filename)
        if match:
            return {
                'type': 'batchsize',
                'dataset': match.group(1),
                'task': match.group(2),
                'workload': 'Batch Size Comparison'
            }
    return None


def load_report_metadata(reports_dir: Path, dataset: str, workload: str) -> List[str]:
    """Load database names from report files."""
    databases = []
    for report_file in reports_dir.glob(f"bench_*_{dataset}_{workload}.json"):
        # Extract database name from filename: bench_{db}_{dataset}_{workload}.json
        parts = report_file.stem.split('_')
        if len(parts) >= 4:
            db_name = parts[1]
            databases.append(db_name)
    return sorted(databases)


def generate_index_html(plots_dir: Path, reports_dir: Path, output_file: Path):
    """Generate a beautiful index.html page."""

    # Collect all plot files
    plot_files = sorted(plots_dir.glob('*.html'))

    # Parse and group plots
    plots_data = []
    for plot_file in plot_files:
        metadata = parse_filename(plot_file.name)
        if metadata:
            # Load databases from reports
            databases = load_report_metadata(reports_dir, metadata['dataset'], metadata['workload'])
            metadata['filename'] = plot_file.name
            metadata['databases'] = databases
            metadata['file_size'] = f"{plot_file.stat().st_size / 1024 / 1024:.2f} MB"
            plots_data.append(metadata)

    # Group by type
    performance_plots = [p for p in plots_data if p['type'] == 'performance']
    batchsize_plots = [p for p in plots_data if p['type'] == 'batchsize']

    # Generate HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Graph Database Benchmark Visualizations</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 2rem;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            overflow: hidden;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 3rem 2rem;
            text-align: center;
        }}

        .header h1 {{
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
            font-weight: 700;
        }}

        .header p {{
            font-size: 1.1rem;
            opacity: 0.9;
        }}

        .stats {{
            display: flex;
            justify-content: center;
            gap: 3rem;
            margin-top: 2rem;
        }}

        .stat-item {{
            text-align: center;
        }}

        .stat-number {{
            font-size: 2.5rem;
            font-weight: 700;
            display: block;
        }}

        .stat-label {{
            font-size: 0.9rem;
            opacity: 0.9;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .content {{
            padding: 3rem 2rem;
        }}

        .section {{
            margin-bottom: 3rem;
        }}

        .section-title {{
            font-size: 1.8rem;
            color: #333;
            margin-bottom: 1.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 3px solid #667eea;
            display: inline-block;
        }}

        .plots-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 2rem;
            margin-top: 2rem;
        }}

        .plot-card {{
            background: white;
            border: 2px solid #e0e0e0;
            border-radius: 12px;
            overflow: hidden;
            transition: all 0.3s ease;
            cursor: pointer;
        }}

        .plot-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.15);
            border-color: #667eea;
        }}

        .plot-card-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem;
        }}

        .plot-card-title {{
            font-size: 1.2rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }}

        .plot-card-subtitle {{
            font-size: 0.9rem;
            opacity: 0.9;
        }}

        .plot-card-body {{
            padding: 1.5rem;
        }}

        .plot-info {{
            display: flex;
            flex-direction: column;
            gap: 0.8rem;
        }}

        .info-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.5rem 0;
            border-bottom: 1px solid #f0f0f0;
        }}

        .info-row:last-child {{
            border-bottom: none;
        }}

        .info-label {{
            font-weight: 600;
            color: #666;
            font-size: 0.9rem;
        }}

        .info-value {{
            color: #333;
            font-size: 0.9rem;
        }}

        .databases-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
        }}

        .db-badge {{
            background: #667eea;
            color: white;
            padding: 0.3rem 0.8rem;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 500;
        }}

        .view-button {{
            display: block;
            width: 100%;
            padding: 1rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            text-align: center;
            text-decoration: none;
            border-radius: 8px;
            font-weight: 600;
            margin-top: 1rem;
            transition: all 0.3s ease;
        }}

        .view-button:hover {{
            transform: scale(1.02);
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
        }}

        .footer {{
            background: #f8f9fa;
            padding: 2rem;
            text-align: center;
            color: #666;
            font-size: 0.9rem;
        }}

        .timestamp {{
            margin-top: 0.5rem;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 GDSE Benchmark</h1>
            <p>Graph Database Storage Engine Benchmark (Native API Latency Test)</p>
            <div class="stats">
                <div class="stat-item">
                    <span class="stat-number">{len(batchsize_plots)}</span>
                    <span class="stat-label">Batch Size Plots</span>
                </div>
                <div class="stat-item">
                    <span class="stat-number">{len(plots_data)}</span>
                    <span class="stat-label">Total Plots</span>
                </div>
            </div>
        </div>

        <div class="content">
"""

    # Performance Comparison Section
    if performance_plots:
        html += """
            <div class="section">
                <h2 class="section-title">🏆 Performance Comparison</h2>
                <p style="color: #666; margin-bottom: 1rem;">Compare database performance across different tasks (best batch size selected)</p>
                <div class="plots-grid">
"""
        for plot in sorted(performance_plots, key=lambda x: (x['workload'], x['dataset'])):
            html += f"""
                    <div class="plot-card" onclick="window.location.href='{plot['filename']}'">
                        <div class="plot-card-header">
                            <div class="plot-card-title">{plot['workload'].replace('_', ' ').title()}</div>
                            <div class="plot-card-subtitle">{plot['dataset']}</div>
                        </div>
                        <div class="plot-card-body">
                            <div class="plot-info">
                                <div class="info-row">
                                    <span class="info-label">Type</span>
                                    <span class="info-value">Performance Comparison</span>
                                </div>
                                <div class="info-row">
                                    <span class="info-label">Dataset</span>
                                    <span class="info-value">{plot['dataset']}</span>
                                </div>
                                <div class="info-row">
                                    <span class="info-label">File Size</span>
                                    <span class="info-value">{plot['file_size']}</span>
                                </div>
                                <div class="info-row">
                                    <span class="info-label">Databases</span>
                                    <div class="databases-list">
"""
            for db in plot['databases']:
                html += f'                                        <span class="db-badge">{db}</span>\n'

            html += f"""
                                    </div>
                                </div>
                            </div>
                            <a href="{plot['filename']}" class="view-button">View Interactive Plot →</a>
                        </div>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    # Batch Size Comparison Section
    if batchsize_plots:
        html += """
            <div class="section">
                <h2 class="section-title">📈 Batch Size Analysis</h2>
                <p style="color: #666; margin-bottom: 1rem;">Analyze how performance varies with different batch sizes</p>
                <div class="plots-grid">
"""
        for plot in sorted(batchsize_plots, key=lambda x: (x['task'], x['dataset'])):
            html += f"""
                    <div class="plot-card" onclick="window.location.href='{plot['filename']}'">
                        <div class="plot-card-header">
                            <div class="plot-card-title">{plot['task'].replace('_', ' ').title()}</div>
                            <div class="plot-card-subtitle">{plot['dataset']}</div>
                        </div>
                        <div class="plot-card-body">
                            <div class="plot-info">
                                <div class="info-row">
                                    <span class="info-label">Type</span>
                                    <span class="info-value">Batch Size Analysis</span>
                                </div>
                                <div class="info-row">
                                    <span class="info-label">Dataset</span>
                                    <span class="info-value">{plot['dataset']}</span>
                                </div>
                                <div class="info-row">
                                    <span class="info-label">File Size</span>
                                    <span class="info-value">{plot['file_size']}</span>
                                </div>
                                <div class="info-row">
                                    <span class="info-label">Databases</span>
                                    <div class="databases-list">
"""
            for db in plot['databases']:
                html += f'                                        <span class="db-badge">{db}</span>\n'

            html += f"""
                                    </div>
                                </div>
                            </div>
                            <a href="{plot['filename']}" class="view-button">View Interactive Plot →</a>
                        </div>
                    </div>
"""
        html += """
                </div>
            </div>
"""

    # Footer
    html += f"""
        </div>

        <div class="footer">
            <p>Graph Database Benchmark Project</p>
            <p class="timestamp">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
    </div>
</body>
</html>
"""

    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"✓ Generated index page: {output_file}")
    print(f"  - {len(performance_plots)} performance plots")
    print(f"  - {len(batchsize_plots)} batch size plots")


def main():
    plots_dir = Path('plots')
    reports_dir = Path('reports')
    output_file = plots_dir / 'index.html'

    if not plots_dir.exists():
        print("❌ plots/ directory not found!")
        return

    generate_index_html(plots_dir, reports_dir, output_file)


if __name__ == '__main__':
    main()