"""
ReportGenerator - Generates visualizations from benchmark results
"""
import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from typing import List, Dict, Any


class ReportGenerator:
    def __init__(self):
        plt.style.use('seaborn-v0_8-darkgrid')

    def generate_comparison_report(self, report_files: List[Path], output_dir: Path, dataset_name: str, workload_name: str):
        """Generate comparison visualizations from multiple report files"""
        output_dir.mkdir(parents=True, exist_ok=True)

        # Load all reports
        reports = []
        for report_file in report_files:
            with open(report_file, 'r') as f:
                data = json.load(f)
                reports.append(data)

        # Generate visualizations
        self._plot_latency_grouped_bar(reports, output_dir, dataset_name, workload_name)
        self._plot_throughput_grouped_bar(reports, output_dir, dataset_name, workload_name)

        print(f"✓ Visualizations saved to {output_dir}")

    def _plot_latency_grouped_bar(self, reports: List[Dict], output_dir: Path, dataset_name: str, workload_name: str):
        """Plot latency comparison as grouped bar chart"""
        # Collect data: task -> database -> mean latency
        task_data = {}
        databases = set()

        for report in reports:
            db_name = report['metadata']['database']
            databases.add(db_name)

            for result in report['results']:
                if 'latency' in result:
                    task = result['task']
                    if task not in task_data:
                        task_data[task] = {}

                    # latency is now just a number (meanUs)
                    task_data[task][db_name] = result['latency']

        if not task_data:
            return

        databases = sorted(list(databases))
        tasks = sorted(list(task_data.keys()))

        # Create single bar chart for mean latency
        fig, ax = plt.subplots(figsize=(14, 8))

        x = np.arange(len(tasks))
        width = 0.8 / len(databases)

        for i, db in enumerate(databases):
            values = []
            for task in tasks:
                if db in task_data[task]:
                    values.append(task_data[task][db])
                else:
                    values.append(0)

            offset = (i - len(databases) / 2) * width + width / 2
            ax.bar(x + offset, values, width, label=db)

        ax.set_xlabel('Task', fontsize=12)
        ax.set_ylabel('Mean Latency (μs)', fontsize=12)
        ax.set_title(f'Mean Latency Comparison - {dataset_name} / {workload_name}', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(tasks, rotation=45, ha='right')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(output_dir / f'latency_comparison_{dataset_name}_{workload_name}.png', dpi=300, bbox_inches='tight')
        plt.close()

    def _plot_throughput_grouped_bar(self, reports: List[Dict], output_dir: Path, dataset_name: str, workload_name: str):
        """Plot throughput comparison as grouped bar chart"""
        # Collect data: task -> database -> throughput
        task_data = {}
        databases = set()

        for report in reports:
            db_name = report['metadata']['database']
            databases.add(db_name)

            for result in report['results']:
                if 'throughput' in result.get('task', ''):
                    task = result['task']
                    if task not in task_data:
                        task_data[task] = {}

                    duration = result['durationSeconds']
                    ops = result['totalOps']
                    throughput = ops / duration if duration > 0 else 0
                    task_data[task][db_name] = throughput

        if not task_data:
            return

        databases = sorted(list(databases))
        tasks = sorted(list(task_data.keys()))

        # Create grouped bar chart
        fig, ax = plt.subplots(figsize=(14, 8))

        x = np.arange(len(tasks))
        width = 0.8 / len(databases)

        for i, db in enumerate(databases):
            values = []
            for task in tasks:
                if db in task_data[task]:
                    values.append(task_data[task][db])
                else:
                    values.append(0)

            offset = (i - len(databases) / 2) * width + width / 2
            ax.bar(x + offset, values, width, label=db)

        ax.set_xlabel('Task', fontsize=12)
        ax.set_ylabel('Throughput (ops/s)', fontsize=12)
        ax.set_title(f'Throughput Comparison - {dataset_name} / {workload_name}', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(tasks, rotation=45, ha='right')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        plt.savefig(output_dir / f'throughput_comparison_{dataset_name}_{workload_name}.png', dpi=300, bbox_inches='tight')
        plt.close()
