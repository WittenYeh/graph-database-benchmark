#!/usr/bin/env python3
"""
Visualization tool for benchmark reports
"""
import argparse
import sys
import json
from pathlib import Path

# Add host directory to path
sys.path.insert(0, str(Path(__file__).parent))

from host.report.report_generator import ReportGenerator


def main():
    parser = argparse.ArgumentParser(
        description='Generate visualizations from benchmark reports'
    )

    parser.add_argument('--report-dir', type=Path, default='reports',
                        help='Directory containing benchmark report JSON files (default: reports)')

    parser.add_argument('--dataset-name', required=True,
                        help='Dataset name to filter reports')

    parser.add_argument('--workload-name', required=True,
                        help='Workload name to filter reports')

    parser.add_argument('--output-dir', type=Path, default='visualizations',
                        help='Output directory for visualizations')

    args = parser.parse_args()

    # Validate report directory
    if not args.report_dir.exists():
        print(f"❌ Error: Report directory not found: {args.report_dir}")
        sys.exit(1)

    # Find all matching report files
    print(f"🔍 Searching for reports in {args.report_dir}...")
    print(f"  Dataset: {args.dataset_name}")
    print(f"  Workload: {args.workload_name}")

    matching_reports = []
    for report_file in args.report_dir.glob("*.json"):
        try:
            with open(report_file, 'r') as f:
                data = json.load(f)
                metadata = data.get('metadata', {})

                # Check if dataset and workload match
                if (metadata.get('dataset') == args.dataset_name and
                    metadata.get('workload') == args.workload_name):
                    matching_reports.append(report_file)
                    print(f"  ✓ Found: {report_file.name}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  ⚠️  Skipping invalid report: {report_file.name}")
            continue

    if not matching_reports:
        print(f"\n❌ Error: No reports found matching dataset '{args.dataset_name}' and workload '{args.workload_name}'")
        sys.exit(1)

    # Generate visualizations
    print(f"\n📊 Generating visualizations from {len(matching_reports)} report(s)...")
    generator = ReportGenerator()
    generator.generate_comparison_report(matching_reports, args.output_dir, args.dataset_name, args.workload_name)

    print(f"\n✅ Visualization complete! Check {args.output_dir}/ for results.")


if __name__ == '__main__':
    main()
