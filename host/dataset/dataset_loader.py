"""
DatasetLoader - Handles dataset path resolution
"""
import os
from pathlib import Path
from typing import Optional, Dict, Any


class DatasetLoader:
    def __init__(self, dataset_config: Dict[str, Any]):
        self.config = dataset_config
        self.root_dir = Path(dataset_config['root_dir'])
        self.datasets = dataset_config['datasets']

    def get_dataset_path(self, dataset_name: str) -> Optional[Path]:
        """Get absolute path to dataset directory containing nodes.csv and edges.csv"""
        if dataset_name not in self.datasets:
            return None

        relative_path = self.datasets[dataset_name]
        # The config points to .mtx file; we want the parent directory with CSV files
        csv_dir = (self.root_dir / relative_path).parent

        nodes_csv = csv_dir / 'nodes.csv'
        edges_csv = csv_dir / 'edges.csv'

        if not nodes_csv.exists() or not edges_csv.exists():
            print(f"⚠️  Warning: CSV files not found in: {csv_dir}")
            return None

        return csv_dir.resolve()

    def list_datasets(self):
        """List all available datasets"""
        return list(self.datasets.keys())
