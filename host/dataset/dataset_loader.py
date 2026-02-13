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
        """Get absolute path to dataset file"""
        if dataset_name not in self.datasets:
            return None

        relative_path = self.datasets[dataset_name]
        full_path = self.root_dir / relative_path

        if not full_path.exists():
            print(f"⚠️  Warning: Dataset file not found: {full_path}")
            return None

        return full_path.resolve()

    def list_datasets(self):
        """List all available datasets"""
        return list(self.datasets.keys())
