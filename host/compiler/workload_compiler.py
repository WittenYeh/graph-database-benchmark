"""
WorkloadCompiler - Compiles workload configurations to native API workload files
"""
import json
import random
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple


class WorkloadCompiler:
    def __init__(self, database_config: Dict[str, Any]):
        self.database_config = database_config
        self.dataset_path: Optional[Path] = None
        self.dataset_edges: List[Tuple[int, int]] = []  # Cache all edges in memory

    def compile_workload(
        self,
        workload_config: Dict[str, Any],
        database_name: str,
        dataset_name: str,
        seed: Optional[int] = None,
        dataset_path: Optional[Path] = None
    ) -> Path:
        """
        Compile workload to native API format
        Returns path to directory containing compiled JSON files
        """
        if seed is not None:
            random.seed(seed)

        # Store dataset path and load edges for random sampling
        if dataset_path:
            self.dataset_path = dataset_path
            self.dataset_edges = self._load_dataset_edges(dataset_path)
            print(f"  Dataset has {len(self.dataset_edges)} edges loaded for sampling")

        # Create output directory (clean if exists)
        output_dir = Path(f"workloads/compiled/{database_name}_{dataset_name}")
        if output_dir.exists():
            import shutil
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Compile each task
        tasks = workload_config.get('tasks', [])
        for idx, task in enumerate(tasks):
            task_name = task['name']
            workload_data = self._compile_task(task, dataset_name)

            # Save to JSON file
            output_file = output_dir / f"{idx:02d}_{task_name}.json"
            with open(output_file, 'w') as f:
                json.dump(workload_data, f, indent=2)

        return output_dir

    def _compile_task(self, task: Dict[str, Any], dataset_name: str) -> Dict[str, Any]:
        """Compile a single task to native API format"""
        task_name = task['name']
        ops = task.get('ops', 0)

        if task_name == 'load_graph':
            return self._compile_load_graph()
        elif task_name == 'add_vertex':
            return self._compile_add_vertex(ops)
        elif task_name == 'upsert_vertex_property':
            return self._compile_upsert_vertex_property(ops)
        elif task_name == 'remove_vertex':
            return self._compile_remove_vertex(ops)
        elif task_name == 'add_edge':
            return self._compile_add_edge(ops)
        elif task_name == 'upsert_edge_property':
            return self._compile_upsert_edge_property(ops)
        elif task_name == 'remove_edge':
            return self._compile_remove_edge(ops)
        elif task_name == 'get_nbrs':
            direction = task.get('direction', 'OUT')
            return self._compile_get_nbrs(ops, direction)
        elif task_name == 'get_vertex_by_property':
            return self._compile_get_vertex_by_property(ops)
        elif task_name == 'get_edge_by_property':
            return self._compile_get_edge_by_property(ops)

        return {}

    def _compile_load_graph(self) -> Dict[str, Any]:
        """Compile LOAD_GRAPH task"""
        return {
            "task_type": "LOAD_GRAPH",
            "ops_count": 0,
            "parameters": {}
        }

    def _compile_add_vertex(self, ops: int) -> Dict[str, Any]:
        """Compile ADD_VERTEX task - generate random vertex IDs"""
        ids = []
        for i in range(ops):
            # Generate random vertex ID
            vertex_id = random.randint(1000000, 9999999)
            ids.append(vertex_id)

        return {
            "task_type": "ADD_VERTEX",
            "ops_count": ops,
            "parameters": {
                "ids": ids
            }
        }

    def _compile_upsert_vertex_property(self, ops: int) -> Dict[str, Any]:
        """Compile UPSERT_VERTEX_PROPERTY task"""
        updates = []
        for i in range(ops):
            # Sample a random node from dataset
            node_id = self._sample_node_from_dataset()

            # Generate random properties
            properties = {
                "name": f"Node_{node_id}",
                "value": random.randint(1, 100),
                "active": random.choice([True, False])
            }

            updates.append({
                "id": node_id,
                "properties": properties
            })

        return {
            "task_type": "UPSERT_VERTEX_PROPERTY",
            "ops_count": ops,
            "parameters": {
                "updates": updates
            }
        }

    def _compile_remove_vertex(self, ops: int) -> Dict[str, Any]:
        """Compile REMOVE_VERTEX task - sample random nodes from dataset"""
        ids = []
        for i in range(ops):
            node_id = self._sample_node_from_dataset()
            ids.append(node_id)

        return {
            "task_type": "REMOVE_VERTEX",
            "ops_count": ops,
            "parameters": {
                "ids": ids
            }
        }

    def _compile_add_edge(self, ops: int) -> Dict[str, Any]:
        """Compile ADD_EDGE task - sample random node pairs from dataset"""
        pairs = []
        for i in range(ops):
            src_id = self._sample_node_from_dataset()
            dst_id = self._sample_node_from_dataset()

            pairs.append({
                "src": src_id,
                "dst": dst_id
            })

        return {
            "task_type": "ADD_EDGE",
            "ops_count": ops,
            "parameters": {
                "label": "knows",
                "pairs": pairs
            }
        }

    def _compile_upsert_edge_property(self, ops: int) -> Dict[str, Any]:
        """Compile UPSERT_EDGE_PROPERTY task"""
        updates = []
        for i in range(ops):
            # Sample a random edge from dataset
            src_id, dst_id = self._sample_edge_from_dataset()

            # Generate random properties
            properties = {
                "weight": round(random.uniform(0.1, 1.0), 2),
                "since": f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            }

            updates.append({
                "src": src_id,
                "dst": dst_id,
                "properties": properties
            })

        return {
            "task_type": "UPSERT_EDGE_PROPERTY",
            "ops_count": ops,
            "parameters": {
                "label": "MyEdge",
                "updates": updates
            }
        }

    def _compile_remove_edge(self, ops: int) -> Dict[str, Any]:
        """Compile REMOVE_EDGE task - sample random edges from dataset"""
        pairs = []
        for i in range(ops):
            src_id, dst_id = self._sample_edge_from_dataset()

            pairs.append({
                "src": src_id,
                "dst": dst_id
            })

        return {
            "task_type": "REMOVE_EDGE",
            "ops_count": ops,
            "parameters": {
                "label": "MyEdge",
                "pairs": pairs
            }
        }

    def _compile_get_nbrs(self, ops: int, direction: str = "OUT") -> Dict[str, Any]:
        """Compile GET_NBRS task - sample random nodes from dataset"""
        ids = []
        for i in range(ops):
            node_id = self._sample_node_from_dataset()
            ids.append(node_id)

        return {
            "task_type": "GET_NBRS",
            "ops_count": ops,
            "parameters": {
                "direction": direction,
                "ids": ids
            }
        }

    def _compile_get_vertex_by_property(self, ops: int) -> Dict[str, Any]:
        """Compile GET_VERTEX_BY_PROPERTY task"""
        queries = []
        for i in range(ops):
            # Generate random property queries
            key = random.choice(["name", "value", "active"])
            if key == "name":
                node_id = self._sample_node_from_dataset()
                value = f"Node_{node_id}"
            elif key == "value":
                value = random.randint(1, 100)
            else:  # active
                value = random.choice([True, False])

            queries.append({
                "key": key,
                "value": value
            })

        return {
            "task_type": "GET_VERTEX_BY_PROPERTY",
            "ops_count": ops,
            "parameters": {
                "queries": queries
            }
        }

    def _compile_get_edge_by_property(self, ops: int) -> Dict[str, Any]:
        """Compile GET_EDGE_BY_PROPERTY task"""
        queries = []
        for i in range(ops):
            # Generate random property queries
            key = random.choice(["weight", "since"])
            if key == "weight":
                value = round(random.uniform(0.1, 1.0), 2)
            else:  # since
                value = f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"

            queries.append({
                "key": key,
                "value": value
            })

        return {
            "task_type": "GET_EDGE_BY_PROPERTY",
            "ops_count": ops,
            "parameters": {
                "label": "MyEdge",
                "queries": queries
            }
        }

    def _load_dataset_edges(self, dataset_path: Path) -> List[Tuple[int, int]]:
        """Load all edges from dataset into memory for efficient random sampling"""
        edges = []
        with open(dataset_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('%'):
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        # Skip header line
                        if len(parts) == 3 and int(parts[0]) > 1000:
                            continue
                        src = int(parts[0])
                        dst = int(parts[1])
                        edges.append((src, dst))
                    except ValueError:
                        continue
        return edges

    def _sample_node_from_dataset(self) -> int:
        """Sample a random node ID from dataset edges"""
        if not self.dataset_edges:
            return random.randint(1, 1000)

        # Pick a random edge and randomly choose src or dst
        edge = random.choice(self.dataset_edges)
        return random.choice([edge[0], edge[1]])

    def _sample_edge_from_dataset(self) -> Tuple[int, int]:
        """Sample a random edge from dataset"""
        if not self.dataset_edges:
            return (random.randint(1, 1000), random.randint(1, 1000))

        return random.choice(self.dataset_edges)
