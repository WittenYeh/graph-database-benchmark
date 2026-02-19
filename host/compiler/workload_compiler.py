"""
WorkloadCompiler - Optimized for "Reset/Restore" Benchmark Model
"""
import json
import random
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

class WorkloadCompiler:
    def __init__(self, database_config: Dict[str, Any]):
        self.database_config = database_config
        self.dataset_path: Optional[Path] = None

        # Reservoir sampling: Store only samples from the initial dataset
        self.SAMPLE_SIZE = 100_000
        self.sampled_nodes: List[int] = []
        self.sampled_edges: List[Tuple[int, int]] = []

        # Core boundary: The maximum ID in the initial dataset
        self.max_dataset_id = 0

    def compile_workload(
        self,
        workload_config: Dict[str, Any],
        database_name: str,
        dataset_name: str,
        seed: Optional[int] = None,
        dataset_path: Optional[Path] = None
    ) -> Path:
        if seed is not None:
            random.seed(seed)

        # 1. Scan the initial dataset (Benchmark Baseline)
        if dataset_path:
            self._scan_dataset(dataset_path)

        # Clean up the specific compiled workload directory
        output_dir = Path(f"workloads/compiled/{database_name}_{dataset_name}")
        if output_dir.exists():
            import shutil
            print(f"  🧹 Removing old compiled workload: {output_dir}")
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        tasks = workload_config.get('tasks', [])
        for idx, task in enumerate(tasks):
            task_name = task['name']

            # ★ Key modification: No need to reset generator state before each task
            # because the graph is restored. However, we must ensure all operations
            # are based on self.sampled_nodes (initial data).
            workload_data = self._compile_task(task)

            output_file = output_dir / f"{idx:02d}_{task_name}.json"
            with open(output_file, 'w') as f:
                json.dump(workload_data, f, indent=2)

        return output_dir

    def _scan_dataset(self, dataset_path: Path):
        """Scan and sample the initial dataset"""
        print(f"Scanning baseline dataset: {dataset_path}...")
        edge_count = 0

        with open(dataset_path, 'r') as f:
            for line in f:
                if not line or line.startswith(('%', '#')): continue
                parts = line.split()
                if len(parts) < 2: continue

                try:
                    src, dst = int(parts[0]), int(parts[1])

                    # Track boundary for AddVertex
                    self.max_dataset_id = max(self.max_dataset_id, src, dst)

                    # Reservoir sampling for edges
                    if len(self.sampled_edges) < self.SAMPLE_SIZE:
                        self.sampled_edges.append((src, dst))
                    else:
                        r = random.randint(0, edge_count)
                        if r < self.SAMPLE_SIZE:
                            self.sampled_edges[r] = (src, dst)

                    # Reservoir sampling for nodes
                    if len(self.sampled_nodes) < self.SAMPLE_SIZE:
                        self.sampled_nodes.append(src)
                        # Could also sample dst, depending on distribution
                    else:
                        r = random.randint(0, edge_count)
                        if r < self.SAMPLE_SIZE:
                            self.sampled_nodes[r] = src

                    edge_count += 1
                except ValueError:
                    continue
        print(f"Baseline Scanned. Max ID: {self.max_dataset_id}")

    def _compile_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_name = task['name']
        ops = task.get('ops', 0)
        batch_sizes = task.get('batch_sizes', None)

        if task_name == 'add_vertex':
            result = self._compile_add_vertex(ops)
        elif task_name == 'remove_vertex':
            result = self._compile_remove_vertex(ops)
        elif task_name == 'add_edge':
            result = self._compile_add_edge(ops)
        elif task_name == 'remove_edge':
            result = self._compile_remove_edge(ops)
        elif task_name in ['update_vertex_property', 'get_vertex_by_property']:
            is_write = (task_name == 'update_vertex_property')
            result = self._compile_property_task(ops, is_edge=False, is_write=is_write)
        elif task_name in ['update_edge_property', 'get_edge_by_property']:
            is_write = (task_name == 'update_edge_property')
            result = self._compile_property_task(ops, is_edge=True, is_write=is_write)
        elif task_name == 'get_nbrs':
            result = self._compile_get_nbrs(ops, task.get('direction', 'OUT'))
        else:
            return {"task_type": task_name.upper(), "ops_count": 0, "parameters": {}}

        if batch_sizes is not None:
            result['batch_sizes'] = batch_sizes
        return result

    # --- Logic implementation for Restore Mechanism ---

    def _compile_add_vertex(self, ops: int) -> Dict[str, Any]:
        """
        Logic:
        Since the graph is restored, we can safely add the same count of vertices every time.
        The database will assign internal IDs automatically.
        """
        return {
            "task_type": "ADD_VERTEX",
            "ops_count": ops,
            "parameters": {"count": ops}
        }

    def _compile_remove_vertex(self, ops: int) -> Dict[str, Any]:
        """
        Logic:
        Must select nodes from self.sampled_nodes (initial data).
        If we select a randomly generated ID, that node won't exist because the graph was restored,
        making the delete operation invalid (Benchmark failure).
        Sample without replacement to ensure each vertex is only removed once.
        """
        # Deduplicate sampled_nodes first, then sample without replacement
        unique_nodes = list(set(self.sampled_nodes))
        available_nodes = len(unique_nodes)

        if ops > available_nodes:
            print(f"Warning: Requested {ops} remove_vertex ops, but only {available_nodes} unique sampled nodes available. Using {available_nodes}.")
            ops = available_nodes

        ids = random.sample(unique_nodes, ops)
        return {
            "task_type": "REMOVE_VERTEX",
            "ops_count": ops,
            "parameters": {
                "ids": ids,
                "detach": True # Must detach, because initial nodes definitely have edges
            }
        }

    def _compile_add_edge(self, ops: int) -> Dict[str, Any]:
        """
        Logic:
        Both endpoints must be from self.sampled_nodes (initial data).
        Cannot connect to a non-existent ID (e.g., nodes added by other tasks that were wiped).
        """
        pairs = []
        for _ in range(ops):
            pairs.append({
                "src": self._sample_existing_node(),
                "dst": self._sample_existing_node()
            })
        return {
            "task_type": "ADD_EDGE",
            "ops_count": ops,
            "parameters": {"label": "knows", "pairs": pairs}
        }

    def _compile_remove_edge(self, ops: int) -> Dict[str, Any]:
        """
        Logic:
        Must select from self.sampled_edges (initial edges).
        Sample without replacement to ensure each edge is only removed once.
        """
        # Sample without replacement to avoid duplicates
        available_edges = len(self.sampled_edges)
        if ops > available_edges:
            print(f"Warning: Requested {ops} remove_edge ops, but only {available_edges} sampled edges available. Using {available_edges}.")
            ops = available_edges

        sampled = random.sample(self.sampled_edges, ops)
        pairs = [{"src": src, "dst": dst} for src, dst in sampled]

        return {
            "task_type": "REMOVE_EDGE",
            "ops_count": ops,
            "parameters": {"label": "knows", "pairs": pairs}
        }

    def _compile_property_task(self, ops: int, is_edge: bool, is_write: bool) -> Dict[str, Any]:
        """
        Logic:
        Must also target initial data (Existing Data).
        Update: Update properties of initial data.
        Get: Query properties of initial data.
        """
        items = []
        for _ in range(ops):
            if is_edge:
                src, dst = self._sample_existing_edge()
                props = self._generate_deterministic_props(src)
                if is_write:
                    items.append({"src": src, "dst": dst, "properties": props})
                else:
                    key = random.choice(list(props.keys()))
                    items.append({"key": key, "value": props[key]})
            else:
                node_id = self._sample_existing_node()
                props = self._generate_deterministic_props(node_id)
                if is_write:
                    items.append({"id": node_id, "properties": props})
                else:
                    key = random.choice(list(props.keys()))
                    items.append({"key": key, "value": props[key]})

        if is_write:
            task_type = f"UPDATE_{'EDGE' if is_edge else 'VERTEX'}_PROPERTY"
        else:
            task_type = f"GET_{'EDGE' if is_edge else 'VERTEX'}_BY_PROPERTY"
        param_key = "updates" if is_write else "queries"

        res = {
            "task_type": task_type,
            "ops_count": ops,
            "parameters": {param_key: items}
        }
        if is_edge:
            res["parameters"]["label"] = "knows"
        return res

    def _compile_get_nbrs(self, ops: int, direction: str) -> Dict[str, Any]:
        """Query neighbors of initial nodes"""
        ids = [self._sample_existing_node() for _ in range(ops)]
        return {
            "task_type": "GET_NBRS",
            "ops_count": ops,
            "parameters": {"direction": direction, "ids": ids}
        }

    # --- Helpers ---

    def _sample_existing_node(self) -> int:
        """Return only nodes from the initial dataset"""
        if not self.sampled_nodes: return 1
        return random.choice(self.sampled_nodes)

    def _sample_existing_edge(self) -> Tuple[int, int]:
        """Return only edges from the initial dataset"""
        if not self.sampled_edges: return (1, 2)
        return random.choice(self.sampled_edges)

    def _generate_deterministic_props(self, seed_id: int) -> Dict[str, Any]:
        """
        ★ Core Logic: Generate deterministic properties based on ID.
        Even if Get and Update tasks are unrelated, as long as they target the same ID,
        the generated properties are consistent.
        """
        return {
            "weight": round((seed_id % 100) / 100.0, 2),
            "age": seed_id % 90 + 10,
            "flag": (seed_id % 2 == 0)
        }