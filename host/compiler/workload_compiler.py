"""
WorkloadCompiler - Optimized for "Reset/Restore" Benchmark Model
"""
import csv
import json
import random
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

STRUCTURAL_TASKS = {'load_graph', 'add_vertex', 'remove_vertex', 'add_edge', 'remove_edge', 'get_nbrs'}
PROPERTY_TASKS = {'load_graph', 'update_vertex_property', 'update_edge_property',
                  'get_vertex_by_property', 'get_edge_by_property'}

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

        # Property metadata from CSV headers
        self.node_property_keys: List[str] = []   # e.g. ["name", "age"]
        self.edge_property_keys: List[str] = []   # e.g. ["weight", "type"]

        # Sampled property values: node_id -> {key: value}, edge (src,dst) -> {key: value}
        self.sampled_node_props: Dict[int, Dict[str, str]] = {}
        self.sampled_edge_props: Dict[Tuple[int, int], Dict[str, str]] = {}

    def compile_workload(
        self,
        workload_config: Dict[str, Any],
        dataset_name: str,
        seed: Optional[int] = None,
        dataset_path: Optional[Path] = None
    ) -> Path:
        if seed is not None:
            random.seed(seed)

        # Validate mode
        mode = workload_config.get('mode', 'structural')
        valid_tasks = STRUCTURAL_TASKS if mode == 'structural' else PROPERTY_TASKS
        tasks = workload_config.get('tasks', [])
        for task in tasks:
            if task['name'] not in valid_tasks:
                raise ValueError(
                    f"Task '{task['name']}' is not valid for mode '{mode}'. "
                    f"Valid tasks: {valid_tasks}"
                )

        # Scan the initial dataset (Benchmark Baseline)
        if dataset_path:
            self._scan_dataset(dataset_path)

        # Clean up the dataset-specific compiled workload directory (database-agnostic)
        output_dir = Path(f"workloads/compiled/{dataset_name}")
        if output_dir.exists():
            import shutil
            print(f"  🧹 Removing old compiled workload: {output_dir}")
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        for idx, task in enumerate(tasks):
            task_name = task['name']
            workload_data = self._compile_task(task)

            output_file = output_dir / f"{idx:02d}_{task_name}.json"
            with open(output_file, 'w') as f:
                json.dump(workload_data, f, indent=2)

        return output_dir

    def _scan_dataset(self, dataset_path: Path):
        """Scan dataset directory containing nodes.csv and edges.csv using pandas for speed"""
        dataset_dir = Path(dataset_path)

        # Scan nodes.csv with pandas
        nodes_file = dataset_dir / 'nodes.csv'
        print(f"Scanning nodes: {nodes_file}...")

        try:
            # Read CSV with pandas (much faster than csv.DictReader)
            nodes_df = pd.read_csv(nodes_file)
            node_count = len(nodes_df)

            # Get property columns
            self.node_property_keys = [c for c in nodes_df.columns if c != 'node_id']

            # Get max ID
            self.max_dataset_id = int(nodes_df['node_id'].max())

            # Sample nodes
            if node_count > self.SAMPLE_SIZE:
                sampled_df = nodes_df.sample(n=self.SAMPLE_SIZE, random_state=random.randint(0, 1000000))
            else:
                sampled_df = nodes_df

            # Extract sampled node IDs
            self.sampled_nodes = sampled_df['node_id'].astype(int).tolist()

            # Store property values for sampled nodes (only if properties exist)
            if self.node_property_keys:
                for _, row in sampled_df.iterrows():
                    node_id = int(row['node_id'])
                    props = {k: str(row[k]) for k in self.node_property_keys if pd.notna(row[k])}
                    if props:
                        self.sampled_node_props[node_id] = props

        except Exception as e:
            print(f"Error scanning nodes: {e}")
            raise

        # Scan edges.csv with pandas
        edges_file = dataset_dir / 'edges.csv'
        print(f"Scanning edges: {edges_file}...")

        try:
            # Read CSV with pandas
            edges_df = pd.read_csv(edges_file)
            edge_count = len(edges_df)

            # Get property columns
            self.edge_property_keys = [c for c in edges_df.columns if c not in ('src', 'dst')]

            # Sample edges
            if edge_count > self.SAMPLE_SIZE:
                sampled_df = edges_df.sample(n=self.SAMPLE_SIZE, random_state=random.randint(0, 1000000))
            else:
                sampled_df = edges_df

            # Extract sampled edges
            self.sampled_edges = [(int(row['src']), int(row['dst']))
                                  for _, row in sampled_df.iterrows()]

            # Store property values for sampled edges (only if properties exist)
            if self.edge_property_keys:
                for _, row in sampled_df.iterrows():
                    src, dst = int(row['src']), int(row['dst'])
                    props = {k: str(row[k]) for k in self.edge_property_keys if pd.notna(row[k])}
                    if props:
                        self.sampled_edge_props[(src, dst)] = props

        except Exception as e:
            print(f"Error scanning edges: {e}")
            raise

        print(f"Baseline Scanned. Nodes: {node_count}, Edges: {edge_count}, Max ID: {self.max_dataset_id}")
        if self.node_property_keys:
            print(f"  Node properties: {self.node_property_keys}")
        if self.edge_property_keys:
            print(f"  Edge properties: {self.edge_property_keys}")

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
        return {
            "task_type": "ADD_VERTEX",
            "ops_count": ops,
            "parameters": {"count": ops}
        }

    def _compile_remove_vertex(self, ops: int) -> Dict[str, Any]:
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
                "detach": True
            }
        }

    def _compile_add_edge(self, ops: int) -> Dict[str, Any]:
        pairs = []
        for _ in range(ops):
            pairs.append({
                "src": self._sample_existing_node(),
                "dst": self._sample_existing_node()
            })
        return {
            "task_type": "ADD_EDGE",
            "ops_count": ops,
            "parameters": {"label": "MyEdge", "pairs": pairs}
        }

    def _compile_remove_edge(self, ops: int) -> Dict[str, Any]:
        available_edges = len(self.sampled_edges)
        if ops > available_edges:
            print(f"Warning: Requested {ops} remove_edge ops, but only {available_edges} sampled edges available. Using {available_edges}.")
            ops = available_edges

        sampled = random.sample(self.sampled_edges, ops)
        pairs = [{"src": src, "dst": dst} for src, dst in sampled]

        return {
            "task_type": "REMOVE_EDGE",
            "ops_count": ops,
            "parameters": {"label": "MyEdge", "pairs": pairs}
        }

    def _compile_property_task(self, ops: int, is_edge: bool, is_write: bool) -> Dict[str, Any]:
        """
        For Update: target existing nodes/edges, modify existing property keys with new values.
        For Get: query using existing key-value pairs from the dataset.
        """
        items = []
        for _ in range(ops):
            if is_edge:
                src, dst = self._sample_existing_edge()
                existing_props = self.sampled_edge_props.get((src, dst), {})
                prop_keys = self.edge_property_keys
                if is_write:
                    # Update: use existing keys, generate new values
                    new_props = self._generate_new_values(prop_keys, src)
                    items.append({"src": src, "dst": dst, "properties": new_props})
                else:
                    # Get: use an actual key-value pair from the dataset
                    kv = self._pick_existing_kv(existing_props, prop_keys, src)
                    items.append(kv)
            else:
                node_id = self._sample_existing_node()
                existing_props = self.sampled_node_props.get(node_id, {})
                prop_keys = self.node_property_keys
                if is_write:
                    new_props = self._generate_new_values(prop_keys, node_id)
                    items.append({"id": node_id, "properties": new_props})
                else:
                    kv = self._pick_existing_kv(existing_props, prop_keys, node_id)
                    items.append(kv)

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
            res["parameters"]["label"] = "MyEdge"
        return res

    def _compile_get_nbrs(self, ops: int, direction: str) -> Dict[str, Any]:
        ids = [self._sample_existing_node() for _ in range(ops)]
        return {
            "task_type": "GET_NBRS",
            "ops_count": ops,
            "parameters": {"direction": direction, "ids": ids}
        }

    # --- Helpers ---

    def _sample_existing_node(self) -> int:
        if not self.sampled_nodes: return 1
        return random.choice(self.sampled_nodes)

    def _sample_existing_edge(self) -> Tuple[int, int]:
        if not self.sampled_edges: return (1, 2)
        return random.choice(self.sampled_edges)

    def _generate_new_values(self, prop_keys: List[str], seed_id: int) -> Dict[str, Any]:
        """Generate new property values for existing keys (for Update operations)."""
        if not prop_keys:
            return {"_bench_prop": f"val_{seed_id}"}
        props = {}
        for key in prop_keys:
            props[key] = f"updated_{seed_id}_{key}"
        return props

    def _pick_existing_kv(self, existing_props: Dict[str, str], prop_keys: List[str], seed_id: int) -> Dict[str, Any]:
        """Pick an existing key-value pair for Get operations."""
        # If we have actual property data for this entity, use it
        if existing_props:
            key = random.choice(list(existing_props.keys()))
            return {"key": key, "value": existing_props[key]}
        # Fallback: use a known key with a generated value
        if prop_keys:
            key = random.choice(prop_keys)
            return {"key": key, "value": f"updated_{seed_id}_{key}"}
        return {"key": "_bench_prop", "value": f"val_{seed_id}"}
