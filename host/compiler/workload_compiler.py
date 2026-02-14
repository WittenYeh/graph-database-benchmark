"""
WorkloadCompiler - Compiles workload configurations to database-specific query languages
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
        Compile workload to database-specific queries
        Returns path to directory containing compiled JSON files
        """
        if seed is not None:
            random.seed(seed)

        db_config = self.database_config[database_name]
        query_language = db_config['query_language']

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
            queries = self._compile_task(task, query_language, dataset_name)

            # Save to JSON file
            output_file = output_dir / f"{idx:02d}_{task_name}.json"
            metadata = {
                'ops': task.get('ops', 0),
                'client_threads': task.get('client_threads', 1),
                'copy_mode': task.get('copy_mode', False),
                'ratios': task.get('ratios', {})
            }

            # Add optional fields only if present
            if 'batch_size' in task:
                metadata['batch_size'] = task['batch_size']
            if 'latency_test_mode' in task:
                metadata['latency_test_mode'] = task['latency_test_mode']

            with open(output_file, 'w') as f:
                json.dump({
                    'task_name': task_name,
                    'queries': queries,
                    'metadata': metadata
                }, f, indent=2)

        return output_dir

    def _compile_task(self, task: Dict[str, Any], query_language: str, dataset_name: str) -> List[str]:
        """Compile a single task to queries"""
        task_name = task['name']
        ops = task.get('ops', 0)

        if task_name == 'load_graph':
            return self._compile_load_graph(query_language)

        elif 'add_nodes' in task_name:
            return self._compile_add_nodes(query_language, ops)

        elif 'add_edges' in task_name:
            return self._compile_add_edges(query_language, ops)

        elif 'delete_nodes' in task_name:
            return self._compile_delete_nodes(query_language, ops)

        elif 'delete_edges' in task_name:
            return self._compile_delete_edges(query_language, ops)

        elif 'read_nbrs' in task_name:
            return self._compile_read_neighbors(query_language, ops)

        elif 'mixed_workload' in task_name:
            ratios = task.get('ratios', {})
            return self._compile_mixed_workload(query_language, ops, ratios)

        return []

    def _compile_load_graph(self, query_language: str) -> List[str]:
        """Compile load_graph task - returns placeholder"""
        # This is handled specially by the Docker container
        return ["LOAD_GRAPH"]

    def _compile_add_nodes(self, query_language: str, ops: int) -> List[str]:
        """Compile add_nodes queries - generate random node IDs"""
        queries = []
        for i in range(ops):
            # Generate random node ID (silently skips if already exists)
            node_id = random.randint(1000000, 9999999)
            if query_language == 'cypher':
                queries.append(f"CREATE (n:MyNode {{id: {node_id}}})")
            elif query_language == 'gremlin':
                queries.append(f"g.addV('MyNode').property('id', {node_id})")
        return queries

    def _compile_add_edges(self, query_language: str, ops: int) -> List[str]:
        """Compile add_edges queries - sample two random nodes from dataset"""
        queries = []
        for i in range(ops):
            # Sample two random lines from dataset to get node IDs
            src_id = self._sample_node_from_dataset()
            dst_id = self._sample_node_from_dataset()

            # Silently skips if source or destination node doesn't exist
            if query_language == 'cypher':
                queries.append(f"MATCH (a:MyNode), (b:MyNode) WHERE a.id = {src_id} AND b.id = {dst_id} CREATE (a)-[:MyEdge]->(b)")
            elif query_language == 'gremlin':
                queries.append(f"g.V().has('id', {src_id}).addE('MyEdge').to(__.V().has('id', {dst_id}))")
        return queries

    def _compile_delete_nodes(self, query_language: str, ops: int) -> List[str]:
        """Compile delete_nodes queries - sample random nodes from dataset"""
        queries = []
        for i in range(ops):
            # Sample first node from a random line in dataset
            node_id = self._sample_node_from_dataset()

            # Silently succeeds (no-op) if node doesn't exist
            if query_language == 'cypher':
                queries.append(f"MATCH (n:MyNode {{id: {node_id}}}) DETACH DELETE n")
            elif query_language == 'gremlin':
                queries.append(f"g.V().has('id', {node_id}).drop()")
        return queries

    def _compile_delete_edges(self, query_language: str, ops: int) -> List[str]:
        """Compile delete_edges queries - sample random edges from dataset"""
        queries = []
        for i in range(ops):
            # Sample a random edge from dataset
            src_id, dst_id = self._sample_edge_from_dataset()

            # Silently succeeds (no-op) if edge doesn't exist
            if query_language == 'cypher':
                queries.append(f"MATCH (a:MyNode {{id: {src_id}}})-[r:MyEdge]->(b:MyNode {{id: {dst_id}}}) DELETE r")
            elif query_language == 'gremlin':
                queries.append(f"g.V().has('id', {src_id}).outE('MyEdge').where(inV().has('id', {dst_id})).drop()")
        return queries

    def _compile_read_neighbors(self, query_language: str, ops: int) -> List[str]:
        """Compile read_neighbors queries - sample random nodes from dataset"""
        queries = []
        for i in range(ops):
            # Sample first node from a random line in dataset
            node_id = self._sample_node_from_dataset()

            if query_language == 'cypher':
                queries.append(f"MATCH (n:MyNode {{id: {node_id}}})-[:MyEdge]->(m) RETURN m.id")
            elif query_language == 'gremlin':
                queries.append(f"g.V().has('id', {node_id}).out('MyEdge').values('id')")
        return queries

    def _compile_mixed_workload(self, query_language: str, ops: int, ratios: Dict[str, float]) -> List[str]:
        """Compile mixed workload with operation ratios - reuse existing compile functions"""
        queries = []
        operations = []

        # Build operation list based on ratios
        for op_type, ratio in ratios.items():
            count = int(ops * ratio)
            operations.extend([op_type] * count)

        # Shuffle operations
        random.shuffle(operations)

        # Generate queries for each operation by calling existing compile functions
        for op_type in operations:
            if op_type == 'add_node':
                queries.extend(self._compile_add_nodes(query_language, 1))
            elif op_type == 'add_edge':
                queries.extend(self._compile_add_edges(query_language, 1))
            elif op_type == 'delete_node':
                queries.extend(self._compile_delete_nodes(query_language, 1))
            elif op_type == 'delete_edge':
                queries.extend(self._compile_delete_edges(query_language, 1))
            elif op_type == 'read_nbrs':
                queries.extend(self._compile_read_neighbors(query_language, 1))

        return queries

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
