# Graph Database Benchmark

An embedded graph database benchmark framework for testing Neo4j and JanusGraph performance.

## Project Structure

```
graph-database-benchmark/
├── host/                          # Python host implementation
│   ├── benchmark_launcher.py      # Main entry point
│   ├── compiler/                  # Workload compiler
│   │   └── workload_compiler.py
│   ├── dataset/                   # Dataset loader
│   │   └── dataset_loader.py
│   ├── db/                        # Docker manager
│   │   └── docker_manager.py
│   └── report/                    # Report generator
│       └── report_generator.py
├── docker/                        # Docker container implementations
│   ├── neo4j/                     # Neo4j embedded benchmark
│   │   ├── Dockerfile
│   │   ├── pom.xml
│   │   └── src/main/java/com/graphbench/neo4j/
│   │       ├── BenchmarkServer.java
│   │       └── Neo4jBenchmarkExecutor.java
│   └── janusgraph/                # JanusGraph embedded benchmark
│       ├── Dockerfile
│       ├── pom.xml
│       └── src/main/java/com/graphbench/janusgraph/
│           ├── BenchmarkServer.java
│           └── JanusGraphBenchmarkExecutor.java
├── config/                        # Configuration files
│   ├── database-config.json
│   └── datasets.json
├── workloads/                     # Workload templates
│   └── templates/
│       └── example_workload.json
├── graph-datasets/                # Dataset files (submodule)
├── reports/                       # Benchmark results (generated)
├── visualizations/                # Visualization outputs (generated)
├── requirements.txt               # Python dependencies
├── visualize.py                   # Visualization tool
└── README.md
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         HOST (Python)                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │  Benchmark   │  │   Workload   │  │    Docker    │              │
│  │   Launcher   │─▶│   Compiler   │─▶│   Manager    │              │
│  └──────────────┘  └──────────────┘  └──────┬───────┘              │
│                                              │                       │
│  ┌──────────────┐  ┌──────────────┐         │                       │
│  │    Report    │  │   Dataset    │         │                       │
│  │  Generator   │  │    Loader    │         │                       │
│  └──────────────┘  └──────────────┘         │                       │
└─────────────────────────────────────────────┼───────────────────────┘
                                               │
                        ┌──────────────────────┴──────────────────────┐
                        │                                              │
         ┌──────────────▼──────────────┐        ┌─────────────────────▼────┐
         │   DOCKER: Neo4j Embedded    │        │ DOCKER: JanusGraph+BDB   │
         │  ┌────────────────────────┐ │        │ ┌──────────────────────┐ │
         │  │  BenchmarkServer       │ │        │ │  BenchmarkServer     │ │
         │  │  (HTTP API: 8080)      │ │        │ │  (HTTP API: 8081)    │ │
         │  └────────────────────────┘ │        │ └──────────────────────┘ │
         │  ┌────────────────────────┐ │        │ ┌──────────────────────┐ │
         │  │ Neo4jBenchmarkExecutor │ │        │ │ JanusGraphExecutor   │ │
         │  │ - Load MTX datasets    │ │        │ │ - Load MTX datasets  │ │
         │  │ - Execute Cypher       │ │        │ │ - Execute Gremlin    │ │
         │  │ - Measure latency      │ │        │ │ - Measure latency    │ │
         │  │ - Measure throughput   │ │        │ │ - Measure throughput │ │
         │  └────────────────────────┘ │        │ └──────────────────────┘ │
         └─────────────────────────────┘        └──────────────────────────┘
```

The benchmark framework consists of two main components:

### Host (Python)
- **BenchmarkLauncher**: Orchestrates the entire benchmark workflow
- **WorkloadCompiler**: Compiles workload configurations to database-specific query languages (Cypher for Neo4j, Gremlin for JanusGraph)
- **DockerManager**: Manages Docker containers and communicates with benchmark servers
- **ReportGenerator**: Generates visualizations from benchmark results using Seaborn

### Docker Container (Java)
- **BenchmarkServer**: HTTP API server that receives workload files and returns metrics
- **BenchmarkExecutor**: Instantiates embedded database, loads datasets, executes queries, and measures performance
- Each container is isolated and provides clean benchmarking environment
- Execution time is pure (excludes network transmission time)

## Features

- Support for Neo4j Embedded and JanusGraph with BerkeleyDB backend
- Flexible workload configuration with multiple task types
- Latency and throughput testing
- Mixed workload support with configurable operation ratios
- Beautiful visualizations with Seaborn
- Docker-based isolation for clean benchmarking
- Reproducible results with seed support

## Installation

### Prerequisites

- Python 3.8+
- Docker
- Java 21 (for Neo4j), Java 17 (for JanusGraph)

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd graph-database-benchmark

# Initialize submodules (for datasets)
git submodule update --init --recursive

# Install Python dependencies
pip install -r requirements.txt

# Build Docker images
./build.sh
```

## Usage

### Quick Start with Shell Script

The easiest way to run benchmarks using named arguments:

```bash
# Single database, single dataset
./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP

# Single database, multiple datasets
./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP,delaunay_n13

# Multiple databases, single dataset
./run_benchmark.sh --database neo4j,janusgraph --dataset coAuthorsDBLP

# Multiple databases, multiple datasets (runs all combinations)
./run_benchmark.sh --database neo4j,janusgraph --dataset coAuthorsDBLP,delaunay_n13

# Custom workload configuration
./run_benchmark.sh --database neo4j --dataset coAuthorsDBLP --workload workloads/templates/quick_test.json

# Short form options
./run_benchmark.sh -d neo4j,janusgraph -s coAuthorsDBLP,delaunay_n13

# Show help
./run_benchmark.sh --help
```

Legacy positional arguments are still supported:
```bash
./run_benchmark.sh neo4j coAuthorsDBLP
./run_benchmark.sh neo4j coAuthorsDBLP workloads/templates/quick_test.json
```

### Using Python Directly

For more control, use the Python launcher directly:

```bash
# Basic usage
python host/benchmark_launcher.py \
  --database-name neo4j \
  --dataset-name coAuthorsDBLP \
  --workload-config workloads/templates/example_workload.json \
  --output-dir reports

# Test multiple datasets
python host/benchmark_launcher.py \
  --database-name janusgraph \
  --dataset-name coAuthorsDBLP delaunay_n13 \
  --output-dir reports

# With reproducible seed
python host/benchmark_launcher.py \
  --database-name neo4j \
  --dataset-name coAuthorsDBLP \
  --seed 42 \
  --output-dir reports

# Force rebuild Docker image
python host/benchmark_launcher.py \
  --database-name neo4j \
  --dataset-name coAuthorsDBLP \
  --rebuild
```

### Command Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--database-name` | (required) | Database to benchmark: `neo4j` or `janusgraph` |
| `--database-config` | `config/database-config.json` | Path to database configuration file |
| `--dataset-config` | `config/datasets.json` | Path to dataset configuration file |
| `--dataset-name` | (required) | Dataset name(s) to test (can specify multiple) |
| `--workload-config` | `workloads/templates/example_workload.json` | Path to workload configuration file |
| `--workload-name` | (all workloads) | Specific workload name to run |
| `--output-dir` | `reports/` | Output directory for benchmark reports |
| `--seed` | (none) | Random seed for reproducibility |
| `--rebuild` | off | Force rebuild the Docker image |

### Visualizing Results

Generate comparison visualizations from multiple benchmark reports:

```bash
python visualize.py \
  reports/bench_neo4j_coAuthorsDBLP.json \
  reports/bench_janusgraph_coAuthorsDBLP.json \
  --output-dir visualizations
```

This will generate:
- `latency_comparison.png`: P50, P90, P95, P99 latency comparison
- `throughput_comparison.png`: Throughput comparison across tasks
- `duration_comparison.png`: Task duration comparison

## Configuration

### Dataset Configuration (`config/datasets.json`)

Maps dataset names to their `.mtx` file paths:

```json
{
  "root_dir": "./graph-datasets/",
  "datasets": {
    "coAuthorsDBLP": "coAuthorsDBLP/coAuthorsDBLP.mtx",
    "delaunay_n13": "delaunay_n13/delaunay_n13.mtx",
    "cit-Patents": "cit-Patents/cit-Patents.mtx"
  }
}
```

### Database Configuration (`config/database-config.json`)

Defines database-specific settings:

```json
{
  "neo4j": {
    "docker_image": "bench-neo4j",
    "dockerfile_path": "./docker/neo4j/Dockerfile",
    "container_name": "neo4j-benchmark",
    "api_port": 8080,
    "query_language": "cypher",
    "runtime": "java",
    "config": {
      "heap_size": "4G",
      "page_cache": "2G"
    }
  },
  "janusgraph": {
    "docker_image": "bench-janusgraph",
    "dockerfile_path": "./docker/janusgraph/Dockerfile",
    "container_name": "janusgraph-benchmark",
    "api_port": 8081,
    "query_language": "gremlin",
    "runtime": "java",
    "config": {
      "heap_size": "4G",
      "storage_backend": "berkeleyje"
    }
  }
}
```

### Workload Configuration

Example workload with multiple tasks:

```json
{
  "server_config": {
    "threads": 8
  },
  "tasks": [
    { "name": "load_graph" },
    { "name": "add_nodes_latency", "ops": 5000 },
    { "name": "add_nodes_throughput", "ops": 20000, "client_threads": 8 },
    { "name": "add_edges_latency", "ops": 5000 },
    { "name": "delete_nodes_latency", "ops": 1000, "copy_mode": true },
    { "name": "read_nbrs_latency", "ops": 1000 },
    { "name": "read_nbrs_throughput", "ops": 50000, "client_threads": 16 },
    {
      "name": "mixed_workload_latency",
      "ops": 10000,
      "ratios": {
        "add_node": 0.1,
        "add_edge": 0.2,
        "delete_node": 0.05,
        "delete_edge": 0.05,
        "read_nbrs": 0.6
      }
    }
  ]
}
```

#### Task Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | string | required | Task name (see Supported Tasks below) |
| `ops` | integer | required | Number of operations to execute |
| `client_threads` | integer | 1 | Number of concurrent client threads |
| `ratios` | object | - | Operation mix ratios for mixed workloads (must sum to 1.0) |
| `copy_mode` | boolean | false | Reload original graph before executing this task |

## Supported Tasks

### Task Semantics

Each task type has specific behavior and error handling semantics. The workload compiler uses **memory-efficient streaming** to sample nodes and edges from the dataset without loading the entire graph into memory.

**NOTICE: Latency Test Execution**

For latency tests (single-threaded execution), queries are executed serially with individual transactions per query:

- Each query is executed independently with its own transaction
- Queries are executed sequentially (thread pool size = 1) to ensure strict serial execution
- Individual query latencies are measured and reported (P50, P90, P95, P99, mean)

For throughput tests (multi-threaded execution), queries are executed concurrently with individual transactions per query to maximize parallelism.

#### 1. `load_graph`
- **Description**: Bulk-loads the entire dataset from MTX file
- **Behavior**:
  - Parses MTX file to extract nodes and edges
  - Creates all nodes first, then creates edges in batches
  - Saves a snapshot for `copy_mode` restoration
- **Error Handling**: Fails if dataset file is invalid or inaccessible

#### 2. `add_nodes_latency` / `add_nodes_throughput`
- **Description**: Inserts new nodes into the graph
- **Behavior**:
  - Generates random node IDs (e.g., `1234567`)
  - Creates nodes with label `MyNode` and property `id`
  - Latency mode: single-threaded, measures per-operation latency
  - Throughput mode: multi-threaded, measures operations per second
- **Cypher**: `CREATE (n:MyNode {id: 1234567})`
- **Gremlin**: `g.addV('MyNode').property('id', 1234567)`
- **Error Handling**: Silently skips if node ID already exists (idempotent)

#### 3. `add_edges_latency` / `add_edges_throughput`
- **Description**: Inserts new edges between existing nodes
- **Behavior**:
  - Randomly samples two node IDs by reading random lines from the dataset
  - Creates directed edge with label `MyEdge`
  - May create duplicate edges between same node pairs
- **Sampling Strategy**: Reads two random lines from MTX file, extracts node IDs
- **Cypher**: `MATCH (a:MyNode), (b:MyNode) WHERE a.id = 123 AND b.id = 456 CREATE (a)-[:MyEdge]->(b)`
- **Gremlin**: `g.V().has('id', 123).addE('MyEdge').to(__.V().has('id', 456))`
- **Error Handling**: Silently skips if source or destination node doesn't exist

#### 4. `delete_nodes_latency` / `delete_nodes_throughput`
- **Description**: Deletes nodes and their incident edges
- **Behavior**:
  - Randomly samples node IDs by reading random lines from the dataset
  - Deletes node and all connected edges (DETACH DELETE in Cypher)
- **Sampling Strategy**: Reads a random line from MTX file, extracts first node ID
- **Cypher**: `MATCH (n:MyNode {id: 123}) DETACH DELETE n`
- **Gremlin**: `g.V().has('id', 123).drop()`
- **Error Handling**: Silently succeeds (no-op) if node doesn't exist

#### 5. `delete_edges_latency` / `delete_edges_throughput`
- **Description**: Deletes edges between nodes
- **Behavior**:
  - Randomly samples edges by reading random lines from the dataset
  - Deletes the specific edge between two nodes
- **Sampling Strategy**: Reads a random line from MTX file, uses both node IDs as edge
- **Cypher**: `MATCH (a:MyNode {id: 123})-[r:MyEdge]->(b:MyNode {id: 456}) DELETE r`
- **Gremlin**: `g.V().has('id', 123).outE('MyEdge').where(inV().has('id', 456)).drop()`
- **Error Handling**: Silently succeeds (no-op) if edge doesn't exist

#### 6. `read_nbrs_latency` / `read_nbrs_throughput`
- **Description**: Reads outgoing neighbors of a node
- **Behavior**:
  - Randomly samples node IDs by reading random lines from the dataset
  - Retrieves all outgoing neighbors via `MyEdge` relationships
  - Returns neighbor node IDs
- **Sampling Strategy**: Reads a random line from MTX file, extracts first node ID
- **Cypher**: `MATCH (n:MyNode {id: 123})-[:MyEdge]->(m) RETURN m.id`
- **Gremlin**: `g.V().has('id', 123).out('MyEdge').values('id')`
- **Error Handling**: Returns empty result if node doesn't exist or has no neighbors

#### 7. `mixed_workload_latency` / `mixed_workload_throughput`
- **Description**: Executes a mix of operations with configurable ratios
- **Behavior**:
  - Combines add_node, add_edge, delete_node, delete_edge, and read_nbrs operations
  - Operations are shuffled randomly
  - Ratios must sum to 1.0
- **Example**:
  ```json
  {
    "name": "mixed_workload_latency",
    "ops": 10000,
    "ratios": {
      "add_node": 0.1,
      "add_edge": 0.2,
      "delete_node": 0.05,
      "delete_edge": 0.05,
      "read_nbrs": 0.6
    }
  }
  ```
- **Error Handling**: Each operation follows its individual error handling semantics

### Memory-Efficient Workload Compilation

The workload compiler uses a **streaming approach** to avoid loading entire datasets into memory:

1. **Counting Phase**: Counts the number of edges in the dataset (one pass through file)
2. **Sampling Phase**: For each query, reads a random line from the dataset on-demand
3. **Memory Usage**: Only stores the line count and dataset path, not all nodes/edges

This approach allows benchmarking on very large datasets (millions of edges) without memory constraints.

### Task Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | string | required | Task name (see task list above) |
| `ops` | integer | required | Number of operations to execute |
| `client_threads` | integer | 1 | Number of concurrent client threads (for throughput tasks) |
| `ratios` | object | - | Operation mix ratios for mixed workloads (must sum to 1.0) |
| `copy_mode` | boolean | false | Reload original graph before executing this task |

### Copy Mode

When `copy_mode: true` is set, the task will restore the graph to its original state (after `load_graph`) before execution. This is useful for:
- **Delete operations**: Ensure nodes/edges from the original graph exist
- **Consistent testing**: Test on the same graph structure across multiple runs
- **Avoiding interference**: Previous add operations won't affect delete/read tests

**Example**:
```json
{
  "tasks": [
    { "name": "load_graph" },
    { "name": "add_nodes_latency", "ops": 5000 },
    { "name": "delete_nodes_latency", "ops": 1000, "copy_mode": true }
  ]
}
```
In this example, `delete_nodes_latency` will test on the original graph, not the graph modified by `add_nodes_latency`.

### Error Handling Philosophy

The benchmark framework follows a **silent failure** approach for most operations:
- **Add operations**: Skip if entity already exists (idempotent)
- **Delete operations**: No-op if entity doesn't exist (idempotent)
- **Read operations**: Return empty results if entity doesn't exist

This design ensures:
1. **Consistent metrics**: Failed operations don't skew latency measurements
2. **Realistic workloads**: Real-world systems often handle missing entities gracefully
3. **Continuous execution**: Benchmark doesn't stop on individual operation failures

All operations are counted in `totalOps` regardless of success/failure, providing accurate throughput measurements.

## Output Format

Benchmark results are saved as JSON files with the naming pattern: `bench_{database}_{dataset}.json`

Example output:

```json
{
  "metadata": {
    "database": "neo4j",
    "dataset": "delaunay_n13",
    "datasetPath": "./graph-datasets/delaunay_n13/delaunay_n13.mtx",
    "timestamp": "2026-02-12T09:21:04.532605396Z",
    "serverThreads": 8,
    "javaVersion": "17.0.18",
    "osName": "Linux",
    "osVersion": "5.19.0-1010-nvidia-lowlatency"
  },
  "results": [
    {
      "task": "load_graph",
      "status": "success",
      "durationSeconds": 1.356723384,
      "totalOps": 32739,
      "clientThreads": 0
    },
    {
      "task": "add_nodes_latency",
      "status": "success",
      "durationSeconds": 15.26413909,
      "totalOps": 5000,
      "clientThreads": 1,
      "latency": {
        "minUs": 1309.674,
        "maxUs": 1121683.218,
        "meanUs": 3050.2426564,
        "medianUs": 2310.056,
        "p50Us": 2310.056,
        "p90Us": 5032.378,
        "p95Us": 6839.976,
        "p99Us": 10948.379
      }
    },
    {
      "task": "read_nbrs_throughput",
      "status": "success",
      "durationSeconds": 8.234567,
      "totalOps": 50000,
      "clientThreads": 16
    }
  ]
}
```

## How It Works

1. **Parameter Configuration**: BenchmarkLauncher reads database, dataset, and workload configurations
2. **Workload Compilation**: WorkloadCompiler translates workload tasks into database-specific query languages (Cypher/Gremlin) and saves them as JSON files
3. **Docker Container Startup**: DockerManager builds/starts Docker container with mounted dataset and compiled workload directories
4. **Benchmark Execution**: Container's BenchmarkExecutor:
   - Initializes embedded database
   - Loads dataset from MTX file
   - Executes each workload task sequentially
   - Measures latency/throughput with pure execution time (no network overhead)
   - Returns results via HTTP API
5. **Result Collection**: Host collects results and saves to JSON file
6. **Visualization**: ReportGenerator creates comparison charts using Seaborn

## Database Versions

- **Neo4j**: 2026.01.4 (requires Java 21)
- **JanusGraph**: 1.2.0-20251114-142114.b424a8f with BerkeleyDB backend (requires Java 17)

## Design Principles

1. **Separation of Concerns**: Host handles orchestration and compilation; containers handle pure execution
2. **Language Agnostic Containers**: Containers only execute queries from JSON files, agnostic to task semantics
3. **Clean Benchmarking**: Docker isolation ensures reproducible, interference-free measurements
4. **Pure Execution Time**: Timing excludes network transmission and only measures query execution
5. **Extensibility**: Easy to add new databases by implementing a new Docker container with the same API

## License

MIT
