# Graph Database Benchmark

An embedded graph database benchmark framework for testing Neo4j, JanusGraph, and ArangoDB **latency performance** using **native APIs** instead of query engines.

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
├── common/                        # Shared Java components
│   └── src/main/java/com/graphbench/
│       ├── api/                   # Core interfaces
│       │   ├── BenchmarkExecutor.java
│       │   └── WorkloadDispatcher.java
│       └── workload/              # Workload data models
│           ├── WorkloadTask.java
│           ├── AddVertexParams.java
│           ├── AddEdgeParams.java
│           └── ...
├── common-cpp/                    # Shared C++ components (headers-only)
│   ├── CMakeLists.txt
│   └── include/graphbench/
│       ├── benchmark_executor.hpp      # CRTP base class
│       ├── property_benchmark_executor.hpp
│       ├── progress_callback.hpp
│       └── benchmark_utils.hpp
├── docker/                        # Docker container implementations
│   ├── neo4j/                     # Neo4j embedded benchmark (Java)
│   │   ├── Dockerfile
│   │   ├── pom.xml
│   │   └── src/main/java/com/graphbench/neo4j/
│   │       ├── BenchmarkServer.java
│   │       └── Neo4jBenchmarkExecutor.java
│   ├── janusgraph/                # JanusGraph embedded benchmark (Java)
│   │   ├── Dockerfile
│   │   ├── pom.xml
│   │   └── src/main/java/com/graphbench/janusgraph/
│   │       ├── BenchmarkServer.java
│   │       └── JanusGraphBenchmarkExecutor.java
│   └── arangodb/                  # ArangoDB benchmark (C++)
│       ├── Dockerfile
│       ├── CMakeLists.txt
│       └── src/
│           ├── arango_utils.hpp
│           ├── arangodb_graph_loader.hpp
│           ├── arangodb_benchmark_executor.hpp
│           ├── arangodb_property_benchmark_executor.hpp
│           └── arangodb_benchmark_server.cpp
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
         │  │  (HTTP API: 50080)     │ │        │ │  (HTTP API: 50081)   │ │
         │  └────────────────────────┘ │        │ └──────────────────────┘ │
         │  ┌────────────────────────┐ │        │ ┌──────────────────────┐ │
         │  │ Neo4jBenchmarkExecutor │ │        │ │ JanusGraphExecutor   │ │
         │  │  (Native Java API)     │ │        │ │ (TinkerPop API)      │ │
         │  └────────────────────────┘ │        │ └──────────────────────┘ │
         └─────────────────────────────┘        └──────────────────────────┘

         ┌─────────────────────▼────┐
         │ DOCKER: ArangoDB         │
         │ ┌──────────────────────┐ │
         │ │  BenchmarkServer     │ │
         │ │  (HTTP API: 50082)   │ │
         │ └──────────────────────┘ │
         │ ┌──────────────────────┐ │
         │ │ ArangoDBExecutor     │ │
         │ │ (Fuerte C++ API)     │ │
         │ └──────────────────────┘ │
         └──────────────────────────┘
```

The benchmark framework consists of two main components:

### Host (Python)
- **BenchmarkLauncher**: Orchestrates the entire benchmark workflow
- **WorkloadCompiler**: Generates native API workload JSON files with structured parameters
- **DockerManager**: Manages Docker containers and communicates with benchmark servers
- **ReportGenerator**: Generates visualizations from benchmark results using Seaborn

### Docker Container (Java/C++)
- **BenchmarkServer**: HTTP API server that receives execution requests and returns metrics
- **WorkloadDispatcher**: Reads JSON workload files and dispatches operations to executor
- **BenchmarkExecutor**: Executes operations serially using native APIs, measures per-operation latency
- Each container is isolated and provides clean benchmarking environment
- Execution time is pure (excludes network transmission time)

## Features

- **Native API Execution**: Direct database API calls (Neo4j Embedded API, TinkerPop Structure API, ArangoDB Fuerte C++ API) instead of query engines
- **Latency Testing**: Serial execution with per-operation latency tracking in microseconds
- **10 Benchmark Operations**: ADD_VERTEX, UPSERT_VERTEX_PROPERTY, REMOVE_VERTEX, ADD_EDGE, UPSERT_EDGE_PROPERTY, REMOVE_EDGE, GET_NBRS, GET_VERTEX_BY_PROPERTY, GET_EDGE_BY_PROPERTY, LOAD_GRAPH
- Support for Neo4j Embedded, JanusGraph with BerkeleyDB backend, and ArangoDB
- **Multi-Language Support**: Java (Neo4j, JanusGraph) and C++ (ArangoDB) implementations
- **Static Polymorphism**: C++ implementation uses CRTP for zero-overhead abstraction
- Optimized graph loading with batch insertion
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
| `--database-name` | (required) | Database to benchmark: `neo4j`, `janusgraph`, or `arangodb` |
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
  "tasks": [
    { "name": "load_graph" },
    { "name": "add_vertex", "ops": 5000 },
    { "name": "upsert_vertex_property", "ops": 2000 },
    { "name": "remove_vertex", "ops": 1000 },
    { "name": "add_edge", "ops": 5000 },
    { "name": "upsert_edge_property", "ops": 2000 },
    { "name": "remove_edge", "ops": 1000 },
    { "name": "get_nbrs", "ops": 10000, "direction": "OUT" },
    { "name": "get_vertex_by_property", "ops": 1000 },
    { "name": "get_edge_by_property", "ops": 1000 }
  ]
}
```

#### Task Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | string | required | Task name (see Supported Operations below) |
| `ops` | integer | required | Number of operations to execute |
| `direction` | string | "OUT" | Direction for GET_NBRS: "OUT", "IN", or "BOTH" |

## Supported Operations

The benchmark supports 10 operations using native database APIs:

### 1. LOAD_GRAPH
- **Description**: Bulk-loads the entire dataset from MTX file
- **Implementation**: Two-pass streaming approach
  - Pass 1: Collect unique node IDs
  - Pass 2: Stream through file again to create edges
- **Optimization**: Batch commits every 10,000 operations, schema index creation
- **Error Handling**: Fails if dataset file is invalid or inaccessible

### 2. ADD_VERTEX
- **Description**: Adds new vertices using native API
- **Neo4j API**: `tx.createNode(Label.label("MyNode")).setProperty("id", vertexId)`
- **TinkerPop API**: `g.addV("MyNode").property("id", vertexId)`
- **Parameters**: List of vertex IDs
- **Error Handling**: Silent failure if vertex already exists (idempotent)

### 3. UPSERT_VERTEX_PROPERTY
- **Description**: Updates or inserts vertex properties using native API
- **Neo4j API**: `node.setProperty(key, value)`
- **TinkerPop API**: `vertex.property(key, value)`
- **Parameters**: List of vertex updates (id + properties map)
- **Error Handling**: Silent failure if vertex doesn't exist

### 4. REMOVE_VERTEX
- **Description**: Removes vertices using native API
- **Neo4j API**: `node.delete()` (after deleting relationships)
- **TinkerPop API**: `g.V(vertexId).drop()`
- **Parameters**: List of vertex IDs
- **Error Handling**: Silent failure if vertex doesn't exist (idempotent)

### 5. ADD_EDGE
- **Description**: Adds edges between vertices using native API
- **Neo4j API**: `srcNode.createRelationshipTo(dstNode, RelationshipType.withName(label))`
- **TinkerPop API**: `g.V(srcId).addE(label).to(g.V(dstId))`
- **Parameters**: Edge label and list of (src, dst) pairs
- **Error Handling**: Silent failure if source or destination vertex doesn't exist

### 6. UPSERT_EDGE_PROPERTY
- **Description**: Updates or inserts edge properties using native API
- **Neo4j API**: `relationship.setProperty(key, value)`
- **TinkerPop API**: `edge.property(key, value)`
- **Parameters**: Edge label and list of edge updates (src, dst, properties map)
- **Error Handling**: Silent failure if edge doesn't exist

### 7. REMOVE_EDGE
- **Description**: Removes edges using native API
- **Neo4j API**: `relationship.delete()`
- **TinkerPop API**: `g.V(srcId).outE(label).where(inV().hasId(dstId)).drop()`
- **Parameters**: Edge label and list of (src, dst) pairs
- **Error Handling**: Silent failure if edge doesn't exist (idempotent)

### 8. GET_NBRS
- **Description**: Gets neighbors of vertices using native API
- **Neo4j API**: `node.getRelationships(direction)` then `rel.getOtherNode(node)`
- **TinkerPop API**: `g.V(vertexId).out()` / `in()` / `both()`
- **Parameters**: Direction ("OUT", "IN", "BOTH") and list of vertex IDs
- **Error Handling**: Returns empty result if vertex doesn't exist

### 9. GET_VERTEX_BY_PROPERTY
- **Description**: Queries vertices by property using native API
- **Neo4j API**: Iterate through `tx.getAllNodes()` and filter by property
- **TinkerPop API**: `g.V().hasLabel(label).has(key, value)`
- **Parameters**: List of property queries (key, value pairs)
- **Error Handling**: Returns empty result if no matches found

### 10. GET_EDGE_BY_PROPERTY
- **Description**: Queries edges by property using native API
- **Neo4j API**: Iterate through relationships and filter by property
- **TinkerPop API**: `g.E().hasLabel(label).has(key, value)`
- **Parameters**: Edge label and list of property queries (key, value pairs)
- **Error Handling**: Returns empty result if no matches found

## Workload Format

Workloads are defined as JSON files with structured parameters:

```json
{
  "task_type": "ADD_VERTEX",
  "ops_count": 3,
  "parameters": {
    "ids": [10001, 10002, 10003]
  }
}
```

```json
{
  "task_type": "GET_NBRS",
  "ops_count": 3,
  "parameters": {
    "direction": "OUT",
    "ids": [10001, 10002, 10005]
  }
}
```

### Error Handling Philosophy

The benchmark framework follows a **silent failure** approach:
- **Add operations**: Skip if entity already exists (idempotent)
- **Delete operations**: No-op if entity doesn't exist (idempotent)
- **Read operations**: Return empty results if entity doesn't exist

This design ensures:
1. **Consistent metrics**: Failed operations don't skew latency measurements
2. **Realistic workloads**: Real-world systems often handle missing entities gracefully
3. **Continuous execution**: Benchmark doesn't stop on individual operation failures

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
      "clientThreads": 16,
      "throughputQps": 6071.23
    }
  ]
}
```

### Metrics Explanation

**Latency Metrics** (for `_latency` tasks):
- `minUs`, `maxUs`, `meanUs`: Minimum, maximum, and mean latency in microseconds
- `medianUs`, `p50Us`: Median (50th percentile) latency
- `p90Us`, `p95Us`, `p99Us`: 90th, 95th, and 99th percentile latencies
- Calculated as: `batch_execution_time / batch_size`

**Throughput Metrics** (for `_throughput` tasks):
- `throughputQps`: Queries per second
- Calculated as: `totalOps / durationSeconds`
- Measures the rate of operations completed during parallel execution

## How It Works

1. **Parameter Configuration**: BenchmarkLauncher reads database, dataset, and workload configurations
2. **Workload Compilation**: WorkloadCompiler generates native API workload JSON files with structured parameters (no query strings)
3. **Docker Container Startup**: DockerManager builds/starts Docker container with mounted dataset and compiled workload directories
4. **Benchmark Execution**: Container's WorkloadDispatcher:
   - Reads JSON workload files
   - Dispatches operations to BenchmarkExecutor
   - Executor calls native APIs serially, measures per-operation latency
   - Measures pure execution time (no network overhead)
   - Returns results via HTTP API
5. **Result Collection**: Host collects results and saves to JSON file
6. **Visualization**: ReportGenerator creates comparison charts using Seaborn

### Native API Execution

**Neo4j (Embedded API):**
- Direct calls to `tx.createNode()`, `node.setProperty()`, `relationship.delete()`, etc.
- Each operation wrapped in its own transaction
- No Cypher query parsing overhead

**JanusGraph (TinkerPop Structure API):**
- Direct calls to `g.addV()`, `vertex.property()`, `g.V().drop()`, etc.
- Transaction management via `g.tx().commit()`
- No Gremlin query parsing overhead

**ArangoDB (Fuerte C++ API):**
- Direct calls to ArangoDB HTTP API via Fuerte driver
- Batch operations using AQL `FOR ... IN @array INSERT/UPDATE/REMOVE` patterns
- Optimized batch insertion for graph loading
- No AQL query parsing overhead for batch operations

## Database Versions

| Database | Version | Release Date | Backend/Driver | Requirements |
|----------|---------|--------------|----------------|--------------|
| Neo4j | 2026.01.4 | January 2026 | Embedded | Java 21 |
| JanusGraph | 1.2.0-20251114-142114.b424a8f | November 14, 2025 | BerkeleyDB | Java 17 |
| ArangoDB | 3.12.7-2 | December 2024 | Fuerte C++ driver | C++17 |

## Design Principles

1. **Native API Performance**: Direct database API calls without query parsing overhead
2. **Separation of Concerns**: Host handles orchestration; containers handle pure execution
3. **Latency Focus**: Serial execution to accurately measure per-operation latency
4. **Clean Benchmarking**: Docker isolation ensures reproducible, interference-free measurements
5. **Pure Execution Time**: Timing excludes network transmission and only measures API call execution
6. **Extensibility**: Easy to add new databases by implementing executor interfaces

## License

MIT
