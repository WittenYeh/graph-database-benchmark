# Graph Database Benchmark

A Docker-based benchmark suite for embedded graph databases. Currently supports **AsterDB** (embedded), **Neo4j** (embedded) and **JanusGraph** (embedded with BerkeleyJE backend).

Measures latency and throughput for graph operations (node/edge insertion, deletion, neighbor reads, and mixed workloads) in a reproducible environment.

## Main Test Results



## Prerequisites

- **Docker** (required)
- **Python 3.10+** (required for orchestrator script)
- Java 17+ and Maven 3.8+ (optional, only for local development)

## Quick Start

### 1. Prepare Datasets

Initialize the graph datasets submodule:

```bash
git submodule init
git submodule update
cd graph-datasets/<dataset-directory>
make   # downloads the .mtx file
```

### 2. Run Benchmarks with Docker

**Neo4j Benchmark:**

```bash
python benchmark.py --database-name neo4j --rebuild
```

**JanusGraph Benchmark:**

```bash
python benchmark.py --database-name janusgraph --rebuild
```

**Custom Configuration:**

```bash
python benchmark.py \
  --database-name janusgraph \
  --workload-config workloads/simple_delete_tests.json \
  --dataset-name delaunay_n13 \
  --seed 42 \
  --output-dir my_reports/ \
  --rebuild
```

### Python Script Arguments

| Argument              | Default                            | Description                                       |
| --------------------- | ---------------------------------- | ------------------------------------------------- |
| `--database-name`   | (required)                         | Database to benchmark:`neo4j` or `janusgraph` |
| `--database-config` | `database-config.json`           | Path to database configuration file               |
| `--dataset-config`  | `datasets.json`                  | Path to dataset configuration file                |
| `--dataset-name`    | (from config)                      | Dataset name override                             |
| `--workload-config` | `workloads/workload_config.json` | Path to workload configuration file               |
| `--workload-name`   | (all workloads)                    | Specific workload name to run                     |
| `--output-dir`      | `reports/`                       | Output directory for benchmark reports            |
| `--seed`            | (none)                             | Random seed for reproducibility                   |
| `--rebuild`         | off                                | Force rebuild the Docker image                    |

Results will be saved to the `--output-dir` directory as JSON files.

## Configuration Files

### Workload Configuration

Edit `workloads/workload_config.json` to define your benchmark scenario:

```json
{
  "dataset": "coAuthorsDBLP",
  "server_config": {
    "threads": 8
  },
  "tasks": [
    { "name": "load_graph" },
    { "name": "add_nodes_latency", "ops": 5000 },
    { "name": "add_nodes_throughput", "ops": 20000, "client_threads": 8 },
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

**Task Parameters:**

| Parameter          | Type    | Default  | Description                                                |
| ------------------ | ------- | -------- | ---------------------------------------------------------- |
| `name`           | string  | required | Task name (see Supported Tasks below)                      |
| `ops`            | integer | required | Number of operations to execute                            |
| `client_threads` | integer | 1        | Number of concurrent client threads                        |
| `ratios`         | object  | -        | Operation mix ratios for mixed workloads (must sum to 1.0) |
| `copy_mode`      | boolean | false    | Reload original graph before executing this task           |

**Supported Tasks:**

| Task Name                     | Mode       | Description                    |
| ----------------------------- | ---------- | ------------------------------ |
| `load_graph`                | -          | Bulk-loads the entire dataset  |
| `add_nodes_latency`         | Latency    | Single-threaded node insertion |
| `add_nodes_throughput`      | Throughput | Multi-threaded node insertion  |
| `add_edges_latency`         | Latency    | Single-threaded edge insertion |
| `add_edges_throughput`      | Throughput | Multi-threaded edge insertion  |
| `delete_nodes_latency`      | Latency    | Single-threaded node deletion  |
| `delete_nodes_throughput`   | Throughput | Multi-threaded node deletion   |
| `delete_edges_latency`      | Latency    | Single-threaded edge deletion  |
| `delete_edges_throughput`   | Throughput | Multi-threaded edge deletion   |
| `read_nbrs_latency`         | Latency    | Single-threaded neighbor reads |
| `read_nbrs_throughput`      | Throughput | Multi-threaded neighbor reads  |
| `mixed_workload_latency`    | Latency    | Mixed operations with ratios   |
| `mixed_workload_throughput` | Throughput | Mixed operations with ratios   |

**Benchmark Modes:**

- **Latency**: Single-threaded execution. Reports percentile statistics (min, max, mean, median, p90, p95, p99) in microseconds.
- **Throughput**: Multi-threaded execution. Reports aggregate operations per second.

### Dataset Configuration

The `datasets.json` file maps dataset names to their `.mtx` file paths:

```json
{
  "root_dir": "./graph-datasets/",
  "datasets": {
    "coAuthorsDBLP": "coAuthorsDBLP/coAuthorsDBLP.mtx",
    "delaunay_n13": "delaunay_n13/delaunay_n13.mtx"
  }
}
```

### Database Configuration

The `database-config.json` file contains database-specific settings (schema, indexing, batch sizes, etc.). You can customize these parameters without recompiling:

```json
{
  "databases": {
    "neo4j": {
      "adapter_class": "com.graphbench.db.neo4j.Neo4jAdapter",
      "query_language": "cypher",
      "schema": {
        "node_label": "Node",
        "edge_type": "EDGE",
        "id_property": "id"
      },
      "performance": {
        "batch_size": 5000
      }
    },
    "janusgraph": {
      "adapter_class": "com.graphbench.db.janusgraph.JanusGraphAdapter",
      "query_language": "gremlin",
      "schema": {
        "vertex_label": "Node",
        "edge_label": "EDGE",
        "property_name": "nodeId"
      },
      "performance": {
        "batch_size": 5000
      }
    }
  }
}
```

## Output Reports

Benchmark results are saved as JSON files in the output directory.

**Filename format:** `bench_<database>_<yyyyMMdd_HHmmss>.json`

**Example output:**

```json
{
  "metadata": {
    "database": "neo4j",
    "dataset": "coAuthorsDBLP",
    "timestamp": "2026-02-09T14:30:00",
    "serverThreads": 8
  },
  "results": [
    {
      "task": "load_graph",
      "status": "success",
      "durationSeconds": 12.503
    },
    {
      "task": "read_nbrs_latency",
      "status": "success",
      "totalOps": 1000,
      "latency": {
        "minUs": 15.2,
        "maxUs": 4520.1,
        "meanUs": 45.3,
        "p90Us": 89.2,
        "p95Us": 120.5,
        "p99Us": 350.8
      }
    },
    {
      "task": "read_nbrs_throughput",
      "status": "success",
      "totalOps": 50000,
      "clientThreads": 16,
      "throughput": {
        "opsPerSecond": 16000.0
      }
    }
  ]
}
```

## Advanced Usage

### Manual Docker Commands

If you prefer to use Docker directly without the Python orchestrator:

**Build image:**

```bash
docker build -f neo4j-benchmark/Dockerfile.neo4j -t graphdb-benchmark-neo4j .
```

**Run benchmark:**

```bash
docker run --rm \
  -v $(pwd)/graph-datasets:/app/graph-datasets \
  -v $(pwd)/database-config.json:/app/database-config.json \
  -v $(pwd)/datasets.json:/app/datasets.json \
  -v $(pwd)/workloads:/app/workloads \
  -v $(pwd)/reports:/app/reports \
  graphdb-benchmark-neo4j benchmark \
    --database-config /app/database-config.json \
    --database-name neo4j \
    --workload-config /app/workloads/workload_config.json \
    --dataset-config /app/datasets.json \
    --db-path /data/db/neo4j \
    --result-dir /app/reports
```

### Local Development (Without Docker)

If you want to build and run locally:

```bash
# Build all modules
mvn clean package

# Run benchmark directly
java -jar benchmark-cli/target/benchmark-cli-1.0-SNAPSHOT.jar benchmark \
  --database-config database-config.json \
  --database-name neo4j \
  --workload-config workloads/workload_config.json \
  --dataset-config datasets.json \
  --db-path /tmp/neo4j-bench \
  --result-dir reports
```

## Project Structure

```text
graphdb-benchmark/
├── benchmark.py                     # Python orchestrator (recommended entry point)
├── docker_utils.py                  # Docker utility functions
├── database-config.json             # Database configuration
├── datasets.json                    # Dataset registry
├── workloads/                       # Workload configurations
│   └── workload_config.json
├── graph-datasets/                  # Git submodule with datasets
├── common/                          # Shared Java code
├── neo4j-benchmark/                 # Neo4j adapter + Dockerfile
├── janusgraph-benchmark/            # JanusGraph adapter + Dockerfile
└── benchmark-cli/                   # CLI entry point
```

## Architecture

- **Docker-first**: Each database runs in an isolated container for reproducibility
- **Configuration-driven**: All parameters externalized to JSON files
- **Multi-module Maven**: Separate modules for common code and database-specific implementations
- **Unified CLI**: Single command for workload generation, compilation, and execution

## License

Copyright 2025 Weitang Ye. Licensed under the Apache License, Version 2.0.
