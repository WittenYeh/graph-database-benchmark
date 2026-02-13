# Graph Database Benchmark - HTTP API Specification

This document defines the standard HTTP API for communication between the host and database containers.

---

## Table of Contents

1. [Overview](#overview)
2. [Container API Endpoints](#container-api-endpoints)
3. [Host API Endpoints](#host-api-endpoints)
4. [Implementation Requirements](#implementation-requirements)
5. [Example Workflow](#example-workflow)
6. [Implementation Status](#implementation-status)

---

## Overview

The benchmark system uses HTTP APIs for bidirectional communication:

- **Host → Container**: Execute benchmark requests via REST API
- **Container → Host**: Real-time progress callbacks during execution

This design is language-agnostic and supports any database implementation (Java, C++, Python, etc.).

---

## Container API Endpoints

All database containers must expose these endpoints on their configured API port (e.g., 50080 for Neo4j, 50081 for JanusGraph).

### 1. Health Check

**Endpoint:** `GET /health`

**Description:** Check if the container is ready to accept benchmark requests.

**Response:**
```json
{
  "status": "ok"
}
```

**Status Codes:**
- `200 OK` - Container is ready

---

### 2. Execute Benchmark

**Endpoint:** `POST /execute`

**Description:** Execute the complete benchmark workflow with all tasks.

**Request Body:**
```json
{
  "dataset_name": "coAuthorsDBLP",
  "dataset_path": "/data/datasets/coAuthorsDBLP.mtx",
  "server_threads": 8,
  "callback_url": "http://host.docker.internal:8888/progress"
}
```

**Request Parameters:**
- `dataset_name` (string, required): Name of the dataset
- `dataset_path` (string, required): Path to the dataset file inside container
- `server_threads` (integer, optional): Number of server threads (default: 8)
- `callback_url` (string, optional): URL for progress callbacks to host

**Response:**
```json
{
  "metadata": {
    "database": "neo4j",
    "dataset": "coAuthorsDBLP",
    "datasetPath": "/data/datasets/coAuthorsDBLP.mtx",
    "timestamp": "2026-02-13T10:30:00Z",
    "serverThreads": 8,
    "javaVersion": "21.0.1",
    "osName": "Linux",
    "osVersion": "5.19.0"
  },
  "results": [
    {
      "task": "load_graph",
      "status": "success",
      "durationSeconds": 12.5,
      "totalOps": 10000,
      "clientThreads": 0
    },
    {
      "task": "read_neighbors",
      "status": "success",
      "durationSeconds": 5.2,
      "totalOps": 1000,
      "clientThreads": 1,
      "latency": {
        "minUs": 100.5,
        "maxUs": 5000.2,
        "meanUs": 520.3,
        "medianUs": 450.1,
        "p50Us": 450.1,
        "p90Us": 1200.5,
        "p95Us": 1500.8,
        "p99Us": 2500.3
      }
    }
  ]
}
```

**Status Codes:**
- `200 OK` - Benchmark completed successfully
- `500 Internal Server Error` - Benchmark execution failed

**Error Response:**
```json
{
  "error": "Error message describing what went wrong"
}
```

---

## Host API Endpoints

The host must expose these endpoints to receive progress updates from containers.

### 3. Progress Callback

**Endpoint:** `POST /progress`

**Description:** Receive progress updates from containers during benchmark execution.

**Request Body (Task Start):**
```json
{
  "event": "task_start",
  "task_name": "load_graph",
  "task_index": 0,
  "total_tasks": 5,
  "workload_file": "01_load_graph.json",
  "timestamp": 1707825000000
}
```

**Request Body (Task Complete):**
```json
{
  "event": "task_complete",
  "task_name": "load_graph",
  "task_index": 0,
  "total_tasks": 5,
  "duration_seconds": 12.5,
  "status": "success",
  "timestamp": 1707825012500
}
```

**Event Types:**
- `task_start` - Sent when a task begins execution
- `task_complete` - Sent when a task finishes (success or failure)

**Response:**
```json
{
  "status": "ok"
}
```

**Status Codes:**
- `200 OK` - Progress update received

---

## Implementation Requirements

### For Container Implementations

1. **All containers MUST implement endpoints 1 and 2** (`/health` and `/execute`)
2. **Containers MUST send progress callbacks** if `callback_url` is provided in the execute request
3. **Containers MUST send callbacks for each task:**
   - One `task_start` callback when task begins
   - One `task_complete` callback when task finishes
4. **Containers MUST handle callback failures gracefully** - if callback fails, log warning but continue execution
5. **Task execution order MUST follow workload file alphabetical order**
6. **Response format MUST match the specification exactly**

### For Host Implementation

1. **Host MUST implement endpoint 3** (`/progress`) to receive callbacks
2. **Host MUST display progress information** in the terminal when callbacks are received
3. **Host MUST handle missing callback_url** - containers should work without callbacks
4. **Host MUST start containers with `extra_hosts={'host.docker.internal': 'host-gateway'}`** to enable container-to-host communication

---

## Example Workflow

1. Host starts HTTP server on port 8888 with `/progress` endpoint
2. Host starts container with `extra_hosts={'host.docker.internal': 'host-gateway'}`
3. Host sends POST to `http://container:50080/execute` with `callback_url: "http://host.docker.internal:8888/progress"`
4. Container begins execution:
   - Sends `task_start` callback for task 0
   - Executes task 0
   - Sends `task_complete` callback for task 0
   - Sends `task_start` callback for task 1
   - Executes task 1
   - Sends `task_complete` callback for task 1
   - ... continues for all tasks
5. Container returns final results to host
6. Host stops HTTP server and processes results

### Terminal Output Example

```
🚀 Starting benchmark for database: neo4j
📡 Progress server started on port 8888
📦 Preparing Docker image: bench-neo4j
📊 Testing dataset: coAuthorsDBLP
⚙️  Compiling workload to cypher...
🐳 Starting Docker container: neo4j-benchmark
⏳ Waiting for container to be ready...
✓ Container ready
⏱️  Executing benchmark tasks...
  ▶️  Task 1/3: load_graph (01_load_graph.json)
  ✓  Task 1/3: load_graph completed in 12.50s (success)
  ▶️  Task 2/3: read_neighbors (02_read_neighbors.json)
  ✓  Task 2/3: read_neighbors completed in 5.20s (success)
  ▶️  Task 3/3: write_edges (03_write_edges.json)
  ✓  Task 3/3: write_edges completed in 8.30s (success)
✅ Results saved to: reports/bench_neo4j_coAuthorsDBLP.json
🛑 Stopping container...
📡 Progress server stopped
🎉 Benchmark completed!
```

---

## Implementation Status

### ✅ Completed

1. **API Specification**
   - All endpoints defined with request/response formats
   - Implementation requirements documented

2. **Container-Side Implementation**
   - **Neo4j**:
     - `docker/neo4j/src/main/java/com/graphbench/neo4j/BenchmarkServer.java` - HTTP server with `/health` and `/execute` endpoints
     - `docker/neo4j/src/main/java/com/graphbench/neo4j/Neo4jBenchmarkExecutor.java` - Benchmark execution with progress callbacks
   - **JanusGraph**:
     - `docker/janusgraph/src/main/java/com/graphbench/janusgraph/BenchmarkServer.java` - HTTP server with `/health` and `/execute` endpoints
     - `docker/janusgraph/src/main/java/com/graphbench/janusgraph/JanusGraphBenchmarkExecutor.java` - Benchmark execution with progress callbacks

3. **Host-Side Implementation**
   - `host/progress_server.py` - HTTP server receiving progress callbacks on port 8888
   - `host/db/docker_manager.py` - Updated to pass `callback_url` and configure `extra_hosts`
   - `host/benchmark_launcher.py` - Integrated progress server lifecycle management

### Key Features

- **Real-time visibility** - See which task is running and progress in terminal
- **Language agnostic** - HTTP API works with any language (Java, C++, Python, etc.)
- **Standardized** - All databases follow same API contract
- **Graceful degradation** - Works without callbacks if `callback_url` not provided
- **Non-blocking** - Callbacks don't block container execution

---

## Testing

To test the implementation:

```bash
# Rebuild containers with new API
./build.sh

# Run benchmark (progress will be displayed in real-time)
./run_benchmark.sh -d neo4j -s delaunay_n13 -w workloads/templates/quick_test.json
```

Expected behavior:
- Progress server starts on port 8888
- Container sends callbacks for each task start/complete
- Terminal displays real-time progress
- Final results saved to reports directory
- Progress server stops after completion
