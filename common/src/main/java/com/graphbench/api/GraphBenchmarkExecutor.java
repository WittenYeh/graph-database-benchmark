package com.graphbench.api;

import java.util.List;
import java.util.Map;

/**
 * Core interface for graph database benchmark executors.
 * All database implementations (Neo4j, JanusGraph, etc.) must implement this interface.
 */
public interface GraphBenchmarkExecutor {

    /**
     * Execute the complete benchmark workflow with all tasks.
     *
     * @param datasetPath Path to the dataset file inside container
     * @param serverThreads Number of server threads for database configuration
     * @param callbackUrl Optional URL for progress callbacks to host (can be null)
     * @return Benchmark results containing metadata and task results
     * @throws Exception if benchmark execution fails
     */
    Map<String, Object> executeBenchmark(String datasetPath, int serverThreads, String callbackUrl) throws Exception;

    /**
     * Initialize the database instance and create schema.
     * Called once before executing any tasks.
     *
     * @throws Exception if database initialization fails
     */
    void initDatabase() throws Exception;

    /**
     * Execute a single benchmark task with the given queries.
     *
     * @param taskName Name of the task (e.g., "load_graph", "read_neighbors")
     * @param queries List of database-specific query strings to execute
     * @param clientThreads Number of client threads for concurrent execution
     * @param datasetPath Path to the dataset file (for load_graph task)
     * @param batchSize Number of queries to execute in a single batch
     * @param latencyTestMode Mode for latency testing: "batch" or "singleton"
     * @return Task execution results including duration, latency stats, etc.
     * @throws Exception if task execution fails
     */
    Map<String, Object> executeTask(String taskName, List<String> queries, int clientThreads, String datasetPath, int batchSize, String latencyTestMode) throws Exception;

    /**
     * Execute a single query string.
     * Implementation should handle transaction management and error handling.
     *
     * @param query Database-specific query string (Cypher for Neo4j, Gremlin for JanusGraph)
     */
    void executeQuery(String query);

    /**
     * Create a snapshot of the current graph state.
     * Used for copy_mode to restore graph state before each task.
     *
     * @return Serialized graph snapshot as byte array
     * @throws Exception if snapshot creation fails
     */
    byte[] snapshotGraph() throws Exception;

    /**
     * Restore graph state from a snapshot.
     * Used for copy_mode to reset graph before executing a task.
     *
     * @param snapshot Serialized graph snapshot
     * @throws Exception if restore fails
     */
    void restoreGraph(byte[] snapshot) throws Exception;

    /**
     * Shutdown the database and cleanup resources.
     * Called after all tasks complete.
     *
     * @throws Exception if shutdown fails
     */
    void shutdown() throws Exception;
}
