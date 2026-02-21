package com.graphbench.api;

import com.graphbench.workload.*;
import java.util.List;
import java.util.Map;

/**
 * Interface for structural benchmark executors.
 * Handles graph structural operations: addVertex, removeVertex, addEdge, removeEdge, getNbrs.
 * Property operations (updateVertexProperty, etc.) are in PropertyBenchmarkExecutor.
 */
public interface BenchmarkExecutor {

    /**
     * Initialize the database instance.
     */
    void initDatabase() throws Exception;

    /**
     * Shutdown the database and cleanup resources.
     */
    void shutdown() throws Exception;

    /**
     * Load graph from dataset directory containing nodes.csv and edges.csv.
     * @param datasetPath Path to the dataset directory
     * @return Map containing metadata (nodes count, edges count, duration)
     */
    Map<String, Object> loadGraph(String datasetPath) throws Exception;

    /**
     * Add vertices using native API with specified batch size.
     * @param count Number of vertices to add
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> addVertex(int count, int batchSize);

    /**
     * Remove vertices using native API with specified batch size.
     * @param systemIds List of system-specific vertex IDs to remove
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> removeVertex(List<Object> systemIds, int batchSize);

    /**
     * Add edges using native API with specified batch size.
     * @param label Edge label
     * @param pairs List of (src, dst) pairs with system IDs
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs, int batchSize);

    /**
     * Remove edges using native API with specified batch size.
     * @param label Edge label
     * @param pairs List of (src, dst) pairs
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs, int batchSize);

    /**
     * Get neighbors using native API with specified batch size.
     * @param direction Direction: "OUT", "IN", or "BOTH"
     * @param systemIds List of system-specific vertex IDs
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> getNbrs(String direction, List<Object> systemIds, int batchSize);

    /**
     * Add vertices using native API (default batch size = 1).
     */
    default List<Double> addVertex(int count) {
        return addVertex(count, 1);
    }

    /**
     * Remove vertices using native API (default batch size = 1).
     */
    default List<Double> removeVertex(List<Object> systemIds) {
        return removeVertex(systemIds, 1);
    }

    /**
     * Add edges using native API (default batch size = 1).
     */
    default List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs) {
        return addEdge(label, pairs, 1);
    }

    /**
     * Remove edges using native API (default batch size = 1).
     */
    default List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs) {
        return removeEdge(label, pairs, 1);
    }

    /**
     * Get neighbors using native API (default batch size = 1).
     */
    default List<Double> getNbrs(String direction, List<Object> systemIds) {
        return getNbrs(direction, systemIds, 1);
    }

    /**
     * Get the database name for metadata.
     */
    String getDatabaseName();

    /**
     * Get the database directory path.
     * @return Path to the database directory
     */
    String getDatabasePath();

    /**
     * Get the snapshot directory path.
     * @return Path to the snapshot directory
     */
    String getSnapshotPath();

    /**
     * Close the database connection.
     * Called before snapshot/restore operations.
     * @throws Exception if close fails
     */
    void closeDatabase() throws Exception;

    /**
     * Open the database connection.
     * Called after snapshot/restore operations.
     * @throws Exception if open fails
     */
    void openDatabase() throws Exception;

    /**
     * Create a snapshot of the current database state.
     * This should be called after loadGraph to preserve the initial state.
     * @throws Exception if snapshot creation fails
     */
    default void snapGraph() throws Exception {
        // Close the database
        closeDatabase();

        // Copy database directory to snapshot location
        java.io.File dbDir = new java.io.File(getDatabasePath());
        java.io.File snapshotDir = new java.io.File(getSnapshotPath());

        if (snapshotDir.exists()) {
            BenchmarkUtils.deleteDirectory(snapshotDir);
        }

        BenchmarkUtils.copyDirectory(dbDir.toPath(), snapshotDir.toPath());

        // Reopen the database
        openDatabase();
    }

    /**
     * Restore the database to the snapshot state.
     * This should be called before each workload to ensure a clean starting state.
     * @throws Exception if restore fails
     */
    default void restoreGraph() throws Exception {
        // Close the database
        closeDatabase();

        // Delete current database directory
        java.io.File dbDir = new java.io.File(getDatabasePath());
        if (dbDir.exists()) {
            BenchmarkUtils.deleteDirectory(dbDir);
        }

        // Copy snapshot back to database location
        java.io.File snapshotDir = new java.io.File(getSnapshotPath());
        if (!snapshotDir.exists()) {
            throw new IllegalStateException("Snapshot does not exist at: " + getSnapshotPath());
        }

        BenchmarkUtils.copyDirectory(snapshotDir.toPath(), dbDir.toPath());

        // Reopen the database
        openDatabase();
    }

    /**
     * Get the count of errors that occurred during operations.
     * @return Number of failed operations
     */
    default int getErrorCount() {
        return 0;
    }

    /**
     * Reset the error count to zero.
     */
    default void resetErrorCount() {
        // Default implementation does nothing
    }

    /**
     * Convert origin ID to system-specific internal ID.
     * @param originId The original vertex ID from the dataset
     * @return The system-specific internal ID, or null if not found
     */
    Object getSystemId(Long originId);
}
