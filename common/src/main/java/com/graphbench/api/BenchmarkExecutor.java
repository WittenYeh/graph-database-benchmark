package com.graphbench.api;

import com.graphbench.workload.*;
import java.util.List;
import java.util.Map;

/**
 * Interface for benchmark executors.
 * All operations are executed serially to measure per-operation latency.
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
     * Load graph from dataset file.
     * @param datasetPath Path to the dataset file
     * @return Map containing metadata (nodes count, edges count, duration)
     */
    Map<String, Object> loadGraph(String datasetPath) throws Exception;

    /**
     * Add vertices using native API.
     * @param ids List of vertex IDs to add
     * @return List of latencies in microseconds for each operation
     */
    List<Double> addVertex(List<Long> ids);

    /**
     * Upsert vertex properties using native API.
     * @param updates List of vertex updates (id + properties)
     * @return List of latencies in microseconds for each operation
     */
    List<Double> upsertVertexProperty(List<UpsertVertexPropertyParams.VertexUpdate> updates);

    /**
     * Remove vertices using native API.
     * @param ids List of vertex IDs to remove
     * @return List of latencies in microseconds for each operation
     */
    List<Double> removeVertex(List<Long> ids);

    /**
     * Add edges using native API.
     * @param label Edge label
     * @param pairs List of (src, dst) pairs
     * @return List of latencies in microseconds for each operation
     */
    List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs);

    /**
     * Upsert edge properties using native API.
     * @param label Edge label
     * @param updates List of edge updates (src, dst, properties)
     * @return List of latencies in microseconds for each operation
     */
    List<Double> upsertEdgeProperty(String label, List<UpsertEdgePropertyParams.EdgeUpdate> updates);

    /**
     * Remove edges using native API.
     * @param label Edge label
     * @param pairs List of (src, dst) pairs
     * @return List of latencies in microseconds for each operation
     */
    List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs);

    /**
     * Get neighbors using native API.
     * @param direction Direction: "OUT", "IN", or "BOTH"
     * @param ids List of vertex IDs
     * @return List of latencies in microseconds for each operation
     */
    List<Double> getNbrs(String direction, List<Long> ids);

    /**
     * Get vertices by property using native API.
     * @param queries List of property queries (key, value)
     * @return List of latencies in microseconds for each operation
     */
    List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries);

    /**
     * Get edges by property using native API.
     * @param label Edge label
     * @param queries List of property queries (key, value)
     * @return List of latencies in microseconds for each operation
     */
    List<Double> getEdgeByProperty(String label, List<GetEdgeByPropertyParams.PropertyQuery> queries);

    /**
     * Get the database name for metadata.
     */
    String getDatabaseName();
}
