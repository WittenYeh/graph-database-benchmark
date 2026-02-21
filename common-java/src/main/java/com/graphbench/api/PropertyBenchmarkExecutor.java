package com.graphbench.api;

import com.graphbench.workload.*;
import java.util.List;

/**
 * Interface for property benchmark executors.
 * Extends BenchmarkExecutor with property-related operations:
 * updateVertexProperty, updateEdgeProperty, getVertexByProperty, getEdgeByProperty.
 */
public interface PropertyBenchmarkExecutor extends BenchmarkExecutor {

    /**
     * Update vertex properties using native API with specified batch size.
     * @param updates List of vertex updates (system id + properties)
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize);

    /**
     * Update edge properties using native API with specified batch size.
     * @param label Edge label
     * @param updates List of edge updates (src, dst, properties)
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> updateEdgeProperty(String label, List<UpdateEdgePropertyParams.EdgeUpdate> updates, int batchSize);

    /**
     * Get vertices by property using native API with specified batch size.
     * @param queries List of property queries (key, value)
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries, int batchSize);

    /**
     * Get edges by property using native API with specified batch size.
     * @param queries List of property queries (key, value)
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of latencies in microseconds for each batch
     */
    List<Double> getEdgeByProperty(List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize);

    /**
     * Update vertex properties using native API (default batch size = 1).
     */
    default List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates) {
        return updateVertexProperty(updates, 1);
    }

    /**
     * Update edge properties using native API (default batch size = 1).
     */
    default List<Double> updateEdgeProperty(String label, List<UpdateEdgePropertyParams.EdgeUpdate> updates) {
        return updateEdgeProperty(label, updates, 1);
    }

    /**
     * Get vertices by property using native API (default batch size = 1).
     */
    default List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries) {
        return getVertexByProperty(queries, 1);
    }

    /**
     * Get edges by property using native API (default batch size = 1).
     */
    default List<Double> getEdgeByProperty(List<GetEdgeByPropertyParams.PropertyQuery> queries) {
        return getEdgeByProperty(queries, 1);
    }
}
