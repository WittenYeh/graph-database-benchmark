package com.graphbench.janusgraph;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.PropertyBenchmarkExecutor;
import com.graphbench.workload.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

/**
 * JanusGraph property benchmark executor.
 * Loads graph with all CSV properties stored on vertices/edges, creates indexes dynamically.
 *
 * In JanusGraph, when PropertyKey and Index are created in the same management transaction
 * on an empty graph, the index transitions directly to ENABLED state after commit.
 * This allows data insertion to automatically populate the index without manual enabling.
 */
public class JanusGraphPropertyBenchmarkExecutor extends JanusGraphBenchmarkExecutor
        implements PropertyBenchmarkExecutor {

    @Override
    public String getDatabaseName() { return "janusgraph-property"; }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        // Read CSV headers first so we can create indexes before loading data.
        CsvGraphReader.CsvMetadata meta = CsvGraphReader.readHeaders(datasetPath);

        // Create loader and indexes
        JanusGraphGraphLoader loader = new JanusGraphGraphLoader(graph, g, progressCallback, true);
        loader.createPropertyIndexes(meta);

        // Load graph data — indexes are now ENABLED and will be populated automatically.
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        return result;
    }

    // --- Property operations ---

    @Override
    public List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize) {
        return transactionalBatchExecute(updates, update -> {
            Vertex v = g.V(update.getSystemId()).tryNext().orElse(null);
            if (v != null) {
                for (Map.Entry<String, Object> e : update.getProperties().entrySet()) {
                    v.property(e.getKey(), e.getValue());
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> updateEdgeProperty(String label, List<UpdateEdgePropertyParams.EdgeUpdate> updates, int batchSize) {
        return transactionalBatchExecute(updates, update -> {
            g.V(update.getSrcSystemId()).outE(label)
                .where(__.inV().hasId(update.getDstSystemId()))
                .forEachRemaining(edge -> {
                    for (Map.Entry<String, Object> e : update.getProperties().entrySet()) {
                        edge.property(e.getKey(), e.getValue());
                    }
                });
        }, batchSize);
    }

    @Override
    public List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalBatchExecute(queries, query -> {
            g.V().has(query.getKey(), query.getValue())
                .forEachRemaining(blackhole::consume);
        }, batchSize);
    }

    @Override
    public List<Double> getEdgeByProperty(List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalBatchExecute(queries, query -> {
            g.E().has(query.getKey(), query.getValue())
                .forEachRemaining(blackhole::consume);
        }, batchSize);
    }
}
