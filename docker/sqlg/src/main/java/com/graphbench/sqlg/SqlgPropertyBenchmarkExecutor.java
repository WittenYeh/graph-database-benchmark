package com.graphbench.sqlg;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.PropertyBenchmarkExecutor;
import com.graphbench.workload.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * SQLG property benchmark executor.
 * Extends SqlgBenchmarkExecutor to support property-based operations.
 */
public class SqlgPropertyBenchmarkExecutor extends SqlgBenchmarkExecutor implements PropertyBenchmarkExecutor {
    private CsvGraphReader.CsvMetadata csvMetadata;

    @Override
    public String getDatabaseName() { return "sqlg-property"; }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        // Read CSV metadata first
        csvMetadata = CsvGraphReader.readHeaders(datasetPath);

        // Create property indexes before loading data
        createPropertyIndexes(csvMetadata);

        // Load graph with properties
        SqlgGraphLoader loader = new SqlgGraphLoader(graph, g, progressCallback, true);
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        csvMetadata = loader.getMetadata();

        return result;
    }

    /**
     * Create property indexes for efficient property queries.
     */
    private void createPropertyIndexes(CsvGraphReader.CsvMetadata metadata) {
        progressCallback.sendLogMessage("Creating property indexes...", "INFO");

        // SQLG automatically creates indexes when properties are first used
        // No explicit index creation needed like JanusGraph

        progressCallback.sendLogMessage("Property indexes will be created automatically", "INFO");
    }

    @Override
    public List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize) {
        return transactionalBatchExecute(updates, batchSize, update -> {
            Vertex v = g.V(update.getSystemId()).next();
            Map<String, Object> properties = update.getProperties();
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                v.property(entry.getKey(), entry.getValue());
            }
        });
    }

    @Override
    public List<Double> updateEdgeProperty(String label, List<UpdateEdgePropertyParams.EdgeUpdate> updates, int batchSize) {
        return transactionalBatchExecute(updates, batchSize, update -> {
            List<Edge> edges = g.V(update.getSrcSystemId())
                .outE(label)
                .where(__.inV().hasId(update.getDstSystemId()))
                .toList();

            Map<String, Object> properties = update.getProperties();
            for (Edge edge : edges) {
                for (Map.Entry<String, Object> entry : properties.entrySet()) {
                    edge.property(entry.getKey(), entry.getValue());
                }
            }
        });
    }

    @Override
    public List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalBatchExecute(queries, batchSize, query -> {
            List<Vertex> vertices = g.V().has(NODE_LABEL, query.getKey(), query.getValue()).toList();
            blackhole.consume(vertices);
        });
    }

    @Override
    public List<Double> getEdgeByProperty(List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalBatchExecute(queries, batchSize, query -> {
            List<Edge> edges = g.E().has(query.getKey(), query.getValue()).toList();
            blackhole.consume(edges);
        });
    }
}