package com.graphbench.janusgraph;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.PropertyBenchmarkExecutor;
import com.graphbench.workload.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.*;

/**
 * JanusGraph property benchmark executor.
 * Loads graph with all CSV properties stored on vertices/edges, creates indexes dynamically.
 */
public class JanusGraphPropertyBenchmarkExecutor extends JanusGraphBenchmarkExecutor
        implements PropertyBenchmarkExecutor {

    @Override
    public String getDatabaseName() { return "janusgraph-property"; }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        JanusGraphGraphLoader loader = new JanusGraphGraphLoader(g, progressCallback, true);
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        createPropertyIndexes(loader.getMetadata());
        return result;
    }

    private void createPropertyIndexes(CsvGraphReader.CsvMetadata metadata) {
        JanusGraphManagement mgmt = graph.openManagement();
        try {
            for (String prop : metadata.getNodePropertyHeaders()) {
                progressCallback.sendLogMessage("Creating node property index: " + prop, "INFO");
                PropertyKey key = mgmt.getPropertyKey(prop);
                if (key == null) {
                    key = mgmt.makePropertyKey(prop).dataType(String.class).make();
                }
                if (!mgmt.containsGraphIndex("node_" + prop + "_idx")) {
                    mgmt.buildIndex("node_" + prop + "_idx", Vertex.class).addKey(key).buildCompositeIndex();
                }
            }
            for (String prop : metadata.getEdgePropertyHeaders()) {
                progressCallback.sendLogMessage("Creating edge property index: " + prop, "INFO");
                PropertyKey key = mgmt.getPropertyKey(prop);
                if (key == null) {
                    key = mgmt.makePropertyKey(prop).dataType(String.class).make();
                }
                if (!mgmt.containsGraphIndex("edge_" + prop + "_idx")) {
                    mgmt.buildIndex("edge_" + prop + "_idx", Edge.class).addKey(key).buildCompositeIndex();
                }
            }
            mgmt.commit();
            progressCallback.sendLogMessage("All property indexes created", "INFO");
        } catch (Exception e) {
            progressCallback.sendLogMessage("Failed to create property indexes: " + e.getMessage(), "WARNING");
            mgmt.rollback();
        }
    }

    // --- Property operations ---

    @Override
    public List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize) {
        return transactionalExecute(updates, update -> {
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
        return transactionalExecute(updates, update -> {
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
        return transactionalExecute(queries, query -> {
            g.V().hasLabel(NODE_LABEL)
                .has(query.getKey(), query.getValue())
                .forEachRemaining(blackhole::consume);
        }, batchSize);
    }

    @Override
    public List<Double> getEdgeByProperty(String label, List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalExecute(queries, query -> {
            g.E().hasLabel(label)
                .has(query.getKey(), query.getValue())
                .forEachRemaining(blackhole::consume);
        }, batchSize);
    }
}
