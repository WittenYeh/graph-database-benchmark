package com.graphbench.neo4j;

import com.graphbench.api.PropertyBenchmarkExecutor;
import com.graphbench.workload.*;
import org.neo4j.graphdb.*;

import java.util.*;

/**
 * Neo4j property benchmark executor.
 * Loads graph with all CSV properties stored on nodes/edges, creates indexes dynamically.
 */
public class Neo4jPropertyBenchmarkExecutor extends Neo4jBenchmarkExecutor
        implements PropertyBenchmarkExecutor {

    @Override
    public String getDatabaseName() { return "neo4j-property"; }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        Neo4jGraphLoader loader = new Neo4jGraphLoader(db, progressCallback, true);
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        loader.createPropertyIndexes(loader.getMetadata());
        return result;
    }

    // --- Property operations ---

    @Override
    public List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize) {
        return transactionalExecute(updates, (tx, update) -> {
            Node node = tx.getNodeById((Long) update.getSystemId());
            if (node != null) {
                for (Map.Entry<String, Object> e : update.getProperties().entrySet()) {
                    node.setProperty(e.getKey(), e.getValue());
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> updateEdgeProperty(String label, List<UpdateEdgePropertyParams.EdgeUpdate> updates, int batchSize) {
        RelationshipType relType = RelationshipType.withName(label);
        return transactionalExecute(updates, (tx, update) -> {
            Node srcNode = tx.getNodeById((Long) update.getSrcSystemId());
            Node dstNode = tx.getNodeById((Long) update.getDstSystemId());
            if (srcNode != null && dstNode != null) {
                for (Relationship rel : srcNode.getRelationships(Direction.OUTGOING, relType)) {
                    if (rel.getEndNode().equals(dstNode)) {
                        for (Map.Entry<String, Object> e : update.getProperties().entrySet()) {
                            rel.setProperty(e.getKey(), e.getValue());
                        }
                        break;
                    }
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalExecute(queries, (tx, query) -> {
            try (ResourceIterator<Node> nodes = tx.findNodes(Label.label(NODE_LABEL), query.getKey(), query.getValue())) {
                nodes.forEachRemaining(blackhole::consume);
            }
        }, batchSize);
    }

    @Override
    public List<Double> getEdgeByProperty(List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalExecute(queries, (tx, query) -> {
            tx.getAllRelationships().forEach(rel -> {
                Object val = rel.getProperty(query.getKey(), null);
                if (val != null && val.equals(query.getValue())) {
                    blackhole.consume(rel);
                }
            });
        }, batchSize);
    }
}
