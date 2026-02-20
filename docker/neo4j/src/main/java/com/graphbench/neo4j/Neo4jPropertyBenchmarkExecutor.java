package com.graphbench.neo4j;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.PropertyBenchmarkExecutor;
import com.graphbench.workload.*;
import org.neo4j.graphdb.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
        createPropertyIndexes(loader.getMetadata());
        return result;
    }

    private void createPropertyIndexes(CsvGraphReader.CsvMetadata metadata) {
        for (String prop : metadata.getNodePropertyHeaders()) {
            progressCallback.sendLogMessage("Creating node property index: " + prop, "INFO");
            try (Transaction tx = db.beginTx()) {
                tx.execute("CREATE INDEX node_" + prop + "_idx IF NOT EXISTS FOR (n:" + NODE_LABEL + ") ON (n." + prop + ")");
                tx.commit();
            } catch (Exception e) {
                progressCallback.sendLogMessage("Failed to create node index for " + prop + ": " + e.getMessage(), "WARNING");
            }
        }
        for (String prop : metadata.getEdgePropertyHeaders()) {
            progressCallback.sendLogMessage("Creating edge property index: " + prop, "INFO");
            try (Transaction tx = db.beginTx()) {
                tx.execute("CREATE INDEX edge_" + prop + "_idx IF NOT EXISTS FOR ()-[r:MyEdge]-() ON (r." + prop + ")");
                tx.commit();
            } catch (Exception e) {
                progressCallback.sendLogMessage("Failed to create edge index for " + prop + ": " + e.getMessage(), "WARNING");
            }
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, TimeUnit.SECONDS);
            tx.commit();
            progressCallback.sendLogMessage("All property indexes online", "INFO");
        } catch (Exception e) {
            progressCallback.sendLogMessage("Timeout waiting for indexes: " + e.getMessage(), "WARNING");
        }
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
    public List<Double> getEdgeByProperty(String label, List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize) {
        RelationshipType relType = RelationshipType.withName(label);
        return transactionalExecute(queries, (tx, query) -> {
            try (ResourceIterator<Relationship> rels = tx.findRelationships(relType, query.getKey(), query.getValue())) {
                rels.forEachRemaining(blackhole::consume);
            }
        }, batchSize);
    }
}
