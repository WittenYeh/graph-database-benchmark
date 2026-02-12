package com.graphbench.db.neo4j;

import com.graphbench.config.DatabaseInstanceConfig;
import com.graphbench.db.DatabaseAdapter;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Neo4j embedded database adapter using the Community Edition Java API.
 *
 * Uses the native Neo4j transaction API for typed operations and
 * Cypher execution for compiled query strings.
 */
public class Neo4jAdapter implements DatabaseAdapter {

    private Label nodeLabel;
    private RelationshipType edgeType;
    private String idProperty;
    private int batchSize;
    private int indexTimeoutSeconds;

    private DatabaseManagementService managementService;
    private GraphDatabaseService db;

    @Override
    public void open(String storagePath, DatabaseInstanceConfig config) {
        // Load configuration
        this.nodeLabel = Label.label(config.getSchemaString("node_label"));
        this.edgeType = RelationshipType.withName(config.getSchemaString("edge_type"));
        this.idProperty = config.getSchemaString("id_property");
        this.batchSize = config.getPerformanceInt("batch_size");
        this.indexTimeoutSeconds = config.getIndexingInt("index_timeout_seconds");
        managementService = new DatabaseManagementServiceBuilder(Path.of(storagePath))
                .build();
        db = managementService.database("neo4j");

        // Create index for fast lookups
        Boolean createIndex = config.getIndexingBoolean("create_id_index");
        if (createIndex != null && createIndex) {
            try (Transaction tx = db.beginTx()) {
                tx.schema().indexFor(nodeLabel).on(idProperty).create();
                tx.commit();
            } catch (Exception e) {
                // Index may already exist on restart; ignore
            }

            // Wait for index to come online
            try (Transaction tx = db.beginTx()) {
                tx.schema().awaitIndexesOnline(indexTimeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
                tx.commit();
            }
        }
    }

    @Override
    public void close() {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Override
    public String getName() {
        return "neo4j";
    }

    @Override
    public void addNode(long nodeId) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(nodeLabel);
            node.setProperty(idProperty, nodeId);
            tx.commit();
        }
    }

    @Override
    public void addEdge(long srcId, long dstId) {
        try (Transaction tx = db.beginTx()) {
            Node src = tx.findNode(nodeLabel, idProperty, srcId);
            Node dst = tx.findNode(nodeLabel, idProperty, dstId);
            if (src != null && dst != null) {
                src.createRelationshipTo(dst, edgeType);
            }
            tx.commit();
        }
    }

    @Override
    public void deleteNode(long nodeId) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.findNode(nodeLabel, idProperty, nodeId);
            if (node != null) {
                // Delete all relationships first
                for (Relationship rel : node.getRelationships()) {
                    rel.delete();
                }
                node.delete();
            }
            tx.commit();
        }
    }

    @Override
    public void deleteEdge(long srcId, long dstId) {
        try (Transaction tx = db.beginTx()) {
            Node src = tx.findNode(nodeLabel, idProperty, srcId);
            if (src != null) {
                for (Relationship rel : src.getRelationships(edgeType)) {
                    Node other = rel.getOtherNode(src);
                    if (other.hasProperty(idProperty)
                            && (long) other.getProperty(idProperty) == dstId) {
                        rel.delete();
                        break;
                    }
                }
            }
            tx.commit();
        }
    }

    @Override
    public List<Long> readNeighbors(long nodeId) {
        List<Long> neighbors = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            Node node = tx.findNode(nodeLabel, idProperty, nodeId);
            if (node != null) {
                for (Relationship rel : node.getRelationships()) {
                    Node other = rel.getOtherNode(node);
                    if (other.hasProperty(idProperty)) {
                        neighbors.add((long) other.getProperty(idProperty));
                    }
                }
            }
            tx.commit();
        }
        return neighbors;
    }

    @Override
    public void loadGraph(List<long[]> edges, int numNodes) {
        // Batch-insert nodes
        for (int i = 1; i <= numNodes; i += batchSize) {
            try (Transaction tx = db.beginTx()) {
                int end = Math.min(i + batchSize, numNodes + 1);
                for (int id = i; id < end; id++) {
                    Node node = tx.createNode(nodeLabel);
                    node.setProperty(idProperty, (long) id);
                }
                tx.commit();
            }
        }

        // Batch-insert edges
        for (int i = 0; i < edges.size(); i += batchSize) {
            try (Transaction tx = db.beginTx()) {
                int end = Math.min(i + batchSize, edges.size());
                for (int j = i; j < end; j++) {
                    long[] edge = edges.get(j);
                    Node src = tx.findNode(nodeLabel, idProperty, edge[0]);
                    Node dst = tx.findNode(nodeLabel, idProperty, edge[1]);
                    if (src != null && dst != null) {
                        src.createRelationshipTo(dst, edgeType);
                    }
                }
                tx.commit();
            }
        }
    }

    @Override
    public void executeQuery(String query) {
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            // Consume the result to ensure the query is fully executed
            while (result.hasNext()) {
                result.next();
            }
            tx.commit();
        }
    }
}
