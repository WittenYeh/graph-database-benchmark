package com.graphbench.neo4j;

import com.graphbench.api.BenchmarkExecutor;
import com.graphbench.api.BenchmarkUtils;
import com.graphbench.api.ProgressCallback;
import com.graphbench.workload.*;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.file.*;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Neo4j implementation of BenchmarkExecutor using native APIs.
 * All operations are executed serially to measure per-operation latency.
 */
public class Neo4jBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/neo4j-benchmark-db";
    private static final String SNAPSHOT_PATH = "/tmp/neo4j-benchmark-db-snapshot";
    private static final String DEFAULT_DB = "neo4j";
    private static final String NODE_LABEL = "MyNode";
    private static final int BATCH_SIZE = 10000;

    private DatabaseManagementService managementService;
    private GraphDatabaseService db;
    private int errorCount = 0;
    private ProgressCallback progressCallback;
    private final Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    private Map<Long, Long> nodeIdsMap = new HashMap<>(); // Maps origin ID to Neo4j internal node ID

    public Neo4jBenchmarkExecutor() {
        this.progressCallback = new ProgressCallback(System.getenv("PROGRESS_CALLBACK_URL"));
    }

    @Override
    public String getDatabaseName() {
        return "neo4j";
    }

    @Override
    public String getDatabasePath() {
        return DB_PATH;
    }

    @Override
    public String getSnapshotPath() {
        return SNAPSHOT_PATH;
    }

    @Override
    public void closeDatabase() throws Exception {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
            db = null;
        }
    }

    @Override
    public void openDatabase() throws Exception {
        managementService = new DatabaseManagementServiceBuilder(new File(DB_PATH).toPath())
            .build();
        db = managementService.database(DEFAULT_DB);
    }

    @Override
    public void initDatabase() throws Exception {
        // Check and clean database directory if not empty
        BenchmarkUtils.checkAndCleanDatabaseDirectory(DB_PATH);

        // Create new database
        managementService = new DatabaseManagementServiceBuilder(Paths.get(DB_PATH)).build();
        db = managementService.database(DEFAULT_DB);

        progressCallback.sendLogMessage("Neo4j database initialized", "INFO");
    }

    @Override
    public void shutdown() throws Exception {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        progressCallback.sendLogMessage("Loading graph from: " + datasetPath, "INFO");
        long startTime = System.nanoTime();

        // Create unique constraint on id property (creates index automatically)
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(Label.label(NODE_LABEL)).assertPropertyIsUnique("id").create();
            tx.commit();
        } catch (Exception e) {
            // Constraint may already exist, ignore
        }

        int edgeCount = 0;
        RelationshipType relType = RelationshipType.withName("MyEdge");
        nodeIdsMap.clear(); // Clear existing map

        // Stream through file and create nodes and edges in batches
        progressCallback.sendLogMessage("Loading graph data...", "INFO");
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath))) {
            String line;
            Transaction tx = db.beginTx();
            int opsInTx = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("%")) continue; // Skip comments

                String[] parts = line.split("\\s+");
                if (parts.length < 2) continue;

                long srcId, dstId;
                try {
                    srcId = Long.parseLong(parts[0]);
                    dstId = Long.parseLong(parts[1]);
                } catch (NumberFormatException e) {
                    // Skip header lines
                    continue;
                }

                // Use in-memory map to avoid repeated findNode queries
                Long srcInternalId = nodeIdsMap.get(srcId);
                Node srcNode;
                if (srcInternalId == null) {
                    srcNode = tx.createNode(Label.label(NODE_LABEL));
                    srcNode.setProperty("id", srcId);
                    nodeIdsMap.put(srcId, srcNode.getId());
                } else {
                    srcNode = tx.getNodeById(srcInternalId);
                }

                Long dstNodeId = nodeIdsMap.get(dstId);
                Node dstNode;
                if (dstNodeId == null) {
                    dstNode = tx.createNode(Label.label(NODE_LABEL));
                    dstNode.setProperty("id", dstId);
                    nodeIdsMap.put(dstId, dstNode.getId());
                } else {
                    dstNode = tx.getNodeById(dstNodeId);
                }

                // Create edge
                srcNode.createRelationshipTo(dstNode, relType);
                edgeCount++;
                opsInTx++;

                if (opsInTx >= BATCH_SIZE) {
                    tx.commit();
                    tx.close();
                    tx = db.beginTx();
                    opsInTx = 0;
                }
            }
            tx.commit();
            tx.close();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

        // Query database for node count
        int nodeCount = 0;
        try (Transaction tx = db.beginTx()) {
            nodeCount = (int) tx.getAllNodes().stream()
                .filter(node -> node.hasLabel(Label.label(NODE_LABEL)))
                .count();
            tx.commit();
        }

        progressCallback.sendLogMessage("Loaded " + nodeCount + " nodes and " + edgeCount + " edges", "INFO");

        // Create indexes for edge properties to enable fast lookups
        progressCallback.sendLogMessage("Creating indexes for edge properties...", "INFO");

        try (Transaction tx = db.beginTx()) {
            // Create indexes for common edge properties used in benchmarks
            // Note: These are relationship property indexes (Neo4j 4.3+)
            tx.execute("CREATE INDEX edge_weight_idx IF NOT EXISTS FOR ()-[r:MyEdge]-() ON (r.weight)");
            tx.execute("CREATE INDEX edge_timestamp_idx IF NOT EXISTS FOR ()-[r:MyEdge]-() ON (r.timestamp)");
            tx.execute("CREATE INDEX edge_type_idx IF NOT EXISTS FOR ()-[r:MyEdge]-() ON (r.type)");
            tx.commit();
        } catch (Exception e) {
            String errorMsg = "Failed to create edge indexes (may require Neo4j 4.3+): " + e.getMessage();
            progressCallback.sendLogMessage(errorMsg, "WARNING");
        }

        // Wait for indexes to come online
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, TimeUnit.SECONDS);
            tx.commit();
            progressCallback.sendLogMessage("Indexes created and online", "INFO");
        } catch (Exception e) {
            String errorMsg = "Timeout waiting for indexes: " + e.getMessage();
            progressCallback.sendLogMessage(errorMsg, "WARNING");
        }

        Map<String, Object> result = new HashMap<>();
        result.put("nodes", nodeCount);
        result.put("edges", edgeCount);
        result.put("durationSeconds", durationSeconds);
        return result;
    }

    /**
     * Functional interface for transactional operations.
     */
    @FunctionalInterface
    private interface TransactionalOperation<T> {
        void execute(Transaction tx, T item) throws Exception;
    }

    /**
     * Functional interface for transactional operations without parameters.
     */
    @FunctionalInterface
    private interface TransactionalOperationNoParam {
        void execute(Transaction tx) throws Exception;
    }

    /**
     * Execute a list of operations within transactions, measuring latency for each batch.
     * @param items List of items to process
     * @param operation The operation to execute for each item
     * @param batchSize Number of items to process in each transaction
     * @return List of per-operation latencies in microseconds
     */
    private <T> List<Double> transactionalExecute(List<T> items, TransactionalOperation<T> operation, int batchSize) {
        List<Double> latencies = new ArrayList<>();

        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            List<T> batch = items.subList(i, end);

            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                for (T item : batch) {
                    operation.execute(tx, item);
                }
                tx.commit();
            } catch (Exception e) {
                errorCount++;
            }
            long endNs = System.nanoTime();
            // Calculate per-operation latency by dividing batch latency by actual batch size
            double batchLatencyUs = (endNs - startNs) / 1000.0;
            latencies.add(batchLatencyUs / batch.size());
        }

        return latencies;
    }

    /**
     * Execute operations without parameters within transactions.
     * @param count Number of operations to execute
     * @param operation The operation to execute
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of per-operation latencies in microseconds
     */
    private List<Double> transactionalExecute(int count, TransactionalOperationNoParam operation, int batchSize) {
        List<Double> latencies = new ArrayList<>();

        for (int i = 0; i < count; i += batchSize) {
            int end = Math.min(i + batchSize, count);
            int batchCount = end - i;

            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                for (int j = 0; j < batchCount; j++) {
                    operation.execute(tx);
                }
                tx.commit();
            } catch (Exception e) {
                errorCount += batchCount;
            }
            long endNs = System.nanoTime();
            // Calculate per-operation latency by dividing batch latency by actual batch size
            double batchLatencyUs = (endNs - startNs) / 1000.0;
            latencies.add(batchLatencyUs / batchCount);
        }

        return latencies;
    }

    @Override
    public List<Double> addVertex(int count, int batchSize) {
        return transactionalExecute(count, tx -> {
            Node node = tx.createNode(Label.label(NODE_LABEL));
        }, batchSize);
    }

    @Override
    public List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize) {
        return transactionalExecute(updates, (tx, update) -> {
            Node node = tx.getNodeById((Long) update.getSystemId());
            if (node != null) {
                for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                    node.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> removeVertex(List<Object> systemIds, int batchSize) {
        return transactionalExecute(systemIds, (tx, systemId) -> {
            Node node = tx.getNodeById((Long) systemId);
            if (node != null) {
                // Delete all relationships first
                for (Relationship rel : node.getRelationships()) {
                    rel.delete();
                }
                node.delete();
            }
        }, batchSize);
    }

    @Override
    public List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs, int batchSize) {
        RelationshipType relType = RelationshipType.withName(label);
        return transactionalExecute(pairs, (tx, pair) -> {
            Node srcNode = tx.getNodeById((Long) pair.getSrcSystemId());
            Node dstNode = tx.getNodeById((Long) pair.getDstSystemId());
            if (srcNode != null && dstNode != null) {
                srcNode.createRelationshipTo(dstNode, relType);
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
                // Find the edge between src and dst
                for (Relationship rel : srcNode.getRelationships(Direction.OUTGOING, relType)) {
                    if (rel.getEndNode().equals(dstNode)) {
                        for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                            rel.setProperty(entry.getKey(), entry.getValue());
                        }
                        break;
                    }
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs, int batchSize) {
        RelationshipType relType = RelationshipType.withName(label);
        return transactionalExecute(pairs, (tx, pair) -> {
            Node srcNode = tx.getNodeById((Long) pair.getSrcSystemId());
            Node dstNode = tx.getNodeById((Long) pair.getDstSystemId());
            if (srcNode != null && dstNode != null) {
                // Find and delete the edge
                for (Relationship rel : srcNode.getRelationships(Direction.OUTGOING, relType)) {
                    if (rel.getEndNode().equals(dstNode)) {
                        rel.delete();
                        break;
                    }
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> getNbrs(String direction, List<Object> systemIds, int batchSize) {
        // Map direction string to Neo4j Direction enum
        Direction dir;
        switch (direction.toUpperCase()) {
            case "OUT": case "OUTGOING":
                dir = Direction.OUTGOING;
                break;
            case "IN": case "INCOMING":
                dir = Direction.INCOMING;
                break;
            case "BOTH":
                dir = Direction.BOTH;
                break;
            default:
                throw new IllegalArgumentException("Invalid direction: " + direction);
        }

        return transactionalExecute(systemIds, (tx, systemId) -> {
            Node node = tx.getNodeById((Long) systemId);
            if (node != null) {
                for (Relationship rel : node.getRelationships(dir)) {
                    Node neighbor = rel.getOtherNode(node);
                    blackhole.consume(neighbor.getProperty("id"));
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalExecute(queries, (tx, query) -> {
            // Use native findNodes API to leverage indexes
            try (ResourceIterator<Node> nodes = tx.findNodes(Label.label(NODE_LABEL), query.getKey(), query.getValue())) {
                nodes.forEachRemaining(blackhole::consume);
            }
        }, batchSize);
    }

    @Override
    public List<Double> getEdgeByProperty(String label, List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize) {
        RelationshipType relType = RelationshipType.withName(label);
        return transactionalExecute(queries, (tx, query) -> {
            // Use Native API for efficient edge property lookup with index
            // This leverages relationship property indexes (Neo4j 4.3+)
            try (ResourceIterator<Relationship> relationships =
                    tx.findRelationships(relType, query.getKey(), query.getValue())) {
                // Consume results to ensure database actually performs the lookup
                relationships.forEachRemaining(blackhole::consume);
            }
        }, batchSize);
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void resetErrorCount() {
        errorCount = 0;
    }

    @Override
    public Object getSystemId(Long originId) {
        return nodeIdsMap.get(originId);
    }
}
