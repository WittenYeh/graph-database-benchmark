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
import org.neo4j.graphdb.Transaction;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Neo4j structural benchmark executor using native APIs.
 * Loads graph topology (node IDs + edges) from CSV. Ignores property columns.
 * Property operations are in Neo4jPropertyBenchmarkExecutor.
 */
public class Neo4jBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/neo4j-benchmark-db";
    private static final String SNAPSHOT_PATH = "/tmp/neo4j-benchmark-db-snapshot";
    private static final String DEFAULT_DB = "neo4j";
    protected static final String NODE_LABEL = "MyNode";
    protected static final int LOAD_BATCH_SIZE = 10000;

    protected DatabaseManagementService managementService;
    protected GraphDatabaseService db;
    protected int errorCount = 0;
    protected ProgressCallback progressCallback;
    protected final Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    protected Map<Long, Long> nodeIdsMap = new HashMap<>();

    public Neo4jBenchmarkExecutor() {
        this.progressCallback = new ProgressCallback(System.getenv("PROGRESS_CALLBACK_URL"));
    }

    @Override
    public String getDatabaseName() { return "neo4j"; }

    @Override
    public String getDatabasePath() { return DB_PATH; }

    @Override
    public String getSnapshotPath() { return SNAPSHOT_PATH; }

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
        managementService = new DatabaseManagementServiceBuilder(new File(DB_PATH).toPath()).build();
        db = managementService.database(DEFAULT_DB);
    }

    @Override
    public void initDatabase() throws Exception {
        BenchmarkUtils.checkAndCleanDatabaseDirectory(DB_PATH);
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
        Neo4jGraphLoader loader = new Neo4jGraphLoader(db, progressCallback, false);
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        return result;
    }

    // --- Transactional execution helpers (protected for subclass access) ---

    @FunctionalInterface
    protected interface TransactionalOperation<T> {
        void execute(Transaction tx, T item) throws Exception;
    }

    @FunctionalInterface
    protected interface TransactionalOperationNoParam {
        void execute(Transaction tx) throws Exception;
    }

    protected <T> List<Double> transactionalExecute(List<T> items, TransactionalOperation<T> operation, int batchSize) {
        List<Double> latencies = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            List<T> batch = items.subList(i, end);
            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                for (T item : batch) { operation.execute(tx, item); }
                tx.commit();
            } catch (Exception e) { errorCount++; }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0 / batch.size());
        }
        return latencies;
    }

    protected List<Double> transactionalExecute(int count, TransactionalOperationNoParam operation, int batchSize) {
        List<Double> latencies = new ArrayList<>();
        for (int i = 0; i < count; i += batchSize) {
            int batchCount = Math.min(i + batchSize, count) - i;
            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                for (int j = 0; j < batchCount; j++) { operation.execute(tx); }
                tx.commit();
            } catch (Exception e) { errorCount += batchCount; }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0 / batchCount);
        }
        return latencies;
    }

    // --- Structural operations ---

    @Override
    public List<Double> addVertex(int count, int batchSize) {
        return transactionalExecute(count, tx -> {
            tx.createNode(Label.label(NODE_LABEL));
        }, batchSize);
    }

    @Override
    public List<Double> removeVertex(List<Object> systemIds, int batchSize) {
        return transactionalExecute(systemIds, (tx, systemId) -> {
            Node node = tx.getNodeById((Long) systemId);
            if (node != null) {
                for (Relationship rel : node.getRelationships()) { rel.delete(); }
                node.delete();
            }
        }, batchSize);
    }

    @Override
    public List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs, int batchSize) {
        RelationshipType relType = RelationshipType.withName(label);
        return transactionalExecute(pairs, (tx, pair) -> {
            Node src = tx.getNodeById((Long) pair.getSrcSystemId());
            Node dst = tx.getNodeById((Long) pair.getDstSystemId());
            if (src != null && dst != null) { src.createRelationshipTo(dst, relType); }
        }, batchSize);
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs, int batchSize) {
        RelationshipType relType = RelationshipType.withName(label);
        return transactionalExecute(pairs, (tx, pair) -> {
            Node src = tx.getNodeById((Long) pair.getSrcSystemId());
            Node dst = tx.getNodeById((Long) pair.getDstSystemId());
            if (src != null && dst != null) {
                for (Relationship rel : src.getRelationships(Direction.OUTGOING, relType)) {
                    if (rel.getEndNode().equals(dst)) { rel.delete(); break; }
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> getNbrs(String direction, List<Object> systemIds, int batchSize) {
        Direction dir;
        switch (direction.toUpperCase()) {
            case "OUT": case "OUTGOING": dir = Direction.OUTGOING; break;
            case "IN": case "INCOMING": dir = Direction.INCOMING; break;
            case "BOTH": dir = Direction.BOTH; break;
            default: throw new IllegalArgumentException("Invalid direction: " + direction);
        }
        return transactionalExecute(systemIds, (tx, systemId) -> {
            Node node = tx.getNodeById((Long) systemId);
            if (node != null) {
                for (Relationship rel : node.getRelationships(dir)) {
                    blackhole.consume(rel.getOtherNode(node));
                }
            }
        }, batchSize);
    }

    public int getErrorCount() { return errorCount; }

    public void resetErrorCount() { errorCount = 0; }

    @Override
    public Object getSystemId(Long originId) { return nodeIdsMap.get(originId); }
}
