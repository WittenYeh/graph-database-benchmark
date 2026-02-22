package com.graphbench.orientdb;

import com.graphbench.api.BenchmarkExecutor;
import com.graphbench.api.BenchmarkUtils;
import com.graphbench.api.ProgressCallback;
import com.graphbench.workload.*;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OVertexDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * OrientDB structural benchmark executor using native APIs.
 * Loads graph topology (node IDs + edges) from CSV. Ignores property columns.
 * Property operations are in OrientDBPropertyBenchmarkExecutor.
 */
public class OrientDBBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/orientdb-benchmark-db";
    private static final String SNAPSHOT_PATH = "/tmp/orientdb-benchmark-db-snapshot";
    private static final String DB_NAME = "benchmark";
    protected static final String VERTEX_CLASS = "MyNode";
    protected static final String EDGE_CLASS = "MyEdge";
    protected static final int LOAD_BATCH_SIZE = 10000;

    protected OrientDB orientDB;
    protected ODatabaseSession db;
    protected int errorCount = 0;
    protected ProgressCallback progressCallback;
    protected final Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    protected Map<Long, Object> nodeIdsMap = new HashMap<>();

    public OrientDBBenchmarkExecutor() {
        this.progressCallback = new ProgressCallback(System.getenv("PROGRESS_CALLBACK_URL"));
    }

    @Override
    public String getDatabaseName() { return "orientdb"; }

    @Override
    public String getDatabasePath() { return DB_PATH; }

    @Override
    public String getSnapshotPath() { return SNAPSHOT_PATH; }

    @Override
    public void closeDatabase() throws Exception {
        if (db != null && !db.isClosed()) {
            db.close();
            db = null;
        }
        if (orientDB != null) {
            orientDB.close();
            orientDB = null;
        }
    }

    @Override
    public void openDatabase() throws Exception {
        orientDB = new OrientDB("plocal:" + DB_PATH, OrientDBConfig.defaultConfig());
        db = orientDB.open(DB_NAME, "admin", "admin");
    }

    @Override
    public void initDatabase() throws Exception {
        BenchmarkUtils.checkAndCleanDatabaseDirectory(DB_PATH);
        orientDB = new OrientDB("plocal:" + DB_PATH, OrientDBConfig.defaultConfig());
        orientDB.execute("create database " + DB_NAME + " plocal users (admin identified by 'admin' role admin)");
        db = orientDB.open(DB_NAME, "admin", "admin");

        // Create vertex and edge classes
        if (!db.getMetadata().getSchema().existsClass(VERTEX_CLASS)) {
            db.createVertexClass(VERTEX_CLASS);
        }
        if (!db.getMetadata().getSchema().existsClass(EDGE_CLASS)) {
            db.createEdgeClass(EDGE_CLASS);
        }

        progressCallback.sendLogMessage("OrientDB database initialized", "INFO");
    }

    @Override
    public void shutdown() throws Exception {
        closeDatabase();
    }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        OrientDBGraphLoader loader = new OrientDBGraphLoader(db, progressCallback, false);
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        return result;
    }

    // --- Transactional execution helpers (protected for subclass access) ---

    @FunctionalInterface
    protected interface TransactionalOperation<T> {
        void execute(T item) throws Exception;
    }

    @FunctionalInterface
    protected interface TransactionalOperationNoParam {
        void execute() throws Exception;
    }

    protected <T> List<Double> transactionalExecute(List<T> items, TransactionalOperation<T> operation, int batchSize) {
        List<Double> latencies = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            List<T> batch = items.subList(i, end);
            long startNs = System.nanoTime();
            try {
                db.begin();
                for (T item : batch) { operation.execute(item); }
                db.commit();
            } catch (Exception e) {
                db.rollback();
                errorCount++;
            }
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
            try {
                db.begin();
                for (int j = 0; j < batchCount; j++) { operation.execute(); }
                db.commit();
            } catch (Exception e) {
                db.rollback();
                errorCount += batchCount;
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0 / batchCount);
        }
        return latencies;
    }

    @Override
    public List<Double> addVertex(int count, int batchSize) {
        return transactionalExecute(count, () -> {
            db.newVertex(VERTEX_CLASS).save();
        }, batchSize);
    }

    @Override
    public List<Double> removeVertex(List<Object> systemIds, int batchSize) {
        return transactionalExecute(systemIds, systemId -> {
            OVertex vertex = db.load((com.orientechnologies.orient.core.id.ORID) systemId);
            if (vertex != null) {
                vertex.delete();
            }
        }, batchSize);
    }

    @Override
    public List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs, int batchSize) {
        return transactionalExecute(pairs, pair -> {
            OVertex src = db.load((com.orientechnologies.orient.core.id.ORID) pair.getSrcSystemId());
            OVertex dst = db.load((com.orientechnologies.orient.core.id.ORID) pair.getDstSystemId());
            if (src != null && dst != null) {
                src.addEdge(dst, EDGE_CLASS).save();
            }
        }, batchSize);
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs, int batchSize) {
        return transactionalExecute(pairs, pair -> {
            OVertex src = db.load((com.orientechnologies.orient.core.id.ORID) pair.getSrcSystemId());
            OVertex dst = db.load((com.orientechnologies.orient.core.id.ORID) pair.getDstSystemId());
            if (src != null && dst != null) {
                Iterable<OEdge> edges = src.getEdges(ODirection.OUT, EDGE_CLASS);
                for (OEdge edge : edges) {
                    if (edge.getTo().equals(dst)) {
                        edge.delete();
                        break;
                    }
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> getNbrs(String direction, List<Object> systemIds, int batchSize) {
        ODirection dir;
        switch (direction.toUpperCase()) {
            case "OUT": case "OUTGOING": dir = ODirection.OUT; break;
            case "IN": case "INCOMING": dir = ODirection.IN; break;
            case "BOTH": dir = ODirection.BOTH; break;
            default: throw new IllegalArgumentException("Invalid direction: " + direction);
        }
        return transactionalExecute(systemIds, systemId -> {
            OVertex vertex = db.load((com.orientechnologies.orient.core.id.ORID) systemId);
            if (vertex != null) {
                vertex.getVertices(dir).iterator().forEachRemaining(blackhole::consume);
            }
        }, batchSize);
    }

    public int getErrorCount() { return errorCount; }

    public void resetErrorCount() { errorCount = 0; }

    @Override
    public ProgressCallback getProgressCallback() { return progressCallback; }

    @Override
    public Object getSystemId(Long originId) { return nodeIdsMap.get(originId); }
}