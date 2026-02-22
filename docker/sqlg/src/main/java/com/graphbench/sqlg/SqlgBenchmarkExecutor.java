package com.graphbench.sqlg;

import com.graphbench.api.BenchmarkExecutor;
import com.graphbench.api.BenchmarkUtils;
import com.graphbench.api.ProgressCallback;
import com.graphbench.workload.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.openjdk.jmh.infra.Blackhole;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;

/**
 * SQLG structural benchmark executor using TinkerPop Structure APIs.
 * Loads graph topology (node IDs + edges) from CSV. Ignores property columns.
 * Property operations are in SqlgPropertyBenchmarkExecutor.
 */
public class SqlgBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/sqlg-benchmark-db";
    private static final String SNAPSHOT_PATH = "/tmp/sqlg-benchmark-db-snapshot";
    protected static final String NODE_LABEL = "MyNode";
    protected static final int LOAD_BATCH_SIZE = 10000;

    protected SqlgGraph graph;
    protected GraphTraversalSource g;
    protected int errorCount = 0;
    protected ProgressCallback progressCallback;
    protected final Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    protected Map<Long, Object> nodeIdsMap = new HashMap<>();

    public SqlgBenchmarkExecutor() {
        this.progressCallback = new ProgressCallback(System.getenv("PROGRESS_CALLBACK_URL"));
    }

    @Override
    public String getDatabaseName() { return "sqlg"; }

    @Override
    public String getDatabasePath() { return DB_PATH; }

    @Override
    public String getSnapshotPath() { return SNAPSHOT_PATH; }

    @Override
    public void closeDatabase() throws Exception {
        if (graph != null) { graph.close(); }
    }

    @Override
    public void openDatabase() throws Exception {
        org.apache.commons.configuration2.Configuration config = new org.apache.commons.configuration2.builder.fluent.Configurations()
            .properties(DB_PATH + "/sqlg.properties");
        graph = SqlgGraph.open(config);
        g = graph.traversal();
    }

    @Override
    public void initDatabase() throws Exception {
        BenchmarkUtils.checkAndCleanDatabaseDirectory(DB_PATH);

        // Create configuration file for H2 embedded database
        java.io.File configFile = new java.io.File(DB_PATH + "/sqlg.properties");
        configFile.getParentFile().mkdirs();
        try (java.io.PrintWriter writer = new java.io.PrintWriter(configFile)) {
            writer.println("jdbc.url=jdbc:h2:file:" + DB_PATH + "/sqlgdb");
            writer.println("jdbc.username=SA");
            writer.println("jdbc.password=");
        }

        org.apache.commons.configuration2.Configuration config = new org.apache.commons.configuration2.builder.fluent.Configurations()
            .properties(DB_PATH + "/sqlg.properties");
        graph = SqlgGraph.open(config);
        g = graph.traversal();

        progressCallback.sendLogMessage("SQLG initialized with H2 embedded backend", "INFO");
    }

    @Override
    public void shutdown() throws Exception {
        if (graph != null) { graph.close(); }
    }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        SqlgGraphLoader loader = new SqlgGraphLoader(graph, g, progressCallback, false);
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        return result;
    }

    // --- Transactional execution helpers (protected for subclass access) ---

    @FunctionalInterface
    protected interface TransactionalOperation<T> {
        void execute(T item) throws Exception;
    }

    /**
     * Execute operations in batches with transaction management.
     * Measures latency per operation.
     */
    protected <T> List<Double> transactionalBatchExecute(List<T> items, int batchSize, TransactionalOperation<T> operation) {
        List<Double> latencies = new ArrayList<>();
        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            List<T> batch = items.subList(i, end);

            long startNs = System.nanoTime();
            try {
                for (T item : batch) {
                    operation.execute(item);
                }
                graph.tx().commit();
            } catch (Exception e) {
                graph.tx().rollback();
                errorCount += batch.size();
            }
            long endNs = System.nanoTime();
            double latencyUs = (endNs - startNs) / 1000.0 / batch.size();
            latencies.add(latencyUs);
        }
        return latencies;
    }

    /**
     * Execute count-based operations in batches with transaction management.
     */
    protected List<Double> transactionalBatchExecute(int count, int batchSize, Runnable operation) {
        List<Double> latencies = new ArrayList<>();
        for (int i = 0; i < count; i += batchSize) {
            int batchCount = Math.min(batchSize, count - i);

            long startNs = System.nanoTime();
            try {
                for (int j = 0; j < batchCount; j++) {
                    operation.run();
                }
                graph.tx().commit();
            } catch (Exception e) {
                graph.tx().rollback();
                errorCount += batchCount;
            }
            long endNs = System.nanoTime();
            double latencyUs = (endNs - startNs) / 1000.0 / batchCount;
            latencies.add(latencyUs);
        }
        return latencies;
    }

    // --- Benchmark Operations ---

    @Override
    public List<Double> addVertex(int count, int batchSize) {
        return transactionalBatchExecute(count, batchSize, () -> {
            g.addV(NODE_LABEL).iterate();
        });
    }

    @Override
    public List<Double> removeVertex(List<Object> systemIds, int batchSize) {
        return transactionalBatchExecute(systemIds, batchSize, id -> {
            g.V(id).drop().iterate();
        });
    }

    @Override
    public List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs, int batchSize) {
        return transactionalBatchExecute(pairs, batchSize, pair -> {
            g.V(pair.getSrcSystemId()).addE(label).to(__.V(pair.getDstSystemId())).iterate();
        });
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs, int batchSize) {
        return transactionalBatchExecute(pairs, batchSize, pair -> {
            g.V(pair.getSrcSystemId()).outE(label).where(__.inV().hasId(pair.getDstSystemId())).drop().iterate();
        });
    }

    @Override
    public List<Double> getNbrs(String direction, List<Object> systemIds, int batchSize) {
        return transactionalBatchExecute(systemIds, batchSize, id -> {
            List<Vertex> neighbors;
            if ("OUT".equals(direction) || "OUTGOING".equals(direction)) {
                neighbors = g.V(id).out().toList();
            } else if ("IN".equals(direction) || "INCOMING".equals(direction)) {
                neighbors = g.V(id).in().toList();
            } else {
                neighbors = g.V(id).both().toList();
            }
            blackhole.consume(neighbors);
        });
    }

    @Override
    public int getErrorCount() { return errorCount; }

    @Override
    public void resetErrorCount() { errorCount = 0; }

    @Override
    public Object getSystemId(Long originId) {
        return nodeIdsMap.get(originId);
    }

    @Override
    public ProgressCallback getProgressCallback() {
        return progressCallback;
    }
}
