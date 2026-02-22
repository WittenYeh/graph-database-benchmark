package com.graphbench.janusgraph;

import com.graphbench.api.BenchmarkExecutor;
import com.graphbench.api.BenchmarkUtils;
import com.graphbench.api.ProgressCallback;
import com.graphbench.workload.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;

/**
 * JanusGraph structural benchmark executor using TinkerPop Structure APIs.
 * Loads graph topology (node IDs + edges) from CSV. Ignores property columns.
 * Property operations are in JanusGraphPropertyBenchmarkExecutor.
 */
public class JanusGraphBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/janusgraph-benchmark-db";
    private static final String SNAPSHOT_PATH = "/tmp/janusgraph-benchmark-db-snapshot";
    protected static final String NODE_LABEL = "MyNode";
    protected static final int LOAD_BATCH_SIZE = 10000;

    protected JanusGraph graph;
    protected GraphTraversalSource g;
    protected int errorCount = 0;
    protected ProgressCallback progressCallback;
    protected final Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    protected Map<Long, Object> nodeIdsMap = new HashMap<>();

    public JanusGraphBenchmarkExecutor() {
        this.progressCallback = new ProgressCallback(System.getenv("PROGRESS_CALLBACK_URL"));
    }

    @Override
    public String getDatabaseName() { return "janusgraph"; }

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
        graph = JanusGraphFactory.build()
            .set("storage.backend", "berkeleyje")
            .set("storage.directory", DB_PATH)
            .set("cache.db-cache", true)
            .set("cache.db-cache-size", 0.5)
            .open();
        g = graph.traversal();
    }

    @Override
    public void initDatabase() throws Exception {
        BenchmarkUtils.checkAndCleanDatabaseDirectory(DB_PATH);

        graph = JanusGraphFactory.build()
            .set("storage.backend", "berkeleyje")
            .set("storage.directory", DB_PATH)
            .set("cache.db-cache", true)
            .set("cache.db-cache-size", 0.5)
            .open();
        g = graph.traversal();

        // Create minimal schema
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeEdgeLabel("MyEdge").make();
        mgmt.makeVertexLabel(NODE_LABEL).make();
        mgmt.commit();

        progressCallback.sendLogMessage("JanusGraph initialized with BerkeleyDB backend", "INFO");
    }

    @Override
    public void shutdown() throws Exception {
        if (graph != null) { graph.close(); }
    }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        JanusGraphGraphLoader loader = new JanusGraphGraphLoader(graph, g, progressCallback, false);
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
                for (T item : batch) { operation.execute(item); }
                g.tx().commit();
            } catch (Exception e) {
                errorCount += batch.size();
                try { g.tx().rollback(); } catch (Exception ex) { /* ignore */ }
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
                for (int j = 0; j < batchCount; j++) { operation.execute(); }
                g.tx().commit();
            } catch (Exception e) {
                errorCount += batchCount;
                try { g.tx().rollback(); } catch (Exception ex) { /* ignore */ }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0 / batchCount);
        }
        return latencies;
    }

    // --- Structural operations ---

    @Override
    public List<Double> addVertex(int count, int batchSize) {
        return transactionalExecute(count, () -> {
            g.addV(NODE_LABEL).next();
        }, batchSize);
    }

    @Override
    public List<Double> removeVertex(List<Object> systemIds, int batchSize) {
        return transactionalExecute(systemIds, systemId -> {
            g.V(systemId).drop().iterate();
        }, batchSize);
    }

    @Override
    public List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs, int batchSize) {
        return transactionalExecute(pairs, pair -> {
            g.V(pair.getSrcSystemId()).addE(label).to(g.V(pair.getDstSystemId())).iterate();
        }, batchSize);
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs, int batchSize) {
        return transactionalExecute(pairs, pair -> {
            g.V(pair.getSrcSystemId()).outE(label)
                .where(__.inV().hasId(pair.getDstSystemId()))
                .drop().iterate();
        }, batchSize);
    }

    @Override
    public List<Double> getNbrs(String direction, List<Object> systemIds, int batchSize) {
        return transactionalExecute(systemIds, systemId -> {
            if ("OUT".equals(direction)) {
                g.V(systemId).out().forEachRemaining(blackhole::consume);
            } else if ("IN".equals(direction)) {
                g.V(systemId).in().forEachRemaining(blackhole::consume);
            } else {
                g.V(systemId).both().forEachRemaining(blackhole::consume);
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
