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

import java.io.*;
import java.nio.file.*;
import java.nio.file.StandardCopyOption;
import java.util.*;

/**
 * JanusGraph implementation of BenchmarkExecutor using TinkerPop Structure APIs.
 * All operations are executed serially to measure per-operation latency.
 */
public class JanusGraphBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/janusgraph-benchmark-db";
    private static final String SNAPSHOT_PATH = "/tmp/janusgraph-benchmark-db-snapshot";
    private static final String NODE_LABEL = "MyNode";
    private static final int BATCH_SIZE = 10000;

    private JanusGraph graph;
    private GraphTraversalSource g;
    private int errorCount = 0;
    private ProgressCallback progressCallback;
    private final Blackhole blackhole = new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    private Map<Long, Object> nodeIdsMap = new HashMap<>(); // Maps origin ID to JanusGraph internal ID

    public JanusGraphBenchmarkExecutor() {
        this.progressCallback = new ProgressCallback(System.getenv("PROGRESS_CALLBACK_URL"));
    }

    @Override
    public String getDatabaseName() {
        return "janusgraph";
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
        if (graph != null) {
            graph.close();
        }
    }

    @Override
    public void openDatabase() throws Exception {
        JanusGraphFactory.Builder config = JanusGraphFactory.build()
            .set("storage.backend", "berkeleyje")
            .set("storage.directory", DB_PATH)
            .set("cache.db-cache", true)
            .set("cache.db-cache-size", 0.5);

        graph = config.open();
        g = graph.traversal();
    }

    @Override
    public void initDatabase() throws Exception {
        // Check and clean database directory if not empty
        BenchmarkUtils.checkAndCleanDatabaseDirectory(DB_PATH);

        // Create new database with BerkeleyDB backend
        JanusGraphFactory.Builder config = JanusGraphFactory.build()
            .set("storage.backend", "berkeleyje")
            .set("storage.directory", DB_PATH)
            .set("cache.db-cache", true)
            .set("cache.db-cache-size", 0.5);

        graph = config.open();
        g = graph.traversal();

        // Create schema
        JanusGraphManagement mgmt = graph.openManagement();
        if (mgmt.getPropertyKey("id") == null) {
            PropertyKey idKey = mgmt.makePropertyKey("id").dataType(Long.class).make();
            mgmt.makeEdgeLabel("MyEdge").make();
            mgmt.makeVertexLabel(NODE_LABEL).make();

            // Create unique composite index on id property
            mgmt.buildIndex("idIndex", Vertex.class).addKey(idKey).unique().buildCompositeIndex();
        }
        mgmt.commit();

        progressCallback.sendLogMessage("JanusGraph initialized with BerkeleyDB backend", "INFO");
    }

    @Override
    public void shutdown() throws Exception {
        if (graph != null) {
            graph.close();
        }
    }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        progressCallback.sendLogMessage("Loading graph from: " + datasetPath, "INFO");
        long startTime = System.nanoTime();

        int edgeCount = 0;
        nodeIdsMap.clear(); // Clear existing map

        // Stream through file and create vertices and edges in batches
        progressCallback.sendLogMessage("Loading graph data...", "INFO");
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath))) {
            String line;
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
                Object srcInternalId = nodeIdsMap.get(srcId);
                if (srcInternalId == null) {
                    Vertex srcVertex = g.addV(NODE_LABEL).property("id", srcId).next();
                    srcInternalId = srcVertex.id();
                    nodeIdsMap.put(srcId, srcInternalId);
                }

                Object dstInternalId = nodeIdsMap.get(dstId);
                if (dstInternalId == null) {
                    Vertex dstVertex = g.addV(NODE_LABEL).property("id", dstId).next();
                    dstInternalId = dstVertex.id();
                    nodeIdsMap.put(dstId, dstInternalId);
                }

                // Create edge
                g.V(srcInternalId).addE("MyEdge").to(__.V(dstInternalId)).iterate();
                edgeCount++;
                opsInTx++;

                if (opsInTx >= BATCH_SIZE) {
                    g.tx().commit();
                    opsInTx = 0;
                }
            }
            g.tx().commit();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

        // Query database for vertex count
        long nodeCount = g.V().hasLabel(NODE_LABEL).count().next();

        progressCallback.sendLogMessage("Loaded " + nodeCount + " vertices and " + edgeCount + " edges", "INFO");

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
        void execute(T item) throws Exception;
    }

    /**
     * Functional interface for transactional operations without parameters.
     */
    @FunctionalInterface
    private interface TransactionalOperationNoParam {
        void execute() throws Exception;
    }

    /**
     * Execute a list of operations within transactions, measuring latency for each batch.
     * @param items List of items to process
     * @param operation The operation to execute for each item
     * @param batchSize Number of operations per transaction (1 = no batching)
     * @return List of per-operation latencies in microseconds
     */
    private <T> List<Double> transactionalExecute(List<T> items, TransactionalOperation<T> operation, int batchSize) {
        List<Double> latencies = new ArrayList<>();

        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            List<T> batch = items.subList(i, end);

            long startNs = System.nanoTime();
            try {
                for (T item : batch) {
                    operation.execute(item);
                }
                g.tx().commit();
            } catch (Exception e) {
                errorCount += batch.size();
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore rollback errors
                }
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
            try {
                for (int j = 0; j < batchCount; j++) {
                    operation.execute();
                }
                g.tx().commit();
            } catch (Exception e) {
                errorCount += batchCount;
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore rollback errors
                }
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
        return transactionalExecute(count, () -> {
            g.addV(NODE_LABEL).next();
        }, batchSize);
    }

    @Override
    public List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize) {
        return transactionalExecute(updates, update -> {
            Vertex v = g.V(update.getSystemId()).tryNext().orElse(null);
            if (v != null) {
                for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                    v.property(entry.getKey(), entry.getValue());
                }
            }
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
    public List<Double> updateEdgeProperty(String label, List<UpdateEdgePropertyParams.EdgeUpdate> updates, int batchSize) {
        return transactionalExecute(updates, update -> {
            g.V(update.getSrcSystemId()).outE(label)
                .where(__.inV().hasId(update.getDstSystemId()))
                .forEachRemaining(edge -> {
                    for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                        edge.property(entry.getKey(), entry.getValue());
                    }
                });
        }, batchSize);
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs, int batchSize) {
        return transactionalExecute(pairs, pair -> {
            g.V(pair.getSrcSystemId()).outE(label)
                .where(__.inV().hasId(pair.getDstSystemId()))
                .drop()
                .iterate();
        }, batchSize);
    }

    @Override
    public List<Double> getNbrs(String direction, List<Object> systemIds, int batchSize) {
        return transactionalExecute(systemIds, systemId -> {
            if ("OUT".equals(direction)) {
                g.V(systemId).out().values("id").forEachRemaining(blackhole::consume);
            } else if ("IN".equals(direction)) {
                g.V(systemId).in().values("id").forEachRemaining(blackhole::consume);
            } else { // BOTH
                g.V(systemId).both().values("id").forEachRemaining(blackhole::consume);
            }
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

    @Override
    public Object getSystemId(Long originId) {
        return nodeIdsMap.get(originId);
    }
}
