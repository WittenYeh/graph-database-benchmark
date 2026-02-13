package com.graphbench.janusgraph;

import com.graphbench.api.AbstractBenchmarkExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.Bindings;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class JanusGraphBenchmarkExecutor extends AbstractBenchmarkExecutor {
    private static final String DB_PATH = "/tmp/janusgraph-benchmark-db";

    private JanusGraph graph;
    private GraphTraversalSource g;
    private List<Object> vertexIds = new ArrayList<>();
    private ScriptEngine gremlinEngine;

    @Override
    protected String getDatabaseName() {
        return "janusgraph";
    }

    @Override
    public void initDatabase() throws Exception {
        // Clean up old database
        try {
            if (Files.exists(Paths.get(DB_PATH))) {
                deleteDirectory(new File(DB_PATH));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create new database with BerkeleyDB backend
        JanusGraphFactory.Builder config = JanusGraphFactory.build()
            .set("storage.backend", "berkeleyje")
            .set("storage.directory", DB_PATH)
            .set("cache.db-cache", true)
            .set("cache.db-cache-size", 0.5);

        graph = config.open();
        g = graph.traversal();

        // Initialize Groovy script engine for executing query strings
        ScriptEngineManager manager = new ScriptEngineManager();
        gremlinEngine = manager.getEngineByName("groovy");

        if (gremlinEngine == null) {
            System.err.println("ERROR: Failed to initialize Groovy script engine");
            System.err.println("Available engines:");
            for (javax.script.ScriptEngineFactory factory : manager.getEngineFactories()) {
                System.err.println("  - " + factory.getEngineName() + " (" + factory.getLanguageName() + ")");
            }
            throw new RuntimeException("Groovy script engine not available");
        }

        System.out.println("Groovy script engine initialized successfully");

        // Create schema
        JanusGraphManagement mgmt = graph.openManagement();
        if (mgmt.getPropertyKey("id") == null) {
            mgmt.makePropertyKey("id").dataType(Long.class).make();
            mgmt.makeEdgeLabel("MyEdge").make();
            mgmt.makeVertexLabel("MyNode").make();
        }
        mgmt.commit();

        System.out.println("JanusGraph initialized with BerkeleyDB backend");
    }

    @Override
    public Map<String, Object> executeTask(String taskName, List<String> queries, int clientThreads, String datasetPath) throws Exception {
        Map<String, Object> result = new HashMap<>();
        result.put("task", taskName);

        long startTime = System.nanoTime();

        try {
            if ("load_graph".equals(taskName)) {
                loadGraph(datasetPath);
                result.put("status", "success");
                result.put("totalOps", vertexIds.size());
                result.put("clientThreads", 0);
            } else if (clientThreads == 1) {
                // Latency test
                executeLatencyTest(queries, result);
            } else {
                // Throughput test
                executeThroughputTest(queries, clientThreads, result);
            }
        } catch (Exception e) {
            result.put("status", "failed");
            result.put("error", e.getMessage());
            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        result.put("durationSeconds", durationSeconds);

        return result;
    }

    private void loadGraph(String datasetPath) throws IOException {
        System.out.println("Loading graph from: " + datasetPath);
        vertexIds.clear();

        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath))) {
            String line;
            boolean headerPassed = false;

            // Parse MTX file
            Set<Long> uniqueNodes = new HashSet<>();
            List<long[]> edges = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("%")) continue;
                if (!headerPassed) {
                    headerPassed = true;
                    continue;
                }

                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    long src = Long.parseLong(parts[0]);
                    long dst = Long.parseLong(parts[1]);
                    uniqueNodes.add(src);
                    uniqueNodes.add(dst);
                    edges.add(new long[]{src, dst});
                }
            }

            // Create vertices
            Map<Long, Object> vertexIdMap = new HashMap<>();
            for (Long nodeId : uniqueNodes) {
                Vertex v = g.addV("MyNode").property("id", nodeId).next();
                vertexIdMap.put(nodeId, v.id());
            }
            g.tx().commit();

            vertexIds.addAll(vertexIdMap.values());

            // Create edges in batches
            int batchSize = 10000;
            for (int i = 0; i < edges.size(); i += batchSize) {
                int end = Math.min(i + batchSize, edges.size());
                for (int j = i; j < end; j++) {
                    long[] edge = edges.get(j);
                    Object srcVertexId = vertexIdMap.get(edge[0]);
                    Object dstVertexId = vertexIdMap.get(edge[1]);
                    if (srcVertexId != null && dstVertexId != null) {
                        g.V(srcVertexId).addE("MyEdge").to(__.V(dstVertexId)).iterate();
                    }
                }
                g.tx().commit();
            }

            System.out.println("Loaded " + uniqueNodes.size() + " vertices and " + edges.size() + " edges");
        }
    }

    private void executeLatencyTest(List<String> queries, Map<String, Object> result) throws InterruptedException {
        // For latency tests, execute queries serially with individual transactions
        List<Double> latencies = executeConcurrent(queries, 1);

        result.put("status", "success");
        result.put("totalOps", queries.size());
        result.put("clientThreads", 1);
        result.put("latency", calculateLatencyStats(latencies));
    }

    private void executeThroughputTest(List<String> queries, int clientThreads, Map<String, Object> result) throws InterruptedException {
        executeConcurrent(queries, clientThreads);

        result.put("status", "success");
        result.put("totalOps", queries.size());
        result.put("clientThreads", clientThreads);
    }

    @Override
    public void executeQuery(String query) {
        // Query is fully determined by host, execute directly using Gremlin script engine
        try {
            Bindings bindings = gremlinEngine.createBindings();
            bindings.put("g", g);
            gremlinEngine.eval(query, bindings);
            g.tx().commit();
        } catch (Exception e) {
            try {
                g.tx().rollback();
            } catch (Exception ex) {
                // Ignore
            }
        }
    }

    @Override
    public byte[] snapshotGraph() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(new ArrayList<>(vertexIds));
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void restoreGraph(byte[] snapshot) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(snapshot);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            vertexIds = (List<Object>) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        if (graph != null) {
            try {
                graph.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void deleteDirectory(File directory) throws IOException {
        if (directory.exists()) {
            Files.walk(directory.toPath())
                .sorted(Comparator.reverseOrder())
                .map(java.nio.file.Path::toFile)
                .forEach(File::delete);
        }
    }
}
