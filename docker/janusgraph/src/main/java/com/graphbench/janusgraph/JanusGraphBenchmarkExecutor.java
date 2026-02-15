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
    private ScriptEngine gremlinEngine;
    private Bindings bindings; // Reuse bindings to avoid repeated creation

    @Override
    protected String getDatabaseName() {
        return "janusgraph";
    }

    @Override
    protected String getWarmupQuery() {
        // Simple query that doesn't depend on data
        return "g.inject(1)";
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

        // Create reusable bindings
        bindings = gremlinEngine.createBindings();
        bindings.put("g", g);

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
    protected void loadGraph(String datasetPath) throws IOException {
        System.out.println("Loading graph from: " + datasetPath);
        graphNodeIds.clear();
        graphEdgeCount = 0;

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

            graphNodeIds.addAll(vertexIdMap.values());
            graphEdgeCount = edges.size();

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

    @Override
    public void executeSingleton(String query) {
        // Query is fully determined by host, execute directly using Gremlin script engine
        try {
            // Reuse bindings instead of creating new ones
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
    protected double executeBatch(List<String> queries) {
        // For JanusGraph: execute all queries transaction-free (no explicit transaction)
        long startNs = System.nanoTime();
        try {
            // Reuse bindings instead of creating new ones
            for (String query : queries) {
                gremlinEngine.eval(query, bindings);
            }
        } catch (Exception e) {
            // Silent failure as per specification
        }
        long endNs = System.nanoTime();
        return (endNs - startNs) / 1000.0; // Convert nanoseconds to microseconds (us)
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
