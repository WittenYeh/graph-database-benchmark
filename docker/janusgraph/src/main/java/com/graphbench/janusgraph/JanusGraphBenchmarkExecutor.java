package com.graphbench.janusgraph;

import com.graphbench.api.BenchmarkExecutor;
import com.graphbench.workload.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * JanusGraph implementation of BenchmarkExecutor using TinkerPop Structure APIs.
 * All operations are executed serially to measure per-operation latency.
 */
public class JanusGraphBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/janusgraph-benchmark-db";
    private static final String NODE_LABEL = "MyNode";
    private static final int BATCH_SIZE = 10000;

    private JanusGraph graph;
    private GraphTraversalSource g;
    private Map<Long, Object> vertexIdMap = new HashMap<>(); // Maps logical ID to JanusGraph vertex ID

    @Override
    public String getDatabaseName() {
        return "janusgraph";
    }

    @Override
    public void initDatabase() throws Exception {
        // Clean up old database
        if (Files.exists(Paths.get(DB_PATH))) {
            deleteDirectory(new File(DB_PATH));
        }

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
            mgmt.makePropertyKey("id").dataType(Long.class).make();
            mgmt.makeEdgeLabel("MyEdge").make();
            mgmt.makeVertexLabel(NODE_LABEL).make();
        }
        mgmt.commit();

        System.out.println("JanusGraph initialized with BerkeleyDB backend");
    }

    @Override
    public void shutdown() throws Exception {
        if (graph != null) {
            graph.close();
        }
    }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        System.out.println("Loading graph from: " + datasetPath);
        vertexIdMap.clear();
        long startTime = System.nanoTime();

        Set<Long> uniqueNodes = new HashSet<>();
        int nodeCount = 0;
        int edgeCount = 0;

        // ==========================================
        // Pass 1: Collect unique node IDs
        // ==========================================
        System.out.println("Pass 1: Collecting unique nodes...");
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("%")) continue; // Skip comments

                String[] parts = line.split("\\s+");
                if (parts.length >= 2) {
                    try {
                        long src = Long.parseLong(parts[0]);
                        long dst = Long.parseLong(parts[1]);
                        uniqueNodes.add(src);
                        uniqueNodes.add(dst);
                    } catch (NumberFormatException e) {
                        // Skip header lines
                    }
                }
            }
        }

        // ==========================================
        // Phase 2: Batch create vertices
        // ==========================================
        System.out.println("Creating " + uniqueNodes.size() + " vertices...");
        int opsInTx = 0;

        for (Long externalId : uniqueNodes) {
            Vertex v = g.addV(NODE_LABEL).property("id", externalId).next();
            vertexIdMap.put(externalId, v.id());

            opsInTx++;
            if (opsInTx >= BATCH_SIZE) {
                g.tx().commit();
                opsInTx = 0;
            }
        }
        g.tx().commit();
        nodeCount = uniqueNodes.size();

        // Release Set memory for vertexIdMap and subsequent operations
        uniqueNodes = null;
        System.gc(); // Suggest GC, but not mandatory

        // ==========================================
        // Pass 3: Stream through file again to create edges
        // ==========================================
        System.out.println("Pass 2: Streaming edges...");
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath))) {
            String line;
            opsInTx = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("%")) continue;

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

                Object srcVertexId = vertexIdMap.get(srcId);
                Object dstVertexId = vertexIdMap.get(dstId);

                // Only create edge if both vertices exist
                if (srcVertexId != null && dstVertexId != null) {
                    g.V(srcVertexId).addE("MyEdge").to(g.V(dstVertexId)).iterate();

                    edgeCount++;
                    opsInTx++;

                    if (opsInTx >= BATCH_SIZE) {
                        g.tx().commit();
                        opsInTx = 0;
                    }
                }
            }
            g.tx().commit();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

        System.out.println("Loaded " + nodeCount + " vertices and " + edgeCount + " edges");

        Map<String, Object> result = new HashMap<>();
        result.put("nodes", nodeCount);
        result.put("edges", edgeCount);
        result.put("durationSeconds", durationSeconds);
        return result;
    }

    @Override
    public List<Double> addVertex(List<Long> ids) {
        List<Double> latencies = new ArrayList<>();

        for (Long id : ids) {
            long startNs = System.nanoTime();
            try {
                Vertex v = g.addV(NODE_LABEL).property("id", id).next();
                vertexIdMap.put(id, v.id());
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0); // Convert to microseconds
        }

        return latencies;
    }

    @Override
    public List<Double> upsertVertexProperty(List<UpsertVertexPropertyParams.VertexUpdate> updates) {
        List<Double> latencies = new ArrayList<>();

        for (UpsertVertexPropertyParams.VertexUpdate update : updates) {
            long startNs = System.nanoTime();
            try {
                Object vertexId = vertexIdMap.get(update.getId());
                if (vertexId != null) {
                    Vertex v = g.V(vertexId).next();
                    for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                        v.property(entry.getKey(), entry.getValue());
                    }
                }
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> removeVertex(List<Long> ids) {
        List<Double> latencies = new ArrayList<>();

        for (Long id : ids) {
            long startNs = System.nanoTime();
            try {
                Object vertexId = vertexIdMap.get(id);
                if (vertexId != null) {
                    g.V(vertexId).drop().iterate();
                    vertexIdMap.remove(id);
                }
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs) {
        List<Double> latencies = new ArrayList<>();

        for (AddEdgeParams.EdgePair pair : pairs) {
            long startNs = System.nanoTime();
            try {
                Object srcVertexId = vertexIdMap.get(pair.getSrc());
                Object dstVertexId = vertexIdMap.get(pair.getDst());
                if (srcVertexId != null && dstVertexId != null) {
                    g.V(srcVertexId).addE(label).to(g.V(dstVertexId)).iterate();
                }
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> upsertEdgeProperty(String label, List<UpsertEdgePropertyParams.EdgeUpdate> updates) {
        List<Double> latencies = new ArrayList<>();

        for (UpsertEdgePropertyParams.EdgeUpdate update : updates) {
            long startNs = System.nanoTime();
            try {
                Object srcVertexId = vertexIdMap.get(update.getSrc());
                Object dstVertexId = vertexIdMap.get(update.getDst());
                if (srcVertexId != null && dstVertexId != null) {
                    // Find edge from src to dst with given label
                    List<Edge> edges = g.V(srcVertexId).outE(label)
                        .where(__.inV().hasId(dstVertexId))
                        .toList();

                    for (Edge edge : edges) {
                        for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                            edge.property(entry.getKey(), entry.getValue());
                        }
                    }
                }
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs) {
        List<Double> latencies = new ArrayList<>();

        for (RemoveEdgeParams.EdgePair pair : pairs) {
            long startNs = System.nanoTime();
            try {
                Object srcVertexId = vertexIdMap.get(pair.getSrc());
                Object dstVertexId = vertexIdMap.get(pair.getDst());
                if (srcVertexId != null && dstVertexId != null) {
                    g.V(srcVertexId).outE(label)
                        .where(__.inV().hasId(dstVertexId))
                        .drop()
                        .iterate();
                }
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> getNbrs(String direction, List<Long> ids) {
        List<Double> latencies = new ArrayList<>();

        for (Long id : ids) {
            long startNs = System.nanoTime();
            try {
                Object vertexId = vertexIdMap.get(id);
                if (vertexId != null) {
                    List<Object> neighbors;
                    if ("OUT".equals(direction)) {
                        neighbors = g.V(vertexId).out().values("id").toList();
                    } else if ("IN".equals(direction)) {
                        neighbors = g.V(vertexId).in().values("id").toList();
                    } else { // BOTH
                        neighbors = g.V(vertexId).both().values("id").toList();
                    }
                }
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries) {
        List<Double> latencies = new ArrayList<>();

        for (GetVertexByPropertyParams.PropertyQuery query : queries) {
            long startNs = System.nanoTime();
            try {
                List<Vertex> results = g.V().hasLabel(NODE_LABEL)
                    .has(query.getKey(), query.getValue())
                    .toList();
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> getEdgeByProperty(String label, List<GetEdgeByPropertyParams.PropertyQuery> queries) {
        List<Double> latencies = new ArrayList<>();

        for (GetEdgeByPropertyParams.PropertyQuery query : queries) {
            long startNs = System.nanoTime();
            try {
                List<Edge> results = g.E().hasLabel(label)
                    .has(query.getKey(), query.getValue())
                    .toList();
                g.tx().commit();
            } catch (Exception e) {
                try {
                    g.tx().rollback();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    private void deleteDirectory(File directory) throws IOException {
        if (directory.exists()) {
            Files.walk(directory.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }
}
