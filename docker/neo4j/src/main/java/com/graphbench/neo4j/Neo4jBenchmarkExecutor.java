package com.graphbench.neo4j;

import com.graphbench.api.BenchmarkExecutor;
import com.graphbench.workload.*;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Neo4j implementation of BenchmarkExecutor using native APIs.
 * All operations are executed serially to measure per-operation latency.
 */
public class Neo4jBenchmarkExecutor implements BenchmarkExecutor {
    private static final String DB_PATH = "/tmp/neo4j-benchmark-db";
    private static final String DEFAULT_DB = "neo4j";
    private static final String NODE_LABEL = "MyNode";
    private static final int BATCH_SIZE = 10000;

    private DatabaseManagementService managementService;
    private GraphDatabaseService db;
    private Map<Long, Long> nodeIdMap = new HashMap<>(); // Maps logical ID to Neo4j internal ID

    @Override
    public String getDatabaseName() {
        return "neo4j";
    }

    @Override
    public void initDatabase() throws Exception {
        // Clean up old database
        if (Files.exists(Paths.get(DB_PATH))) {
            deleteDirectory(new File(DB_PATH));
        }

        // Create new database
        managementService = new DatabaseManagementServiceBuilder(Paths.get(DB_PATH)).build();
        db = managementService.database(DEFAULT_DB);

        System.out.println("Neo4j database initialized");
    }

    @Override
    public void shutdown() throws Exception {
        if (managementService != null) {
            managementService.shutdown();
        }
    }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        System.out.println("Loading graph from: " + datasetPath);
        nodeIdMap.clear();
        long startTime = System.nanoTime();

        // Step 0: Create schema index to prevent full table scans
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(Label.label(NODE_LABEL)).on("id").create();
            tx.commit();
        } catch (Exception e) {
            // Index may already exist, ignore
        }

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
        // Phase 2: Batch create nodes
        // ==========================================
        System.out.println("Creating " + uniqueNodes.size() + " nodes...");
        Transaction tx = db.beginTx();
        int opsInTx = 0;

        for (Long externalId : uniqueNodes) {
            Node node = tx.createNode(Label.label(NODE_LABEL));
            node.setProperty("id", externalId);
            // Record mapping: External ID -> Neo4j Internal ID
            nodeIdMap.put(externalId, node.getId());

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
        nodeCount = uniqueNodes.size();

        // Release Set memory for nodeIdMap and subsequent operations
        uniqueNodes = null;
        System.gc(); // Suggest GC, but not mandatory

        // ==========================================
        // Pass 3: Stream through file again to create edges
        // ==========================================
        System.out.println("Pass 2: Streaming edges...");
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath))) {
            String line;
            tx = db.beginTx();
            opsInTx = 0;
            RelationshipType relType = RelationshipType.withName("MyEdge");

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

                Long srcNeo4jId = nodeIdMap.get(srcId);
                Long dstNeo4jId = nodeIdMap.get(dstId);

                // Only create edge if both nodes exist
                if (srcNeo4jId != null && dstNeo4jId != null) {
                    Node srcNode = tx.getNodeById(srcNeo4jId);
                    Node dstNode = tx.getNodeById(dstNeo4jId);
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
            }
            tx.commit();
            tx.close();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

        System.out.println("Loaded " + nodeCount + " nodes and " + edgeCount + " edges");

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
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode(Label.label(NODE_LABEL));
                node.setProperty("id", id);
                nodeIdMap.put(id, node.getId());
                tx.commit();
            } catch (Exception e) {
                // Silent failure
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
            try (Transaction tx = db.beginTx()) {
                Long neo4jId = nodeIdMap.get(update.getId());
                if (neo4jId != null) {
                    Node node = tx.getNodeById(neo4jId);
                    for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                        node.setProperty(entry.getKey(), entry.getValue());
                    }
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
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
            try (Transaction tx = db.beginTx()) {
                Long neo4jId = nodeIdMap.get(id);
                if (neo4jId != null) {
                    Node node = tx.getNodeById(neo4jId);
                    // Delete all relationships first
                    for (Relationship rel : node.getRelationships()) {
                        rel.delete();
                    }
                    node.delete();
                    nodeIdMap.remove(id);
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> addEdge(String label, List<AddEdgeParams.EdgePair> pairs) {
        List<Double> latencies = new ArrayList<>();
        RelationshipType relType = RelationshipType.withName(label);

        for (AddEdgeParams.EdgePair pair : pairs) {
            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                Long srcNeo4jId = nodeIdMap.get(pair.getSrc());
                Long dstNeo4jId = nodeIdMap.get(pair.getDst());
                if (srcNeo4jId != null && dstNeo4jId != null) {
                    Node srcNode = tx.getNodeById(srcNeo4jId);
                    Node dstNode = tx.getNodeById(dstNeo4jId);
                    srcNode.createRelationshipTo(dstNode, relType);
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> upsertEdgeProperty(String label, List<UpsertEdgePropertyParams.EdgeUpdate> updates) {
        List<Double> latencies = new ArrayList<>();
        RelationshipType relType = RelationshipType.withName(label);

        for (UpsertEdgePropertyParams.EdgeUpdate update : updates) {
            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                Long srcNeo4jId = nodeIdMap.get(update.getSrc());
                Long dstNeo4jId = nodeIdMap.get(update.getDst());
                if (srcNeo4jId != null && dstNeo4jId != null) {
                    Node srcNode = tx.getNodeById(srcNeo4jId);
                    Node dstNode = tx.getNodeById(dstNeo4jId);

                    // Find the edge between src and dst
                    for (Relationship rel : srcNode.getRelationships(Direction.OUTGOING, relType)) {
                        if (rel.getEndNode().getId() == dstNeo4jId) {
                            for (Map.Entry<String, Object> entry : update.getProperties().entrySet()) {
                                rel.setProperty(entry.getKey(), entry.getValue());
                            }
                            break;
                        }
                    }
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> removeEdge(String label, List<RemoveEdgeParams.EdgePair> pairs) {
        List<Double> latencies = new ArrayList<>();
        RelationshipType relType = RelationshipType.withName(label);

        for (RemoveEdgeParams.EdgePair pair : pairs) {
            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                Long srcNeo4jId = nodeIdMap.get(pair.getSrc());
                Long dstNeo4jId = nodeIdMap.get(pair.getDst());
                if (srcNeo4jId != null && dstNeo4jId != null) {
                    Node srcNode = tx.getNodeById(srcNeo4jId);

                    // Find and delete the edge
                    for (Relationship rel : srcNode.getRelationships(Direction.OUTGOING, relType)) {
                        if (rel.getEndNode().getId() == dstNeo4jId) {
                            rel.delete();
                            break;
                        }
                    }
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> getNbrs(String direction, List<Long> ids) {
        List<Double> latencies = new ArrayList<>();
        Direction dir = Direction.valueOf(direction);

        for (Long id : ids) {
            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                Long neo4jId = nodeIdMap.get(id);
                if (neo4jId != null) {
                    Node node = tx.getNodeById(neo4jId);
                    List<Long> neighbors = new ArrayList<>();
                    for (Relationship rel : node.getRelationships(dir)) {
                        Node neighbor = rel.getOtherNode(node);
                        if (neighbor.hasProperty("id")) {
                            neighbors.add((Long) neighbor.getProperty("id"));
                        }
                    }
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
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
            try (Transaction tx = db.beginTx()) {
                List<Node> results = new ArrayList<>();
                for (Node node : tx.getAllNodes()) {
                    if (node.hasLabel(Label.label(NODE_LABEL)) && node.hasProperty(query.getKey())) {
                        Object value = node.getProperty(query.getKey());
                        if (value.equals(query.getValue())) {
                            results.add(node);
                        }
                    }
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
            }
            long endNs = System.nanoTime();
            latencies.add((endNs - startNs) / 1000.0);
        }

        return latencies;
    }

    @Override
    public List<Double> getEdgeByProperty(String label, List<GetEdgeByPropertyParams.PropertyQuery> queries) {
        List<Double> latencies = new ArrayList<>();
        RelationshipType relType = RelationshipType.withName(label);

        for (GetEdgeByPropertyParams.PropertyQuery query : queries) {
            long startNs = System.nanoTime();
            try (Transaction tx = db.beginTx()) {
                List<Relationship> results = new ArrayList<>();
                for (Node node : tx.getAllNodes()) {
                    for (Relationship rel : node.getRelationships(Direction.OUTGOING, relType)) {
                        if (rel.hasProperty(query.getKey())) {
                            Object value = rel.getProperty(query.getKey());
                            if (value.equals(query.getValue())) {
                                results.add(rel);
                            }
                        }
                    }
                }
                tx.commit();
            } catch (Exception e) {
                // Silent failure
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
