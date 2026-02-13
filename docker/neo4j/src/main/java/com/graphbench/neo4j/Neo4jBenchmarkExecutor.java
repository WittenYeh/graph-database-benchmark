package com.graphbench.neo4j;

import com.graphbench.api.AbstractBenchmarkExecutor;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Neo4jBenchmarkExecutor extends AbstractBenchmarkExecutor {
    private static final String DB_PATH = "/tmp/neo4j-benchmark-db";
    private static final String DEFAULT_DB = "neo4j";

    private DatabaseManagementService managementService;
    private GraphDatabaseService db;
    private List<Long> nodeIds = new ArrayList<>();

    @Override
    protected String getDatabaseName() {
        return "neo4j";
    }

    @Override
    public void initDatabase() {
        // Clean up old database
        try {
            if (Files.exists(Paths.get(DB_PATH))) {
                deleteDirectory(new File(DB_PATH));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create new database with minimal configuration
        managementService = new DatabaseManagementServiceBuilder(Paths.get(DB_PATH))
            .build();
        db = managementService.database(DEFAULT_DB);

        System.out.println("Database initialized");
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
                result.put("totalOps", nodeIds.size());
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
        nodeIds.clear();

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

            // Create nodes
            Map<Long, Long> nodeIdMap = new HashMap<>();
            try (Transaction tx = db.beginTx()) {
                for (Long nodeId : uniqueNodes) {
                    Node node = tx.createNode(Label.label("MyNode"));
                    node.setProperty("id", nodeId);
                    nodeIdMap.put(nodeId, node.getId());
                }
                tx.commit();
            }

            nodeIds.addAll(nodeIdMap.values());

            // Create edges in batches
            int batchSize = 10000;
            for (int i = 0; i < edges.size(); i += batchSize) {
                try (Transaction tx = db.beginTx()) {
                    int end = Math.min(i + batchSize, edges.size());
                    for (int j = i; j < end; j++) {
                        long[] edge = edges.get(j);
                        Long srcNeo4jId = nodeIdMap.get(edge[0]);
                        Long dstNeo4jId = nodeIdMap.get(edge[1]);
                        if (srcNeo4jId != null && dstNeo4jId != null) {
                            Node srcNode = tx.getNodeById(srcNeo4jId);
                            Node dstNode = tx.getNodeById(dstNeo4jId);
                            srcNode.createRelationshipTo(dstNode, RelationshipType.withName("MyEdge"));
                        }
                    }
                    tx.commit();
                }
            }

            System.out.println("Loaded " + uniqueNodes.size() + " nodes and " + edges.size() + " edges");
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
        // Query is fully determined by host, no placeholder replacement needed
        try (Transaction tx = db.beginTx()) {
            tx.execute(query).resultAsString();
            tx.commit();
        }
    }

    @Override
    public byte[] snapshotGraph() {
        // Simple snapshot: just save node IDs
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(new ArrayList<>(nodeIds));
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void restoreGraph(byte[] snapshot) {
        // Restore by clearing and reloading
        try (ByteArrayInputStream bais = new ByteArrayInputStream(snapshot);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            nodeIds = (List<Long>) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown() {
        if (managementService != null) {
            managementService.shutdown();
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
