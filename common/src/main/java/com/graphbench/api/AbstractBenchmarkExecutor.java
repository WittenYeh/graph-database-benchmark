package com.graphbench.api;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * Abstract base class implementing common benchmark execution logic.
 * Database-specific implementations should extend this class.
 */
public abstract class AbstractBenchmarkExecutor implements GraphBenchmarkExecutor {

    protected static final Gson gson = new Gson();
    protected Random random = new Random();
    protected List<Object> graphNodeIds = new ArrayList<>();
    protected int graphEdgeCount = 0;

    /**
     * Get the database name for metadata (e.g., "neo4j", "janusgraph")
     */
    protected abstract String getDatabaseName();

    /**
     * Execute a batch of queries.
     * For transaction-free databases (JanusGraph): execute all queries without transaction.
     * For transactional databases (Neo4j): execute all queries in a single transaction.
     * Returns the total execution time in microseconds.
     */
    protected abstract double executeBatch(List<String> queries);

    /**
     * Load graph from dataset file.
     * Database-specific implementation for bulk loading.
     */
    protected abstract void loadGraph(String datasetPath) throws Exception;

    @Override
    public Map<String, Object> executeTask(String taskName, List<String> queries, int clientThreads, String datasetPath, int batchSize, String latencyTestMode) throws Exception {
        Map<String, Object> result = new HashMap<>();

        // Build task name with mode and batch size for latency tests
        String fullTaskName = taskName;
        if (taskName.contains("latency")) {
            if ("singleton".equals(latencyTestMode)) {
                fullTaskName = taskName + "_singleton";
            } else {
                fullTaskName = taskName + "_batch_" + batchSize;
            }
        }
        result.put("task", fullTaskName);

        long startTime = System.nanoTime();

        try {
            if ("load_graph".equals(taskName)) {
                loadGraph(datasetPath);
                result.put("status", "success");
                result.put("nodes", graphNodeIds.size());
                result.put("edges", graphEdgeCount);
                result.put("clientThreads", 0);
            } else if (taskName.contains("latency")) {
                // Latency test
                executeLatencyTest(queries, batchSize, latencyTestMode, result);
            } else if (taskName.contains("throughput")) {
                // Throughput test
                executeThroughputTest(queries, clientThreads, batchSize, result);
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

    /**
     * Execute latency test with queries.
     */
    protected void executeLatencyTest(List<String> queries, int batchSize, String latencyTestMode, Map<String, Object> result) {
        // JIT warm-up: run a few queries to warm up the JVM
        warmupJIT(queries, batchSize, latencyTestMode);

        List<Double> latencies;
        if ("singleton".equals(latencyTestMode)) {
            latencies = executeSingleton(queries);
            result.put("latencyTestMode", "singleton");
            result.put("batchSize", 1);
        } else {
            latencies = executeSerial(queries, batchSize);
            result.put("latencyTestMode", "batch");
            result.put("batchSize", batchSize);
        }

        result.put("status", "success");
        result.put("totalOps", queries.size());
        result.put("clientThreads", 1);
        result.put("latency", calculateLatencyStats(latencies));
    }

    /**
     * Warm up JIT compiler before actual benchmark.
     * Runs a small subset of queries to trigger JIT compilation.
     */
    protected void warmupJIT(List<String> queries, int batchSize, String latencyTestMode) {
        int warmupOps = Math.min(100, queries.size() / 10); // 10% of queries or 100, whichever is smaller
        if (warmupOps == 0) return;

        System.out.println("JIT warm-up: running " + warmupOps + " operations...");
        List<String> warmupQueries = queries.subList(0, warmupOps);

        try {
            if ("singleton".equals(latencyTestMode)) {
                for (String query : warmupQueries) {
                    executeQuery(query);
                }
            } else {
                // Warm up with batches
                for (int i = 0; i < warmupQueries.size(); i += batchSize) {
                    int end = Math.min(i + batchSize, warmupQueries.size());
                    executeBatch(warmupQueries.subList(i, end));
                }
            }
        } catch (Exception e) {
            // Ignore warm-up errors
        }
        System.out.println("JIT warm-up completed");
    }

    /**
     * Execute throughput test with queries.
     */
    protected void executeThroughputTest(List<String> queries, int clientThreads, int batchSize, Map<String, Object> result) throws InterruptedException {
        // JIT warm-up: run a few queries to warm up the JVM
        warmupJITThroughput(queries, clientThreads);

        double durationSeconds = executeConcurrent(queries, clientThreads);

        result.put("status", "success");
        result.put("totalOps", queries.size());
        result.put("clientThreads", clientThreads);
        result.put("batchSize", batchSize);
        result.put("throughputQps", queries.size() / durationSeconds);
    }

    /**
     * Warm up JIT compiler for throughput test.
     */
    protected void warmupJITThroughput(List<String> queries, int clientThreads) throws InterruptedException {
        int warmupOps = Math.min(100, queries.size() / 10);
        if (warmupOps == 0) return;

        System.out.println("JIT warm-up (throughput): running " + warmupOps + " operations...");
        List<String> warmupQueries = queries.subList(0, warmupOps);

        try {
            executeConcurrent(warmupQueries, clientThreads);
        } catch (Exception e) {
            // Ignore warm-up errors
        }
        System.out.println("JIT warm-up completed");
    }

    @Override
    public Map<String, Object> executeBenchmark(String datasetPath, int serverThreads, String callbackUrl) throws Exception {
        // Initialize database
        initDatabase();

        // Load workload files
        File workloadDir = new File("/data/workloads");
        File[] workloadFiles = workloadDir.listFiles((dir, name) -> name.endsWith(".json"));
        if (workloadFiles == null || workloadFiles.length == 0) {
            throw new RuntimeException("No workload files found");
        }
        Arrays.sort(workloadFiles);

        // Build metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("database", getDatabaseName());

        // Extract dataset name without extension
        String fileName = new File(datasetPath).getName();
        String datasetName = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
        metadata.put("dataset", datasetName);

        metadata.put("datasetPath", datasetPath);
        metadata.put("timestamp", Instant.now().toString());
        metadata.put("serverThreads", serverThreads);
        metadata.put("javaVersion", System.getProperty("java.version"));
        metadata.put("osName", System.getProperty("os.name"));
        metadata.put("osVersion", System.getProperty("os.version"));

        // Execute tasks
        List<Map<String, Object>> results = new ArrayList<>();
        byte[] originalGraphSnapshot = null;

        for (int i = 0; i < workloadFiles.length; i++) {
            File workloadFile = workloadFiles[i];
            System.out.println("Executing: " + workloadFile.getName());

            // Parse workload
            Map<String, Object> workload = gson.fromJson(
                new FileReader(workloadFile),
                new TypeToken<Map<String, Object>>(){}.getType()
            );

            String taskName = (String) workload.get("task_name");
            List<String> queries = (List<String>) workload.get("queries");
            Map<String, Object> taskMetadata = (Map<String, Object>) workload.get("metadata");
            boolean copyMode = (Boolean) taskMetadata.getOrDefault("copy_mode", false);
            int clientThreads = ((Double) taskMetadata.getOrDefault("client_threads", 1.0)).intValue();
            int batchSize = ((Double) taskMetadata.getOrDefault("batch_size", 128.0)).intValue();
            String latencyTestMode = (String) taskMetadata.getOrDefault("latency_test_mode", "batch");

            // Handle copy mode
            if (copyMode && originalGraphSnapshot != null) {
                restoreGraph(originalGraphSnapshot);
            }

            // Send task start callback
            sendProgressCallback(callbackUrl, "task_start", taskName, i, workloadFiles.length, workloadFile.getName(), 0, null);

            // Execute task
            Map<String, Object> result = executeTask(taskName, queries, clientThreads, datasetPath, batchSize, latencyTestMode);
            results.add(result);

            // Send task complete callback
            double duration = (Double) result.getOrDefault("durationSeconds", 0.0);
            String status = (String) result.getOrDefault("status", "unknown");
            sendProgressCallback(callbackUrl, "task_complete", taskName, i, workloadFiles.length, null, duration, status);

            // If load_graph failed, stop benchmark execution
            if ("load_graph".equals(taskName) && "failed".equals(status)) {
                System.err.println("❌ load_graph task failed, stopping benchmark execution");
                String error = (String) result.get("error");
                throw new RuntimeException("load_graph failed: " + error);
            }

            // Save snapshot after load_graph
            if ("load_graph".equals(taskName)) {
                originalGraphSnapshot = snapshotGraph();
            }
        }

        // Build final result
        Map<String, Object> finalResult = new HashMap<>();
        finalResult.put("metadata", metadata);
        finalResult.put("results", results);

        return finalResult;
    }

    /**
     * Calculate latency statistics from a list of latencies in microseconds.
     */
    protected Double calculateLatencyStats(List<Double> latencies) {
        if (latencies.isEmpty()) {
            return null;
        }

        return latencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    /**
     * Send progress callback to host.
     */
    protected void sendProgressCallback(String callbackUrl, String event, String taskName, int taskIndex,
                                       int totalTasks, String workloadFile, double durationSeconds, String status) {
        if (callbackUrl == null || callbackUrl.isEmpty()) {
            return;
        }

        try {
            java.net.URL url = new java.net.URL(callbackUrl);
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);

            Map<String, Object> payload = new HashMap<>();
            payload.put("event", event);
            payload.put("task_name", taskName);
            payload.put("task_index", taskIndex);
            payload.put("total_tasks", totalTasks);
            payload.put("timestamp", System.currentTimeMillis());

            if ("task_start".equals(event)) {
                payload.put("workload_file", workloadFile);
            } else if ("task_complete".equals(event)) {
                payload.put("duration_seconds", durationSeconds);
                payload.put("status", status);
            }

            String jsonPayload = gson.toJson(payload);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.println("⚠️  Callback failed with status: " + responseCode);
            }

            conn.disconnect();
        } catch (Exception e) {
            System.err.println("⚠️  Failed to send callback: " + e.getMessage());
        }
    }

    /**
     * Execute queries serially for latency testing.
     * Splits queries into batches and executes each batch serially.
     * Each batch calls executeBatch to execute all queries in the batch.
     * Returns list of per-operation latencies in microseconds.
     */
    protected List<Double> executeSerial(List<String> queries, int batchSize) {
        List<Double> latencies = new ArrayList<>();

        for (int i = 0; i < queries.size(); i += batchSize) {
            int end = Math.min(i + batchSize, queries.size());
            List<String> batch = queries.subList(i, end);

            try {
                double batchLatencyUs = executeBatch(batch);
                // Calculate per-operation latency for this batch
                double perOpLatencyUs = batchLatencyUs / batch.size();
                // Add per-operation latency for each query in the batch
                for (int j = 0; j < batch.size(); j++) {
                    latencies.add(perOpLatencyUs);
                }
            } catch (Exception e) {
                // Silent failure as per specification
            }
        }

        return latencies;
    }

    /**
     * Execute queries serially in singleton mode for latency testing.
     * Each query is executed individually with its own transaction.
     * Returns list of per-operation latencies in microseconds.
     */
    protected List<Double> executeSingleton(List<String> queries) {
        List<Double> latencies = new ArrayList<>();

        for (String query : queries) {
            try {
                long startNs = System.nanoTime();
                executeQuery(query);
                long endNs = System.nanoTime();
                double latencyUs = (endNs - startNs) / 1000.0;
                latencies.add(latencyUs);
            } catch (Exception e) {
                // Silent failure as per specification
            }
        }

        return latencies;
    }

    /**
     * Execute queries concurrently for throughput testing.
     * Splits queries into batches and assigns each batch to a thread.
     * Each thread calls executeBatch to execute its assigned batch.
     * Returns total execution time in seconds.
     */
    protected double executeConcurrent(List<String> queries, int clientThreads) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(clientThreads);

        // Split queries into batches for each thread
        List<List<String>> batches = new ArrayList<>();
        int batchSize = (int) Math.ceil((double) queries.size() / clientThreads);

        for (int i = 0; i < queries.size(); i += batchSize) {
            int end = Math.min(i + batchSize, queries.size());
            batches.add(queries.subList(i, end));
        }

        CountDownLatch latch = new CountDownLatch(batches.size());

        long startNs = System.nanoTime();

        for (List<String> batch : batches) {
            executor.submit(() -> {
                try {
                    executeBatch(batch);
                } catch (Exception e) {
                    // Silent failure as per specification
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endNs = System.nanoTime();
        executor.shutdown();

        return (endNs - startNs) / 1_000_000_000.0;
    }

    @Override
    public byte[] snapshotGraph() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(new ArrayList<>(graphNodeIds));
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
            graphNodeIds = (List<Object>) ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
