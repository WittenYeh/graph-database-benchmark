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

    /**
     * Get the database name for metadata (e.g., "neo4j", "janusgraph")
     */
    protected abstract String getDatabaseName();

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
        metadata.put("dataset", new File(datasetPath).getName());
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

            // Handle copy mode
            if (copyMode && originalGraphSnapshot != null) {
                restoreGraph(originalGraphSnapshot);
            }

            // Send task start callback
            sendProgressCallback(callbackUrl, "task_start", taskName, i, workloadFiles.length, workloadFile.getName(), 0, null);

            // Execute task
            Map<String, Object> result = executeTask(taskName, queries, clientThreads, datasetPath);
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
    protected Map<String, Object> calculateLatencyStats(List<Double> latencies) {
        if (latencies.isEmpty()) {
            return null;
        }

        Collections.sort(latencies);
        Map<String, Object> stats = new HashMap<>();

        stats.put("minUs", latencies.get(0));
        stats.put("maxUs", latencies.get(latencies.size() - 1));
        stats.put("meanUs", latencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0));
        stats.put("medianUs", getPercentile(latencies, 0.5));
        stats.put("p50Us", getPercentile(latencies, 0.5));
        stats.put("p90Us", getPercentile(latencies, 0.9));
        stats.put("p95Us", getPercentile(latencies, 0.95));
        stats.put("p99Us", getPercentile(latencies, 0.99));

        return stats;
    }

    private double getPercentile(List<Double> sortedList, double percentile) {
        int index = (int) Math.ceil(percentile * sortedList.size()) - 1;
        return sortedList.get(Math.max(0, index));
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
     * Execute queries concurrently using a thread pool.
     * Returns list of latencies in microseconds.
     */
    protected List<Double> executeConcurrent(List<String> queries, int clientThreads) throws InterruptedException {
        List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
        ExecutorService executor = Executors.newFixedThreadPool(clientThreads);
        CountDownLatch latch = new CountDownLatch(queries.size());

        for (String query : queries) {
            executor.submit(() -> {
                try {
                    long startNs = System.nanoTime();
                    executeQuery(query);
                    long endNs = System.nanoTime();
                    double latencyUs = (endNs - startNs) / 1000.0;
                    latencies.add(latencyUs);
                } catch (Exception e) {
                    // Silent failure as per specification
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        return latencies;
    }
}
