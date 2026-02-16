package com.graphbench.api;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.graphbench.workload.*;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/**
 * WorkloadDispatcher reads workload JSON files and dispatches them to the executor.
 */
public class WorkloadDispatcher {
    private static final Gson gson = new Gson();

    private final BenchmarkExecutor executor;
    private final String datasetPath;

    /**
     * Create a dispatcher for benchmarks.
     */
    public WorkloadDispatcher(BenchmarkExecutor executor, String datasetPath) {
        this.executor = executor;
        this.datasetPath = datasetPath;
    }

    /**
     * Execute all workload files in the specified directory.
     * @param workloadDir Directory containing workload JSON files
     * @return Benchmark results
     */
    public Map<String, Object> executeBenchmark(String workloadDir) throws Exception {
        // Initialize database
        executor.initDatabase();

        // Load workload files
        File dir = new File(workloadDir);
        File[] workloadFiles = dir.listFiles((d, name) -> name.endsWith(".json"));
        if (workloadFiles == null || workloadFiles.length == 0) {
            throw new RuntimeException("No workload files found in: " + workloadDir);
        }
        Arrays.sort(workloadFiles);

        // Build metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("database", executor.getDatabaseName());

        String fileName = new File(datasetPath).getName();
        String datasetName = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
        metadata.put("dataset", datasetName);
        metadata.put("datasetPath", datasetPath);
        metadata.put("timestamp", Instant.now().toString());
        metadata.put("javaVersion", System.getProperty("java.version"));
        metadata.put("osName", System.getProperty("os.name"));
        metadata.put("osVersion", System.getProperty("os.version"));

        // Execute tasks
        List<Map<String, Object>> results = new ArrayList<>();

        for (File workloadFile : workloadFiles) {
            System.out.println("Executing: " + workloadFile.getName());

            Map<String, Object> result = executeWorkloadFile(workloadFile);
            results.add(result);

            // If load_graph failed, stop execution
            String taskType = (String) result.get("task_type");
            String status = (String) result.get("status");
            if ("LOAD_GRAPH".equals(taskType) && "failed".equals(status)) {
                System.err.println("❌ LOAD_GRAPH task failed, stopping benchmark execution");
                throw new RuntimeException("LOAD_GRAPH failed: " + result.get("error"));
            }
        }

        // Shutdown database
        executor.shutdown();

        // Build final result
        Map<String, Object> finalResult = new HashMap<>();
        finalResult.put("metadata", metadata);
        finalResult.put("results", results);

        return finalResult;
    }

    /**
     * Execute a single workload file.
     */
    private Map<String, Object> executeWorkloadFile(File workloadFile) {
        Map<String, Object> result = new HashMap<>();
        long startTime = System.nanoTime();

        try {
            // Parse workload file
            WorkloadTask task = gson.fromJson(new FileReader(workloadFile), WorkloadTask.class);
            String taskType = task.getTaskType();
            Map<String, Object> parameters = task.getParameters();

            result.put("task_type", taskType);
            result.put("ops_count", task.getOpsCount());

            // Dispatch to appropriate executor method
            if ("LOAD_GRAPH".equals(taskType)) {
                Map<String, Object> loadResult = executor.loadGraph(datasetPath);
                result.putAll(loadResult);
                result.put("status", "success");
            } else {
                executeTask(taskType, parameters, result);
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
     * Execute a task.
     */
    private void executeTask(String taskType, Map<String, Object> parameters, Map<String, Object> result) {
        List<Double> latencies = null;

        switch (taskType) {
            case "ADD_VERTEX":
                List<Long> addVertexIds = parseIdList(parameters.get("ids"));
                latencies = executor.addVertex(addVertexIds);
                break;

            case "UPSERT_VERTEX_PROPERTY":
                List<UpsertVertexPropertyParams.VertexUpdate> vertexUpdates =
                    parseVertexUpdates((List<Map<String, Object>>) parameters.get("updates"));
                latencies = executor.upsertVertexProperty(vertexUpdates);
                break;

            case "REMOVE_VERTEX":
                List<Long> removeVertexIds = parseIdList(parameters.get("ids"));
                latencies = executor.removeVertex(removeVertexIds);
                break;

            case "ADD_EDGE":
                String addEdgeLabel = (String) parameters.get("label");
                List<AddEdgeParams.EdgePair> addEdgePairs =
                    parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                latencies = executor.addEdge(addEdgeLabel, addEdgePairs);
                break;

            case "UPSERT_EDGE_PROPERTY":
                String upsertEdgeLabel = (String) parameters.get("label");
                List<UpsertEdgePropertyParams.EdgeUpdate> edgeUpdates =
                    parseEdgeUpdates((List<Map<String, Object>>) parameters.get("updates"));
                latencies = executor.upsertEdgeProperty(upsertEdgeLabel, edgeUpdates);
                break;

            case "REMOVE_EDGE":
                String removeEdgeLabel = (String) parameters.get("label");
                List<RemoveEdgeParams.EdgePair> removeEdgePairs =
                    parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                latencies = executor.removeEdge(removeEdgeLabel, removeEdgePairs);
                break;

            case "GET_NBRS":
                String direction = (String) parameters.get("direction");
                List<Long> nbrsIds = parseIdList(parameters.get("ids"));
                latencies = executor.getNbrs(direction, nbrsIds);
                break;

            case "GET_VERTEX_BY_PROPERTY":
                List<GetVertexByPropertyParams.PropertyQuery> vertexQueries =
                    parsePropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                latencies = executor.getVertexByProperty(vertexQueries);
                break;

            case "GET_EDGE_BY_PROPERTY":
                String getEdgeLabel = (String) parameters.get("label");
                List<GetEdgeByPropertyParams.PropertyQuery> edgeQueries =
                    parsePropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                latencies = executor.getEdgeByProperty(getEdgeLabel, edgeQueries);
                break;

            default:
                throw new RuntimeException("Unknown task type: " + taskType);
        }

        result.put("status", "success");
        result.put("latency", calculateLatencyStats(latencies));
    }

    // Helper methods for parsing parameters

    private List<Long> parseIdList(Object obj) {
        List<Long> ids = new ArrayList<>();
        if (obj instanceof List) {
            for (Object item : (List<?>) obj) {
                if (item instanceof Number) {
                    ids.add(((Number) item).longValue());
                }
            }
        }
        return ids;
    }

    private List<UpsertVertexPropertyParams.VertexUpdate> parseVertexUpdates(List<Map<String, Object>> updates) {
        List<UpsertVertexPropertyParams.VertexUpdate> result = new ArrayList<>();
        for (Map<String, Object> update : updates) {
            UpsertVertexPropertyParams.VertexUpdate vu = new UpsertVertexPropertyParams.VertexUpdate();
            vu.setId(((Number) update.get("id")).longValue());
            vu.setProperties((Map<String, Object>) update.get("properties"));
            result.add(vu);
        }
        return result;
    }

    private List<AddEdgeParams.EdgePair> parseEdgePairs(List<Map<String, Object>> pairs) {
        List<AddEdgeParams.EdgePair> result = new ArrayList<>();
        for (Map<String, Object> pair : pairs) {
            AddEdgeParams.EdgePair ep = new AddEdgeParams.EdgePair();
            ep.setSrc(((Number) pair.get("src")).longValue());
            ep.setDst(((Number) pair.get("dst")).longValue());
            result.add(ep);
        }
        return result;
    }

    private List<UpsertEdgePropertyParams.EdgeUpdate> parseEdgeUpdates(List<Map<String, Object>> updates) {
        List<UpsertEdgePropertyParams.EdgeUpdate> result = new ArrayList<>();
        for (Map<String, Object> update : updates) {
            UpsertEdgePropertyParams.EdgeUpdate eu = new UpsertEdgePropertyParams.EdgeUpdate();
            eu.setSrc(((Number) update.get("src")).longValue());
            eu.setDst(((Number) update.get("dst")).longValue());
            eu.setProperties((Map<String, Object>) update.get("properties"));
            result.add(eu);
        }
        return result;
    }

    private <T> List<T> parsePropertyQueries(List<Map<String, Object>> queries) {
        List<T> result = new ArrayList<>();
        for (Map<String, Object> query : queries) {
            // Create a generic property query (works for both vertex and edge)
            GetVertexByPropertyParams.PropertyQuery pq = new GetVertexByPropertyParams.PropertyQuery();
            pq.setKey((String) query.get("key"));
            pq.setValue(query.get("value"));
            result.add((T) pq);
        }
        return result;
    }

    private Double calculateLatencyStats(List<Double> latencies) {
        if (latencies == null || latencies.isEmpty()) {
            return null;
        }
        return latencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
}


    /**
     * Execute all workload files in the specified directory.
     * @param workloadDir Directory containing workload JSON files
     * @return Benchmark results
     */
    public Map<String, Object> executeBenchmark(String workloadDir) throws Exception {
        // Initialize database
        executor.initDatabase();

        // Load workload files
        File dir = new File(workloadDir);
        File[] workloadFiles = dir.listFiles((d, name) -> name.endsWith(".json"));
        if (workloadFiles == null || workloadFiles.length == 0) {
            throw new RuntimeException("No workload files found in: " + workloadDir);
        }
        Arrays.sort(workloadFiles);

        // Build metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("database", mode.equals("latency") ?
            latencyExecutor.getDatabaseName() : throughputExecutor.getDatabaseName());

        String fileName = new File(datasetPath).getName();
        String datasetName = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
        metadata.put("dataset", datasetName);
        metadata.put("datasetPath", datasetPath);
        metadata.put("timestamp", Instant.now().toString());
        metadata.put("mode", mode);
        metadata.put("clientThreads", clientThreads);
        metadata.put("javaVersion", System.getProperty("java.version"));
        metadata.put("osName", System.getProperty("os.name"));
        metadata.put("osVersion", System.getProperty("os.version"));

        // Execute tasks
        List<Map<String, Object>> results = new ArrayList<>();

        for (File workloadFile : workloadFiles) {
            System.out.println("Executing: " + workloadFile.getName());

            Map<String, Object> result = executeWorkloadFile(workloadFile);
            results.add(result);

            // If load_graph failed, stop execution
            String taskType = (String) result.get("task_type");
            String status = (String) result.get("status");
            if ("LOAD_GRAPH".equals(taskType) && "failed".equals(status)) {
                System.err.println("❌ LOAD_GRAPH task failed, stopping benchmark execution");
                throw new RuntimeException("LOAD_GRAPH failed: " + result.get("error"));
            }
        }

        // Shutdown database
        if (mode.equals("latency")) {
            latencyExecutor.shutdown();
        } else {
            throughputExecutor.shutdown();
        }

        // Build final result
        Map<String, Object> finalResult = new HashMap<>();
        finalResult.put("metadata", metadata);
        finalResult.put("results", results);

        return finalResult;
    }

    /**
     * Execute a single workload file.
     */
    private Map<String, Object> executeWorkloadFile(File workloadFile) {
        Map<String, Object> result = new HashMap<>();
        long startTime = System.nanoTime();

        try {
            // Parse workload file
            WorkloadTask task = gson.fromJson(new FileReader(workloadFile), WorkloadTask.class);
            String taskType = task.getTaskType();
            Map<String, Object> parameters = task.getParameters();

            result.put("task_type", taskType);
            result.put("ops_count", task.getOpsCount());

            // Dispatch to appropriate executor method
            if ("LOAD_GRAPH".equals(taskType)) {
                Map<String, Object> loadResult = mode.equals("latency") ?
                    latencyExecutor.loadGraph(datasetPath) : throughputExecutor.loadGraph(datasetPath);
                result.putAll(loadResult);
                result.put("status", "success");
            } else if (mode.equals("latency")) {
                executeLatencyTask(taskType, parameters, result);
            } else {
                executeThroughputTask(taskType, parameters, result);
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
     * Execute a latency task.
     */
    private void executeLatencyTask(String taskType, Map<String, Object> parameters, Map<String, Object> result) {
        List<Double> latencies = null;

        switch (taskType) {
            case "ADD_VERTEX":
                List<Long> addVertexIds = parseIdList(parameters.get("ids"));
                latencies = latencyExecutor.addVertex(addVertexIds);
                break;

            case "UPSERT_VERTEX_PROPERTY":
                List<UpsertVertexPropertyParams.VertexUpdate> vertexUpdates =
                    parseVertexUpdates((List<Map<String, Object>>) parameters.get("updates"));
                latencies = latencyExecutor.upsertVertexProperty(vertexUpdates);
                break;

            case "REMOVE_VERTEX":
                List<Long> removeVertexIds = parseIdList(parameters.get("ids"));
                latencies = latencyExecutor.removeVertex(removeVertexIds);
                break;

            case "ADD_EDGE":
                String addEdgeLabel = (String) parameters.get("label");
                List<AddEdgeParams.EdgePair> addEdgePairs =
                    parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                latencies = latencyExecutor.addEdge(addEdgeLabel, addEdgePairs);
                break;

            case "UPSERT_EDGE_PROPERTY":
                String upsertEdgeLabel = (String) parameters.get("label");
                List<UpsertEdgePropertyParams.EdgeUpdate> edgeUpdates =
                    parseEdgeUpdates((List<Map<String, Object>>) parameters.get("updates"));
                latencies = latencyExecutor.upsertEdgeProperty(upsertEdgeLabel, edgeUpdates);
                break;

            case "REMOVE_EDGE":
                String removeEdgeLabel = (String) parameters.get("label");
                List<RemoveEdgeParams.EdgePair> removeEdgePairs =
                    parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                latencies = latencyExecutor.removeEdge(removeEdgeLabel, removeEdgePairs);
                break;

            case "GET_NBRS":
                String direction = (String) parameters.get("direction");
                List<Long> nbrsIds = parseIdList(parameters.get("ids"));
                latencies = latencyExecutor.getNbrs(direction, nbrsIds);
                break;

            case "GET_VERTEX_BY_PROPERTY":
                List<GetVertexByPropertyParams.PropertyQuery> vertexQueries =
                    parsePropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                latencies = latencyExecutor.getVertexByProperty(vertexQueries);
                break;

            case "GET_EDGE_BY_PROPERTY":
                String getEdgeLabel = (String) parameters.get("label");
                List<GetEdgeByPropertyParams.PropertyQuery> edgeQueries =
                    parsePropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                latencies = latencyExecutor.getEdgeByProperty(getEdgeLabel, edgeQueries);
                break;

            default:
                throw new RuntimeException("Unknown task type: " + taskType);
        }

        result.put("status", "success");
        result.put("latency", calculateLatencyStats(latencies));
    }

    /**
     * Execute a throughput task.
     */
    private void executeThroughputTask(String taskType, Map<String, Object> parameters, Map<String, Object> result) throws InterruptedException {
        double durationSeconds = 0;
        int opsCount = 0;

        switch (taskType) {
            case "ADD_VERTEX":
                List<Long> addVertexIds = parseIdList(parameters.get("ids"));
                opsCount = addVertexIds.size();
                durationSeconds = throughputExecutor.addVertex(addVertexIds, clientThreads);
                break;

            case "UPSERT_VERTEX_PROPERTY":
                List<UpsertVertexPropertyParams.VertexUpdate> vertexUpdates =
                    parseVertexUpdates((List<Map<String, Object>>) parameters.get("updates"));
                opsCount = vertexUpdates.size();
                durationSeconds = throughputExecutor.upsertVertexProperty(vertexUpdates, clientThreads);
                break;

            case "REMOVE_VERTEX":
                List<Long> removeVertexIds = parseIdList(parameters.get("ids"));
                opsCount = removeVertexIds.size();
                durationSeconds = throughputExecutor.removeVertex(removeVertexIds, clientThreads);
                break;

            case "ADD_EDGE":
                String addEdgeLabel = (String) parameters.get("label");
                List<AddEdgeParams.EdgePair> addEdgePairs =
                    parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                opsCount = addEdgePairs.size();
                durationSeconds = throughputExecutor.addEdge(addEdgeLabel, addEdgePairs, clientThreads);
                break;

            case "UPSERT_EDGE_PROPERTY":
                String upsertEdgeLabel = (String) parameters.get("label");
                List<UpsertEdgePropertyParams.EdgeUpdate> edgeUpdates =
                    parseEdgeUpdates((List<Map<String, Object>>) parameters.get("updates"));
                opsCount = edgeUpdates.size();
                durationSeconds = throughputExecutor.upsertEdgeProperty(upsertEdgeLabel, edgeUpdates, clientThreads);
                break;

            case "REMOVE_EDGE":
                String removeEdgeLabel = (String) parameters.get("label");
                List<RemoveEdgeParams.EdgePair> removeEdgePairs =
                    parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                opsCount = removeEdgePairs.size();
                durationSeconds = throughputExecutor.removeEdge(removeEdgeLabel, removeEdgePairs, clientThreads);
                break;

            case "GET_NBRS":
                String direction = (String) parameters.get("direction");
                List<Long> nbrsIds = parseIdList(parameters.get("ids"));
                opsCount = nbrsIds.size();
                durationSeconds = throughputExecutor.getNbrs(direction, nbrsIds, clientThreads);
                break;

            case "GET_VERTEX_BY_PROPERTY":
                List<GetVertexByPropertyParams.PropertyQuery> vertexQueries =
                    parsePropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                opsCount = vertexQueries.size();
                durationSeconds = throughputExecutor.getVertexByProperty(vertexQueries, clientThreads);
                break;

            case "GET_EDGE_BY_PROPERTY":
                String getEdgeLabel = (String) parameters.get("label");
                List<GetEdgeByPropertyParams.PropertyQuery> edgeQueries =
                    parsePropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                opsCount = edgeQueries.size();
                durationSeconds = throughputExecutor.getEdgeByProperty(getEdgeLabel, edgeQueries, clientThreads);
                break;

            default:
                throw new RuntimeException("Unknown task type: " + taskType);
        }

        result.put("status", "success");
        result.put("throughputQps", opsCount / durationSeconds);
    }

    // Helper methods for parsing parameters

    private List<Long> parseIdList(Object obj) {
        List<Long> ids = new ArrayList<>();
        if (obj instanceof List) {
            for (Object item : (List<?>) obj) {
                if (item instanceof Number) {
                    ids.add(((Number) item).longValue());
                }
            }
        }
        return ids;
    }

    private List<UpsertVertexPropertyParams.VertexUpdate> parseVertexUpdates(List<Map<String, Object>> updates) {
        List<UpsertVertexPropertyParams.VertexUpdate> result = new ArrayList<>();
        for (Map<String, Object> update : updates) {
            UpsertVertexPropertyParams.VertexUpdate vu = new UpsertVertexPropertyParams.VertexUpdate();
            vu.setId(((Number) update.get("id")).longValue());
            vu.setProperties((Map<String, Object>) update.get("properties"));
            result.add(vu);
        }
        return result;
    }

    private List<AddEdgeParams.EdgePair> parseEdgePairs(List<Map<String, Object>> pairs) {
        List<AddEdgeParams.EdgePair> result = new ArrayList<>();
        for (Map<String, Object> pair : pairs) {
            AddEdgeParams.EdgePair ep = new AddEdgeParams.EdgePair();
            ep.setSrc(((Number) pair.get("src")).longValue());
            ep.setDst(((Number) pair.get("dst")).longValue());
            result.add(ep);
        }
        return result;
    }

    private List<UpsertEdgePropertyParams.EdgeUpdate> parseEdgeUpdates(List<Map<String, Object>> updates) {
        List<UpsertEdgePropertyParams.EdgeUpdate> result = new ArrayList<>();
        for (Map<String, Object> update : updates) {
            UpsertEdgePropertyParams.EdgeUpdate eu = new UpsertEdgePropertyParams.EdgeUpdate();
            eu.setSrc(((Number) update.get("src")).longValue());
            eu.setDst(((Number) update.get("dst")).longValue());
            eu.setProperties((Map<String, Object>) update.get("properties"));
            result.add(eu);
        }
        return result;
    }

    private <T> List<T> parsePropertyQueries(List<Map<String, Object>> queries) {
        List<T> result = new ArrayList<>();
        for (Map<String, Object> query : queries) {
            // Create a generic property query (works for both vertex and edge)
            GetVertexByPropertyParams.PropertyQuery pq = new GetVertexByPropertyParams.PropertyQuery();
            pq.setKey((String) query.get("key"));
            pq.setValue(query.get("value"));
            result.add((T) pq);
        }
        return result;
    }

    private Double calculateLatencyStats(List<Double> latencies) {
        if (latencies == null || latencies.isEmpty()) {
            return null;
        }
        return latencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
}
