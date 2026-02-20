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
    private final ProgressCallback progressCallback;

    /**
     * Create a dispatcher for benchmarks.
     */
    public WorkloadDispatcher(BenchmarkExecutor executor, String datasetPath) {
        this(executor, datasetPath, System.getenv("PROGRESS_CALLBACK_URL"));
    }

    /**
     * Create a dispatcher with explicit progress callback URL.
     */
    public WorkloadDispatcher(BenchmarkExecutor executor, String datasetPath, String progressCallbackUrl) {
        this.executor = executor;
        this.datasetPath = datasetPath;
        this.progressCallback = new ProgressCallback(progressCallbackUrl);
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

        // datasetPath may be a directory (CSV mode) or file; use directory/file name as dataset name
        String datasetName = new File(datasetPath).getName();
        if (datasetName.contains(".")) {
            datasetName = datasetName.substring(0, datasetName.lastIndexOf('.'));
        }
        metadata.put("dataset", datasetName);
        metadata.put("datasetPath", datasetPath);
        metadata.put("timestamp", Instant.now().toString());
        metadata.put("javaVersion", System.getProperty("java.version"));
        metadata.put("osName", System.getProperty("os.name"));
        metadata.put("osVersion", System.getProperty("os.version"));

        // Execute tasks
        List<Map<String, Object>> results = new ArrayList<>();

        for (int i = 0; i < workloadFiles.length; i++) {
            File workloadFile = workloadFiles[i];
            System.out.println("Executing: " + workloadFile.getName());

            Map<String, Object> result = executeWorkloadFile(workloadFile, i, workloadFiles.length);
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

        // Cleanup database files with callback
        progressCallback.sendProgressCallback("cleanup_start", "CLEANUP", null, null, null, 0, 0);
        try {
            BenchmarkUtils.cleanupDatabaseFiles(executor.getDatabasePath(), executor.getSnapshotPath());
            progressCallback.sendProgressCallback("cleanup_complete", "CLEANUP", null, "success", null, 0, 0);
        } catch (Exception e) {
            progressCallback.sendProgressCallback("cleanup_complete", "CLEANUP", null, "failed", null, 0, 0);
            progressCallback.sendErrorMessage("Failed to cleanup database files: " + e.getMessage(), "CleanupError");
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
    private Map<String, Object> executeWorkloadFile(File workloadFile, int taskIndex, int totalTasks) {
        Map<String, Object> result = new HashMap<>();
        long startTime = System.nanoTime();

        try {
            // Parse workload file
            WorkloadTask task = gson.fromJson(new FileReader(workloadFile), WorkloadTask.class);
            String taskType = task.getTaskType();
            Map<String, Object> parameters = task.getParameters();

            result.put("task_type", taskType);
            result.put("ops_count", task.getOpsCount());

            // Send task start callback
            progressCallback.sendProgressCallback("task_start", taskType, workloadFile.getName(), null, null, taskIndex, totalTasks);

            // Dispatch to appropriate executor method
            if ("LOAD_GRAPH".equals(taskType)) {
                Map<String, Object> loadResult = executor.loadGraph(datasetPath);
                result.putAll(loadResult);
                result.put("status", "success");

                // Create snapshot after loading graph
                progressCallback.sendProgressCallback("snapshot_start", "SNAPSHOT", null, null, null, taskIndex, totalTasks);
                executor.snapGraph();
                progressCallback.sendProgressCallback("snapshot_complete", "SNAPSHOT", null, "success", null, taskIndex, totalTasks);
            } else {
                // Get batch_sizes, default to [1] if not specified
                List<Integer> batchSizes = task.getBatchSizes();
                if (batchSizes == null || batchSizes.isEmpty()) {
                    batchSizes = Arrays.asList(1);
                }

                // Execute task for each batch size
                List<Map<String, Object>> batchResults = new ArrayList<>();

                for (int batchSize : batchSizes) {
                    // Restore graph to clean state before executing workload
                    progressCallback.sendProgressCallback("restore_start", "RESTORE", null, null, null, taskIndex, totalTasks);
                    executor.restoreGraph();
                    progressCallback.sendProgressCallback("restore_complete", "RESTORE", null, "success", null, taskIndex, totalTasks);

                    // Send subtask start callback
                    String subtaskName = taskType + " (batch_size=" + batchSize + ")";
                    progressCallback.sendProgressCallback("subtask_start", subtaskName, null, null, null, taskIndex, totalTasks);

                    Map<String, Object> subtaskResult = new HashMap<>();
                    long taskStartTime = System.nanoTime();
                    executeTask(taskType, parameters, subtaskResult, batchSize);
                    long taskEndTime = System.nanoTime();
                    double taskDuration = (taskEndTime - taskStartTime) / 1_000_000_000.0;

                    // Send subtask complete callback with operation counts
                    Integer originalOpsCount = (Integer) subtaskResult.get("originalOpsCount");
                    Integer validOpsCount = (Integer) subtaskResult.get("validOpsCount");
                    Integer filteredOpsCount = (Integer) subtaskResult.get("filteredOpsCount");
                    progressCallback.sendProgressCallback("subtask_complete", subtaskName, null, "success", taskDuration,
                                                         taskIndex, totalTasks, originalOpsCount, validOpsCount, filteredOpsCount);

                    batchResults.add(subtaskResult);
                }

                result.put("batch_results", batchResults);
                result.put("status", "success");
            }

        } catch (Exception e) {
            result.put("status", "failed");
            result.put("error", e.getMessage());

            // Send error message to host
            progressCallback.sendErrorMessage(e.getMessage(), e.getClass().getSimpleName());

            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;
        result.put("durationSeconds", durationSeconds);

        // Send task complete callback
        String status = (String) result.getOrDefault("status", "failed");
        String taskType = (String) result.get("task_type");
        progressCallback.sendProgressCallback("task_complete", taskType, null, status, durationSeconds, taskIndex, totalTasks);

        return result;
    }

    /**
     * Get the executor as a PropertyBenchmarkExecutor, or throw if not supported.
     */
    private PropertyBenchmarkExecutor requirePropertyExecutor() {
        if (!(executor instanceof PropertyBenchmarkExecutor)) {
            throw new RuntimeException(
                "Property operations require a PropertyBenchmarkExecutor, but got: "
                + executor.getClass().getName());
        }
        return (PropertyBenchmarkExecutor) executor;
    }

    /**
     * Execute a task with specified batch size.
     */
    private void executeTask(String taskType, Map<String, Object> parameters, Map<String, Object> result, int batchSize) {
        List<Double> latencies = null;
        int errorsBefore = executor.getErrorCount();
        int originalOpsCount = 0;
        int validOpsCount = 0;

        switch (taskType) {
            case "ADD_VERTEX":
                int count = ((Number) parameters.get("count")).intValue();
                originalOpsCount = count;
                validOpsCount = count;
                latencies = executor.addVertex(count, batchSize);
                break;

            case "REMOVE_VERTEX":
                List<Object> removeVertexSystemIds = parseIdList(parameters.get("ids"));
                originalOpsCount = ((List<?>) parameters.get("ids")).size();
                validOpsCount = removeVertexSystemIds.size();
                latencies = executor.removeVertex(removeVertexSystemIds, batchSize);
                break;

            case "ADD_EDGE":
                String addEdgeLabel = (String) parameters.get("label");
                List<AddEdgeParams.EdgePair> addEdgePairs =
                    parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                originalOpsCount = ((List<?>) parameters.get("pairs")).size();
                validOpsCount = addEdgePairs.size();
                latencies = executor.addEdge(addEdgeLabel, addEdgePairs, batchSize);
                break;

            case "REMOVE_EDGE":
                String removeEdgeLabel = (String) parameters.get("label");
                List<RemoveEdgeParams.EdgePair> removeEdgePairs =
                    parseRemoveEdgePairs((List<Map<String, Object>>) parameters.get("pairs"));
                originalOpsCount = ((List<?>) parameters.get("pairs")).size();
                validOpsCount = removeEdgePairs.size();
                latencies = executor.removeEdge(removeEdgeLabel, removeEdgePairs, batchSize);
                break;

            case "GET_NBRS":
                String direction = (String) parameters.get("direction");
                List<Object> nbrsSystemIds = parseIdList(parameters.get("ids"));
                originalOpsCount = ((List<?>) parameters.get("ids")).size();
                validOpsCount = nbrsSystemIds.size();
                latencies = executor.getNbrs(direction, nbrsSystemIds, batchSize);
                break;

            case "UPDATE_VERTEX_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                List<UpdateVertexPropertyParams.VertexUpdate> vertexUpdates =
                    parseVertexUpdates((List<Map<String, Object>>) parameters.get("updates"));
                originalOpsCount = ((List<?>) parameters.get("updates")).size();
                validOpsCount = vertexUpdates.size();
                latencies = propExec.updateVertexProperty(vertexUpdates, batchSize);
                break;
            }

            case "UPDATE_EDGE_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                String updateEdgeLabel = (String) parameters.get("label");
                List<UpdateEdgePropertyParams.EdgeUpdate> edgeUpdates =
                    parseEdgeUpdates((List<Map<String, Object>>) parameters.get("updates"));
                originalOpsCount = ((List<?>) parameters.get("updates")).size();
                validOpsCount = edgeUpdates.size();
                latencies = propExec.updateEdgeProperty(updateEdgeLabel, edgeUpdates, batchSize);
                break;
            }

            case "GET_VERTEX_BY_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                List<GetVertexByPropertyParams.PropertyQuery> vertexQueries =
                    parseVertexPropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                originalOpsCount = ((List<?>) parameters.get("queries")).size();
                validOpsCount = vertexQueries.size();
                latencies = propExec.getVertexByProperty(vertexQueries, batchSize);
                break;
            }

            case "GET_EDGE_BY_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                String getEdgeLabel = (String) parameters.get("label");
                List<GetEdgeByPropertyParams.PropertyQuery> edgeQueries =
                    parseEdgePropertyQueries((List<Map<String, Object>>) parameters.get("queries"));
                originalOpsCount = ((List<?>) parameters.get("queries")).size();
                validOpsCount = edgeQueries.size();
                latencies = propExec.getEdgeByProperty(getEdgeLabel, edgeQueries, batchSize);
                break;
            }

            default:
                throw new RuntimeException("Unknown task type: " + taskType);
        }

        int errorsAfter = executor.getErrorCount();
        int errorCount = errorsAfter - errorsBefore;

        result.put("batch_size", batchSize);
        result.put("status", "success");
        result.put("latency_us", calculateLatencyStats(latencies));
        result.put("errorCount", errorCount);
        result.put("originalOpsCount", originalOpsCount);
        result.put("validOpsCount", validOpsCount);
        result.put("filteredOpsCount", originalOpsCount - validOpsCount);
    }

    // Helper methods for parsing parameters

    private List<Object> parseIdList(Object obj) {
        List<Object> systemIds = new ArrayList<>();
        if (obj instanceof List) {
            for (Object item : (List<?>) obj) {
                if (item instanceof Number) {
                    Long originId = ((Number) item).longValue();
                    Object systemId = executor.getSystemId(originId);
                    if (systemId != null) {
                        systemIds.add(systemId);
                    }
                }
            }
        }
        return systemIds;
    }

    private List<UpdateVertexPropertyParams.VertexUpdate> parseVertexUpdates(List<Map<String, Object>> updates) {
        List<UpdateVertexPropertyParams.VertexUpdate> result = new ArrayList<>();
        for (Map<String, Object> update : updates) {
            Long originId = ((Number) update.get("id")).longValue();
            Object systemId = executor.getSystemId(originId);
            if (systemId != null) {
                UpdateVertexPropertyParams.VertexUpdate vu = new UpdateVertexPropertyParams.VertexUpdate();
                vu.setSystemId(systemId);
                vu.setProperties((Map<String, Object>) update.get("properties"));
                result.add(vu);
            }
        }
        return result;
    }

    private List<AddEdgeParams.EdgePair> parseEdgePairs(List<Map<String, Object>> pairs) {
        List<AddEdgeParams.EdgePair> result = new ArrayList<>();
        for (Map<String, Object> pair : pairs) {
            Long srcOriginId = ((Number) pair.get("src")).longValue();
            Long dstOriginId = ((Number) pair.get("dst")).longValue();
            Object srcSystemId = executor.getSystemId(srcOriginId);
            Object dstSystemId = executor.getSystemId(dstOriginId);
            if (srcSystemId != null && dstSystemId != null) {
                AddEdgeParams.EdgePair ep = new AddEdgeParams.EdgePair();
                ep.setSrcSystemId(srcSystemId);
                ep.setDstSystemId(dstSystemId);
                result.add(ep);
            }
        }
        return result;
    }

    private List<RemoveEdgeParams.EdgePair> parseRemoveEdgePairs(List<Map<String, Object>> pairs) {
        List<RemoveEdgeParams.EdgePair> result = new ArrayList<>();
        for (Map<String, Object> pair : pairs) {
            Long srcOriginId = ((Number) pair.get("src")).longValue();
            Long dstOriginId = ((Number) pair.get("dst")).longValue();
            Object srcSystemId = executor.getSystemId(srcOriginId);
            Object dstSystemId = executor.getSystemId(dstOriginId);
            if (srcSystemId != null && dstSystemId != null) {
                RemoveEdgeParams.EdgePair ep = new RemoveEdgeParams.EdgePair();
                ep.setSrcSystemId(srcSystemId);
                ep.setDstSystemId(dstSystemId);
                result.add(ep);
            }
        }
        return result;
    }

    private List<UpdateEdgePropertyParams.EdgeUpdate> parseEdgeUpdates(List<Map<String, Object>> updates) {
        List<UpdateEdgePropertyParams.EdgeUpdate> result = new ArrayList<>();
        for (Map<String, Object> update : updates) {
            Long srcOriginId = ((Number) update.get("src")).longValue();
            Long dstOriginId = ((Number) update.get("dst")).longValue();
            Object srcSystemId = executor.getSystemId(srcOriginId);
            Object dstSystemId = executor.getSystemId(dstOriginId);
            if (srcSystemId != null && dstSystemId != null) {
                UpdateEdgePropertyParams.EdgeUpdate eu = new UpdateEdgePropertyParams.EdgeUpdate();
                eu.setSrcSystemId(srcSystemId);
                eu.setDstSystemId(dstSystemId);
                eu.setProperties((Map<String, Object>) update.get("properties"));
                result.add(eu);
            }
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

    private List<GetVertexByPropertyParams.PropertyQuery> parseVertexPropertyQueries(List<Map<String, Object>> queries) {
        List<GetVertexByPropertyParams.PropertyQuery> result = new ArrayList<>();
        for (Map<String, Object> query : queries) {
            GetVertexByPropertyParams.PropertyQuery pq = new GetVertexByPropertyParams.PropertyQuery();
            pq.setKey((String) query.get("key"));
            pq.setValue(query.get("value"));
            result.add(pq);
        }
        return result;
    }

    private List<GetEdgeByPropertyParams.PropertyQuery> parseEdgePropertyQueries(List<Map<String, Object>> queries) {
        List<GetEdgeByPropertyParams.PropertyQuery> result = new ArrayList<>();
        for (Map<String, Object> query : queries) {
            GetEdgeByPropertyParams.PropertyQuery pq = new GetEdgeByPropertyParams.PropertyQuery();
            pq.setKey((String) query.get("key"));
            pq.setValue(query.get("value"));
            result.add(pq);
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
