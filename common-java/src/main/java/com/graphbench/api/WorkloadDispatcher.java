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

    private static final Set<String> VERTEX_PROPERTY_TASKS = new HashSet<>(Arrays.asList(
        "UPDATE_VERTEX_PROPERTY", "GET_VERTEX_BY_PROPERTY"
    ));
    private static final Set<String> EDGE_PROPERTY_TASKS = new HashSet<>(Arrays.asList(
        "UPDATE_EDGE_PROPERTY", "GET_EDGE_BY_PROPERTY"
    ));

    private final BenchmarkExecutor executor;
    private final String datasetPath;
    private final ProgressCallback progressCallback;

    // Populated after LOAD_GRAPH; used to skip property tasks when dataset has no properties.
    private CsvGraphReader.CsvMetadata csvMetadata;

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

                // Read CSV headers to determine whether the dataset has vertex/edge properties.
                // This is used later to skip property benchmark tasks on plain topology datasets.
                csvMetadata = CsvGraphReader.readHeaders(datasetPath);
                String nodeProps = Arrays.toString(csvMetadata.getNodePropertyHeaders());
                String edgeProps = Arrays.toString(csvMetadata.getEdgePropertyHeaders());
                progressCallback.sendLogMessage(
                    "Dataset headers loaded — node properties: " + nodeProps
                    + ", edge properties: " + edgeProps,
                    "INFO"
                );

                // Create snapshot after loading graph
                progressCallback.sendProgressCallback("snapshot_start", "SNAPSHOT", null, null, null, taskIndex, totalTasks);
                executor.snapGraph();
                progressCallback.sendProgressCallback("snapshot_complete", "SNAPSHOT", null, "success", null, taskIndex, totalTasks);
            } else {
                // Check whether the dataset supports the required property columns.
                // csvMetadata must be populated by a prior LOAD_GRAPH task.
                if (csvMetadata == null) {
                    throw new RuntimeException("csvMetadata is null: LOAD_GRAPH must run before any other task");
                }

                boolean isVertexPropTask = VERTEX_PROPERTY_TASKS.contains(taskType);
                boolean isEdgePropTask   = EDGE_PROPERTY_TASKS.contains(taskType);
                boolean hasVertexProps   = csvMetadata.getNodePropertyHeaders().length > 0;
                boolean hasEdgeProps     = csvMetadata.getEdgePropertyHeaders().length > 0;

                if ((isVertexPropTask && !hasVertexProps) || (isEdgePropTask && !hasEdgeProps)) {
                    String reason = isVertexPropTask
                        ? "Dataset has no vertex property columns in nodes.csv"
                        : "Dataset has no edge property columns in edges.csv";
                    progressCallback.sendLogMessage(
                        "Skipping " + taskType + ": " + reason, "INFO"
                    );
                    result.put("status", "unexecute");
                    result.put("skip_reason", reason);
                    return result;
                }

                // Get batch_sizes, default to [1] if not specified
                List<Integer> batchSizes = task.getBatchSizes();
                if (batchSizes == null || batchSizes.isEmpty()) {
                    batchSizes = Arrays.asList(1);
                }

                // Pre-process parameters once (type conversion, parsing) before all batch size tests
                Map<String, Object> preprocessedParams = preprocessParameters(taskType, parameters);

                // Execute task for each batch size
                List<Map<String, Object>> batchResults = new ArrayList<>();

                for (int batchSize : batchSizes) {
                    // Restore graph to clean state before executing workload
                    progressCallback.sendProgressCallback("restore_start", "RESTORE", null, null, null, taskIndex, totalTasks);
                    executor.restoreGraph();
                    progressCallback.sendProgressCallback("restore_complete", "RESTORE", null, "success", null, taskIndex, totalTasks);

                    // Calculate num_ops for timeout monitoring
                    Integer numOps = getNumOps(taskType, preprocessedParams);

                    // Send subtask start callback with num_ops
                    String subtaskName = taskType + " (batch_size=" + batchSize + ")";
                    progressCallback.sendProgressCallback("subtask_start", subtaskName, null, null, null, taskIndex, totalTasks,
                                                         null, null, null, numOps);

                    Map<String, Object> subtaskResult = new HashMap<>();
                    long taskStartTime = System.nanoTime();
                    executeTask(taskType, preprocessedParams, subtaskResult, batchSize);
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
     * Pre-process parameters before timing starts.
     * This includes type conversion and parsing that should not be counted in benchmark timing.
     */
    private Map<String, Object> preprocessParameters(String taskType, Map<String, Object> parameters) {
        Map<String, Object> preprocessed = new HashMap<>(parameters);
        switch (taskType) {
            case "REMOVE_VERTEX":
                preprocessed.put("_parsed_ids", parseIdList(parameters.get("ids")));
                break;
            case "ADD_EDGE":
                preprocessed.put("_parsed_pairs", parseEdgePairs((List<Map<String, Object>>) parameters.get("pairs")));
                break;
            case "REMOVE_EDGE":
                preprocessed.put("_parsed_pairs", parseRemoveEdgePairs((List<Map<String, Object>>) parameters.get("pairs")));
                break;
            case "GET_NBRS":
                preprocessed.put("_parsed_ids", parseIdList(parameters.get("ids")));
                break;
            case "UPDATE_VERTEX_PROPERTY":
                preprocessed.put("_parsed_updates", parseVertexUpdates((List<Map<String, Object>>) parameters.get("updates")));
                break;
            case "UPDATE_EDGE_PROPERTY":
                preprocessed.put("_parsed_updates", parseEdgeUpdates((List<Map<String, Object>>) parameters.get("updates")));
                break;
            case "GET_VERTEX_BY_PROPERTY":
                preprocessed.put("_parsed_queries", parseVertexPropertyQueries((List<Map<String, Object>>) parameters.get("queries")));
                break;
            case "GET_EDGE_BY_PROPERTY":
                preprocessed.put("_parsed_queries", parseEdgePropertyQueries((List<Map<String, Object>>) parameters.get("queries")));
                break;
        }
        return preprocessed;
    }

    /**
     * Get the number of operations for a task (for timeout calculation).
     */
    private Integer getNumOps(String taskType, Map<String, Object> parameters) {
        switch (taskType) {
            case "ADD_NODE":
            case "REMOVE_NODE":
            case "GET_NODE":
            case "GET_NBRS":
                Object ids = parameters.get("ids");
                return (ids instanceof List) ? ((List<?>) ids).size() : 0;

            case "ADD_EDGE":
            case "REMOVE_EDGE":
                Object pairs = parameters.get("pairs");
                return (pairs instanceof List) ? ((List<?>) pairs).size() : 0;

            case "UPDATE_VERTEX_PROPERTY":
            case "UPDATE_EDGE_PROPERTY":
                Object updates = parameters.get("updates");
                return (updates instanceof List) ? ((List<?>) updates).size() : 0;

            case "GET_VERTEX_BY_PROPERTY":
            case "GET_EDGE_BY_PROPERTY":
                Object queries = parameters.get("queries");
                return (queries instanceof List) ? ((List<?>) queries).size() : 0;

            default:
                return 0;
        }
    }

    /**
     * Execute a task with specified batch size.
     * Parameters should already be preprocessed.
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
                List<Object> removeVertexSystemIds = (List<Object>) parameters.get("_parsed_ids");
                originalOpsCount = ((List<?>) parameters.get("ids")).size();
                validOpsCount = removeVertexSystemIds.size();
                latencies = executor.removeVertex(removeVertexSystemIds, batchSize);
                break;

            case "ADD_EDGE":
                String addEdgeLabel = (String) parameters.get("label");
                List<AddEdgeParams.EdgePair> addEdgePairs = (List<AddEdgeParams.EdgePair>) parameters.get("_parsed_pairs");
                originalOpsCount = ((List<?>) parameters.get("pairs")).size();
                validOpsCount = addEdgePairs.size();
                latencies = executor.addEdge(addEdgeLabel, addEdgePairs, batchSize);
                break;

            case "REMOVE_EDGE":
                String removeEdgeLabel = (String) parameters.get("label");
                List<RemoveEdgeParams.EdgePair> removeEdgePairs = (List<RemoveEdgeParams.EdgePair>) parameters.get("_parsed_pairs");
                originalOpsCount = ((List<?>) parameters.get("pairs")).size();
                validOpsCount = removeEdgePairs.size();
                latencies = executor.removeEdge(removeEdgeLabel, removeEdgePairs, batchSize);
                break;

            case "GET_NBRS":
                String direction = (String) parameters.get("direction");
                List<Object> nbrsSystemIds = (List<Object>) parameters.get("_parsed_ids");
                originalOpsCount = ((List<?>) parameters.get("ids")).size();
                validOpsCount = nbrsSystemIds.size();
                latencies = executor.getNbrs(direction, nbrsSystemIds, batchSize);
                break;

            case "UPDATE_VERTEX_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                List<UpdateVertexPropertyParams.VertexUpdate> vertexUpdates =
                    (List<UpdateVertexPropertyParams.VertexUpdate>) parameters.get("_parsed_updates");
                originalOpsCount = ((List<?>) parameters.get("updates")).size();
                validOpsCount = vertexUpdates.size();
                latencies = propExec.updateVertexProperty(vertexUpdates, batchSize);
                break;
            }

            case "UPDATE_EDGE_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                String updateEdgeLabel = (String) parameters.get("label");
                List<UpdateEdgePropertyParams.EdgeUpdate> edgeUpdates =
                    (List<UpdateEdgePropertyParams.EdgeUpdate>) parameters.get("_parsed_updates");
                originalOpsCount = ((List<?>) parameters.get("updates")).size();
                validOpsCount = edgeUpdates.size();
                latencies = propExec.updateEdgeProperty(updateEdgeLabel, edgeUpdates, batchSize);
                break;
            }

            case "GET_VERTEX_BY_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                List<GetVertexByPropertyParams.PropertyQuery> vertexQueries =
                    (List<GetVertexByPropertyParams.PropertyQuery>) parameters.get("_parsed_queries");
                originalOpsCount = ((List<?>) parameters.get("queries")).size();
                validOpsCount = vertexQueries.size();
                latencies = propExec.getVertexByProperty(vertexQueries, batchSize);
                break;
            }

            case "GET_EDGE_BY_PROPERTY": {
                PropertyBenchmarkExecutor propExec = requirePropertyExecutor();
                List<GetEdgeByPropertyParams.PropertyQuery> edgeQueries =
                    (List<GetEdgeByPropertyParams.PropertyQuery>) parameters.get("_parsed_queries");
                originalOpsCount = ((List<?>) parameters.get("queries")).size();
                validOpsCount = edgeQueries.size();
                latencies = propExec.getEdgeByProperty(edgeQueries, batchSize);
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
            String key = (String) query.get("key");
            Object value = query.get("value");

            // Convert value to the correct type based on metadata
            if (csvMetadata != null && value != null) {
                Class<?> targetType = csvMetadata.getNodePropertyType(key);
                value = TypeConverter.convertQueryValue(value, targetType);
            }

            pq.setKey(key);
            pq.setValue(value);
            result.add(pq);
        }
        return result;
    }

    private List<GetEdgeByPropertyParams.PropertyQuery> parseEdgePropertyQueries(List<Map<String, Object>> queries) {
        List<GetEdgeByPropertyParams.PropertyQuery> result = new ArrayList<>();
        for (Map<String, Object> query : queries) {
            GetEdgeByPropertyParams.PropertyQuery pq = new GetEdgeByPropertyParams.PropertyQuery();
            String key = (String) query.get("key");
            Object value = query.get("value");

            // Convert value to the correct type based on metadata
            if (csvMetadata != null && value != null) {
                Class<?> targetType = csvMetadata.getEdgePropertyType(key);
                value = TypeConverter.convertQueryValue(value, targetType);
            }

            pq.setKey(key);
            pq.setValue(value);
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
