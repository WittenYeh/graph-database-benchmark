package com.graphbench.api;

import com.graphbench.workload.*;

import java.util.*;

/**
 * ParameterParser handles parsing and preprocessing of workload parameters.
 * This includes type conversion, ID mapping, and validation that should not be counted in benchmark timing.
 */
public class ParameterParser {
    private final BenchmarkExecutor executor;
    private final CsvGraphReader.CsvMetadata csvMetadata;

    /**
     * Create a parameter parser.
     * @param executor The benchmark executor for ID mapping
     * @param csvMetadata CSV metadata for type conversion (can be null)
     */
    public ParameterParser(BenchmarkExecutor executor, CsvGraphReader.CsvMetadata csvMetadata) {
        this.executor = executor;
        this.csvMetadata = csvMetadata;
    }

    /**
     * Pre-process parameters before timing starts.
     * This includes type conversion and parsing that should not be counted in benchmark timing.
     */
    public Map<String, Object> preprocessParameters(String taskType, Map<String, Object> parameters) {
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
    public static Integer getNumOps(String taskType, Map<String, Object> parameters) {
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
}
