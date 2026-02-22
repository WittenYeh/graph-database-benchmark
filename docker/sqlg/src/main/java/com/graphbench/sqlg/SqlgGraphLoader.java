package com.graphbench.sqlg;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.ProgressCallback;
import com.graphbench.api.TypeConverter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.umlg.sqlg.structure.SqlgGraph;

import java.util.*;

/**
 * CSV graph loader for SQLG.
 * Loads nodes and edges from CSV files with optional property support.
 */
public class SqlgGraphLoader {
    private static final String NODE_LABEL = "MyNode";
    private static final String EDGE_LABEL = "MyEdge";
    private static final int BATCH_SIZE = 10000;

    private final SqlgGraph graph;
    private final GraphTraversalSource g;
    private final ProgressCallback progressCallback;
    private final boolean doLoadProperty;
    private final Map<Long, Object> nodeIdsMap = new HashMap<>();
    private CsvGraphReader.CsvMetadata metadata;

    public SqlgGraphLoader(SqlgGraph graph, GraphTraversalSource g, ProgressCallback progressCallback, boolean doLoadProperty) {
        this.graph = graph;
        this.g = g;
        this.progressCallback = progressCallback;
        this.doLoadProperty = doLoadProperty;
    }

    public Map<String, Object> load(String datasetPath) throws Exception {
        long startTime = System.nanoTime();

        progressCallback.sendLogMessage("Loading graph from CSV...", "INFO");

        final int[] nodeCount = {0};
        final int[] edgeCount = {0};
        final int[] batchCount = {0};

        // Read CSV with callbacks
        metadata = CsvGraphReader.read(
            datasetPath,
            (nodeId, properties) -> {
                try {
                    Vertex v = g.addV(NODE_LABEL).next();
                    nodeIdsMap.put(nodeId, v.id());

                    if (doLoadProperty && properties != null && !properties.isEmpty()) {
                        for (Map.Entry<String, String> entry : properties.entrySet()) {
                            String key = entry.getKey();
                            String value = entry.getValue();
                            if (value != null && !value.isEmpty()) {
                                Class<?> targetType = metadata.getNodePropertyType(key);
                                Object convertedValue = TypeConverter.convertFromString(value, targetType);
                                v.property(key, convertedValue);
                            }
                        }
                    }

                    nodeCount[0]++;
                    batchCount[0]++;

                    if (batchCount[0] >= BATCH_SIZE) {
                        graph.tx().commit();
                        batchCount[0] = 0;
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Error loading node: " + nodeId, e);
                }
            },
            (srcId, dstId, properties) -> {
                try {
                    Object srcSystemId = nodeIdsMap.get(srcId);
                    Object dstSystemId = nodeIdsMap.get(dstId);

                    if (srcSystemId != null && dstSystemId != null) {
                        org.apache.tinkerpop.gremlin.structure.Edge e =
                            g.V(srcSystemId).addE(EDGE_LABEL).to(__.V(dstSystemId)).next();

                        if (doLoadProperty && properties != null && !properties.isEmpty()) {
                            for (Map.Entry<String, String> entry : properties.entrySet()) {
                                String key = entry.getKey();
                                String value = entry.getValue();
                                if (value != null && !value.isEmpty()) {
                                    Class<?> targetType = metadata.getEdgePropertyType(key);
                                    Object convertedValue = TypeConverter.convertFromString(value, targetType);
                                    e.property(key, convertedValue);
                                }
                            }
                        }

                        edgeCount[0]++;
                        batchCount[0]++;

                        if (batchCount[0] >= BATCH_SIZE) {
                            graph.tx().commit();
                            batchCount[0] = 0;
                        }
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Error loading edge: " + srcId + " -> " + dstId, ex);
                }
            }
        );

        if (batchCount[0] > 0) {
            graph.tx().commit();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

        progressCallback.sendLogMessage("Loaded " + nodeCount[0] + " nodes and " + edgeCount[0] + " edges", "INFO");

        Map<String, Object> result = new HashMap<>();
        result.put("nodes", nodeCount[0]);
        result.put("edges", edgeCount[0]);
        result.put("durationSeconds", durationSeconds);

        if (doLoadProperty) {
            result.put("nodePropertyHeaders", metadata.getNodePropertyHeaders());
            result.put("edgePropertyHeaders", metadata.getEdgePropertyHeaders());
        }

        return result;
    }

    public Map<Long, Object> getNodeIdsMap() {
        return nodeIdsMap;
    }

    public CsvGraphReader.CsvMetadata getMetadata() {
        return metadata;
    }
}
