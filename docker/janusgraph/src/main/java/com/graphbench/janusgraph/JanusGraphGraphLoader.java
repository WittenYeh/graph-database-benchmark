package com.graphbench.janusgraph;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.ProgressCallback;
import com.graphbench.api.TypeConverter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles CSV graph loading for JanusGraph.
 * The doLoadProperty flag controls whether CSV property columns are stored on vertices/edges.
 */
public class JanusGraphGraphLoader {

    private static final String NODE_LABEL = "MyNode";
    private static final int BATCH_SIZE = 10000;

    private final JanusGraph graph;
    private final GraphTraversalSource g;
    private final ProgressCallback progressCallback;
    private final boolean doLoadProperty;

    private final Map<Long, Object> nodeIdsMap = new HashMap<>();
    private CsvGraphReader.CsvMetadata metadata;

    public JanusGraphGraphLoader(JanusGraph graph, GraphTraversalSource g, ProgressCallback progressCallback, boolean doLoadProperty) {
        this.graph = graph;
        this.g = g;
        this.progressCallback = progressCallback;
        this.doLoadProperty = doLoadProperty;
    }

    public Map<String, Object> load(String datasetPath) throws Exception {
        progressCallback.sendLogMessage("Loading graph from CSV: " + datasetPath
                + (doLoadProperty ? " (with properties)" : " (topology only)"), "INFO");
        long startTime = System.nanoTime();

        // Read metadata first so we can use it during loading
        metadata = CsvGraphReader.readHeaders(datasetPath);

        nodeIdsMap.clear();
        int[] edgeCount = {0};
        int[] opsInTx = {0};

        CsvGraphReader.read(datasetPath,
            (nodeId, properties) -> {
                Vertex v = g.addV(NODE_LABEL).next();
                nodeIdsMap.put(nodeId, v.id());
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        Class<?> targetType = metadata.getNodePropertyType(e.getKey());
                        Object value = TypeConverter.convertFromString(e.getValue(), targetType);
                        if (value != null) {
                            v.property(e.getKey(), value);
                        }
                    }
                }
                opsInTx[0]++;
                if (opsInTx[0] >= BATCH_SIZE) {
                    g.tx().commit();
                    opsInTx[0] = 0;
                }
            },
            (srcId, dstId, properties) -> {
                Object srcInternal = nodeIdsMap.get(srcId);
                Object dstInternal = nodeIdsMap.get(dstId);
                if (srcInternal == null || dstInternal == null) return;
                Edge edge = g.V(srcInternal).addE("MyEdge").to(__.V(dstInternal)).next();
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        Class<?> targetType = metadata.getEdgePropertyType(e.getKey());
                        Object value = TypeConverter.convertFromString(e.getValue(), targetType);
                        if (value != null) {
                            edge.property(e.getKey(), value);
                        }
                    }
                }
                edgeCount[0]++;
                opsInTx[0]++;
                if (opsInTx[0] >= BATCH_SIZE) {
                    g.tx().commit();
                    opsInTx[0] = 0;
                }
            }
        );

        g.tx().commit();

        double durationSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
        int nodeCount = nodeIdsMap.size();
        progressCallback.sendLogMessage("Loaded " + nodeCount + " nodes and " + edgeCount[0] + " edges", "INFO");

        Map<String, Object> result = new HashMap<>();
        result.put("nodes", nodeCount);
        result.put("edges", edgeCount[0]);
        result.put("durationSeconds", durationSeconds);
        if (doLoadProperty) {
            result.put("nodePropertyHeaders", metadata.getNodePropertyHeaders());
            result.put("edgePropertyHeaders", metadata.getEdgePropertyHeaders());
        }
        return result;
    }

    public Map<Long, Object> getNodeIdsMap() { return nodeIdsMap; }

    public CsvGraphReader.CsvMetadata getMetadata() { return metadata; }

    /**
     * Create property indexes for all node and edge properties.
     * Must be called before loading data to ensure indexes are populated during insertion.
     */
    public void createPropertyIndexes(CsvGraphReader.CsvMetadata metadata) throws Exception {
        JanusGraphManagement mgmt = graph.openManagement();
        List<String> nodeIndexNames = new ArrayList<>();
        List<String> edgeIndexNames = new ArrayList<>();

        try {
            for (String prop : metadata.getNodePropertyHeaders()) {
                Class<?> dataType = metadata.getNodePropertyType(prop);
                progressCallback.sendLogMessage("▶️ Creating node property index: " + prop + " (" + dataType.getSimpleName() + ")", "INFO");
                PropertyKey key = mgmt.getPropertyKey(prop);
                if (key == null) {
                    key = mgmt.makePropertyKey(prop).dataType(dataType).make();
                }
                String indexName = "node_" + prop + "_idx";
                if (!mgmt.containsGraphIndex(indexName)) {
                    mgmt.buildIndex(indexName, Vertex.class).addKey(key).buildCompositeIndex();
                    nodeIndexNames.add(indexName);
                    progressCallback.sendLogMessage("✅ Created node property index: " + prop, "INFO");
                }
            }
            for (String prop : metadata.getEdgePropertyHeaders()) {
                Class<?> dataType = metadata.getEdgePropertyType(prop);
                progressCallback.sendLogMessage("▶️ Creating edge property index: " + prop + " (" + dataType.getSimpleName() + ")", "INFO");
                PropertyKey key = mgmt.getPropertyKey(prop);
                if (key == null) {
                    key = mgmt.makePropertyKey(prop).dataType(dataType).make();
                }
                String indexName = "edge_" + prop + "_idx";
                if (!mgmt.containsGraphIndex(indexName)) {
                    mgmt.buildIndex(indexName, Edge.class).addKey(key).buildCompositeIndex();
                    edgeIndexNames.add(indexName);
                    progressCallback.sendLogMessage("✅ Created edge property index: " + prop, "INFO");
                }
            }
            mgmt.commit();
            progressCallback.sendLogMessage("✅ All property indexes created and enabled", "INFO");
        } catch (Exception e) {
            progressCallback.sendLogMessage("Failed to create property indexes: " + e.getMessage(), "WARNING");
            try { mgmt.rollback(); } catch (Exception ex) { /* ignore */ }
            throw e;
        }
    }
}
