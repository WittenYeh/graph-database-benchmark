package com.graphbench.janusgraph;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.ProgressCallback;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles CSV graph loading for JanusGraph.
 * The doLoadProperty flag controls whether CSV property columns are stored on vertices/edges.
 */
public class JanusGraphGraphLoader {

    private static final String NODE_LABEL = "MyNode";
    private static final int BATCH_SIZE = 10000;

    private final GraphTraversalSource g;
    private final ProgressCallback progressCallback;
    private final boolean doLoadProperty;

    private final Map<Long, Object> nodeIdsMap = new HashMap<>();
    private CsvGraphReader.CsvMetadata metadata;

    public JanusGraphGraphLoader(GraphTraversalSource g, ProgressCallback progressCallback, boolean doLoadProperty) {
        this.g = g;
        this.progressCallback = progressCallback;
        this.doLoadProperty = doLoadProperty;
    }

    public Map<String, Object> load(String datasetPath) throws Exception {
        progressCallback.sendLogMessage("Loading graph from CSV: " + datasetPath
                + (doLoadProperty ? " (with properties)" : " (topology only)"), "INFO");
        long startTime = System.nanoTime();

        nodeIdsMap.clear();
        int[] edgeCount = {0};
        int[] opsInTx = {0};

        metadata = CsvGraphReader.read(datasetPath,
            (nodeId, properties) -> {
                Vertex v = g.addV(NODE_LABEL).next();
                nodeIdsMap.put(nodeId, v.id());
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        v.property(e.getKey(), e.getValue());
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
                        edge.property(e.getKey(), e.getValue());
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
}
