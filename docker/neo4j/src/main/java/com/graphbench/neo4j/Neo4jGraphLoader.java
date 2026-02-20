package com.graphbench.neo4j;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.ProgressCallback;
import org.neo4j.graphdb.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles CSV graph loading for Neo4j.
 * The doLoadProperty flag controls whether CSV property columns are stored on nodes/edges.
 */
public class Neo4jGraphLoader {

    private static final String NODE_LABEL = "MyNode";
    private static final int BATCH_SIZE = 10000;

    private final GraphDatabaseService db;
    private final ProgressCallback progressCallback;
    private final boolean doLoadProperty;

    /** After loading, contains origin ID -> Neo4j internal node ID mapping */
    private final Map<Long, Long> nodeIdsMap = new HashMap<>();
    private CsvGraphReader.CsvMetadata metadata;

    public Neo4jGraphLoader(GraphDatabaseService db, ProgressCallback progressCallback, boolean doLoadProperty) {
        this.db = db;
        this.progressCallback = progressCallback;
        this.doLoadProperty = doLoadProperty;
    }

    public Map<String, Object> load(String datasetPath) throws Exception {
        progressCallback.sendLogMessage("Loading graph from CSV: " + datasetPath
                + (doLoadProperty ? " (with properties)" : " (topology only)"), "INFO");
        long startTime = System.nanoTime();

        nodeIdsMap.clear();
        RelationshipType relType = RelationshipType.withName("MyEdge");
        int[] edgeCount = {0};
        int[] opsInTx = {0};
        Transaction[] txHolder = {db.beginTx()};

        metadata = CsvGraphReader.read(datasetPath,
            (nodeId, properties) -> {
                Node node = txHolder[0].createNode(Label.label(NODE_LABEL));
                nodeIdsMap.put(nodeId, node.getId());
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        node.setProperty(e.getKey(), e.getValue());
                    }
                }
                opsInTx[0]++;
                if (opsInTx[0] >= BATCH_SIZE) {
                    txHolder[0].commit(); txHolder[0].close();
                    txHolder[0] = db.beginTx(); opsInTx[0] = 0;
                }
            },
            (srcId, dstId, properties) -> {
                Long srcInternal = nodeIdsMap.get(srcId);
                Long dstInternal = nodeIdsMap.get(dstId);
                if (srcInternal == null || dstInternal == null) return;
                Node srcNode = txHolder[0].getNodeById(srcInternal);
                Node dstNode = txHolder[0].getNodeById(dstInternal);
                Relationship rel = srcNode.createRelationshipTo(dstNode, relType);
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        rel.setProperty(e.getKey(), e.getValue());
                    }
                }
                edgeCount[0]++;
                opsInTx[0]++;
                if (opsInTx[0] >= BATCH_SIZE) {
                    txHolder[0].commit(); txHolder[0].close();
                    txHolder[0] = db.beginTx(); opsInTx[0] = 0;
                }
            }
        );

        txHolder[0].commit();
        txHolder[0].close();

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

    public Map<Long, Long> getNodeIdsMap() { return nodeIdsMap; }

    public CsvGraphReader.CsvMetadata getMetadata() { return metadata; }
}
