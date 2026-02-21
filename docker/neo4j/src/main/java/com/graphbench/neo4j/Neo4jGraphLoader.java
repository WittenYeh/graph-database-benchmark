package com.graphbench.neo4j;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.ProgressCallback;
import com.graphbench.api.TypeConverter;
import org.neo4j.graphdb.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

        // Read metadata first so we can use it during loading
        metadata = CsvGraphReader.readHeaders(datasetPath);

        nodeIdsMap.clear();
        RelationshipType relType = RelationshipType.withName("MyEdge");
        int[] edgeCount = {0};
        int[] opsInTx = {0};
        Transaction[] txHolder = {db.beginTx()};

        CsvGraphReader.read(datasetPath,
            (nodeId, properties) -> {
                Node node = txHolder[0].createNode(Label.label(NODE_LABEL));
                nodeIdsMap.put(nodeId, node.getId());
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        Class<?> targetType = metadata.getNodePropertyType(e.getKey());
                        Object value = TypeConverter.convertFromString(e.getValue(), targetType);
                        if (value != null) {
                            node.setProperty(e.getKey(), value);
                        }
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
                        Class<?> targetType = metadata.getEdgePropertyType(e.getKey());
                        Object value = TypeConverter.convertFromString(e.getValue(), targetType);
                        if (value != null) {
                            rel.setProperty(e.getKey(), value);
                        }
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

    /**
     * Create property indexes for all node and edge properties.
     * Should be called after loading data.
     */
    public void createPropertyIndexes(CsvGraphReader.CsvMetadata metadata) {
        for (String prop : metadata.getNodePropertyHeaders()) {
            Class<?> type = metadata.getNodePropertyType(prop);
            progressCallback.sendLogMessage("▶️ Creating node property index: " + prop + " (" + type.getSimpleName() + ")", "INFO");
            try (Transaction tx = db.beginTx()) {
                tx.execute("CREATE INDEX node_" + prop + "_idx IF NOT EXISTS FOR (n:" + NODE_LABEL + ") ON (n." + prop + ")");
                tx.commit();
                progressCallback.sendLogMessage("✅ Created node property index: " + prop, "INFO");
            } catch (Exception e) {
                progressCallback.sendLogMessage("Failed to create node index for " + prop + ": " + e.getMessage(), "WARNING");
            }
        }
        for (String prop : metadata.getEdgePropertyHeaders()) {
            Class<?> type = metadata.getEdgePropertyType(prop);
            progressCallback.sendLogMessage("▶️ Creating edge property index: " + prop + " (" + type.getSimpleName() + ")", "INFO");
            try (Transaction tx = db.beginTx()) {
                tx.execute("CREATE INDEX edge_" + prop + "_idx IF NOT EXISTS FOR ()-[r:MyEdge]-() ON (r." + prop + ")");
                tx.commit();
                progressCallback.sendLogMessage("✅ Created edge property index: " + prop, "INFO");
            } catch (Exception e) {
                progressCallback.sendLogMessage("Failed to create edge index for " + prop + ": " + e.getMessage(), "WARNING");
            }
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(30, TimeUnit.SECONDS);
            tx.commit();
            progressCallback.sendLogMessage("✅ All property indexes online", "INFO");
        } catch (Exception e) {
            progressCallback.sendLogMessage("Timeout waiting for indexes: " + e.getMessage(), "WARNING");
        }
    }
}
