package com.graphbench.orientdb;

import com.graphbench.api.CsvGraphReader;
import com.graphbench.api.ProgressCallback;
import com.graphbench.api.TypeConverter;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.OEdge;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles CSV graph loading for OrientDB.
 * The doLoadProperty flag controls whether CSV property columns are stored on nodes/edges.
 */
public class OrientDBGraphLoader {

    private static final String VERTEX_CLASS = "MyNode";
    private static final String EDGE_CLASS = "MyEdge";
    private static final int BATCH_SIZE = 10000;

    private final ODatabaseSession db;
    private final ProgressCallback progressCallback;
    private final boolean doLoadProperty;

    /** After loading, contains origin ID -> OrientDB RID mapping */
    private final Map<Long, Object> nodeIdsMap = new HashMap<>();
    private CsvGraphReader.CsvMetadata metadata;

    public OrientDBGraphLoader(ODatabaseSession db, ProgressCallback progressCallback, boolean doLoadProperty) {
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
        int[] edgeCount = {0};
        int[] opsInTx = {0};
        boolean[] inTransaction = {false};

        CsvGraphReader.read(datasetPath,
            (nodeId, properties) -> {
                if (!inTransaction[0]) {
                    db.begin();
                    inTransaction[0] = true;
                }
                OVertex vertex = db.newVertex(VERTEX_CLASS);
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        Class<?> targetType = metadata.getNodePropertyType(e.getKey());
                        Object value = TypeConverter.convertFromString(e.getValue(), targetType);
                        if (value != null) {
                            vertex.setProperty(e.getKey(), value);
                        }
                    }
                }
                vertex.save();
                nodeIdsMap.put(nodeId, vertex.getIdentity());
                opsInTx[0]++;
                if (opsInTx[0] >= BATCH_SIZE) {
                    db.commit();
                    inTransaction[0] = false;
                    opsInTx[0] = 0;
                }
            },
            (srcId, dstId, properties) -> {
                if (!inTransaction[0]) {
                    db.begin();
                    inTransaction[0] = true;
                }
                Object srcRID = nodeIdsMap.get(srcId);
                Object dstRID = nodeIdsMap.get(dstId);
                if (srcRID == null || dstRID == null) return;
                OVertex srcVertex = db.load((com.orientechnologies.orient.core.id.ORID) srcRID);
                OVertex dstVertex = db.load((com.orientechnologies.orient.core.id.ORID) dstRID);
                OEdge edge = srcVertex.addEdge(dstVertex, EDGE_CLASS);
                if (doLoadProperty) {
                    for (Map.Entry<String, String> e : properties.entrySet()) {
                        Class<?> targetType = metadata.getEdgePropertyType(e.getKey());
                        Object value = TypeConverter.convertFromString(e.getValue(), targetType);
                        if (value != null) {
                            edge.setProperty(e.getKey(), value);
                        }
                    }
                }
                edge.save();
                edgeCount[0]++;
                opsInTx[0]++;
                if (opsInTx[0] >= BATCH_SIZE) {
                    db.commit();
                    inTransaction[0] = false;
                    opsInTx[0] = 0;
                }
            }
        );

        if (inTransaction[0]) {
            db.commit();
        }

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
     * Should be called after loading data.
     */
    public void createPropertyIndexes(CsvGraphReader.CsvMetadata metadata) {
        for (String prop : metadata.getNodePropertyHeaders()) {
            Class<?> type = metadata.getNodePropertyType(prop);
            progressCallback.sendLogMessage("▶️ Creating vertex property index: " + prop + " (" + type.getSimpleName() + ")", "INFO");
            try {
                OClass vertexClass = db.getClass(VERTEX_CLASS);
                if (vertexClass != null && vertexClass.getProperty(prop) == null) {
                    OType oType = getOrientDBType(type);
                    vertexClass.createProperty(prop, oType);
                }
                db.command("CREATE INDEX " + VERTEX_CLASS + "." + prop + " IF NOT EXISTS ON " + VERTEX_CLASS + " (" + prop + ") NOTUNIQUE");
                progressCallback.sendLogMessage("✅ Created vertex property index: " + prop, "INFO");
            } catch (Exception e) {
                progressCallback.sendLogMessage("Failed to create vertex index for " + prop + ": " + e.getMessage(), "WARNING");
            }
        }
        for (String prop : metadata.getEdgePropertyHeaders()) {
            Class<?> type = metadata.getEdgePropertyType(prop);
            progressCallback.sendLogMessage("▶️ Creating edge property index: " + prop + " (" + type.getSimpleName() + ")", "INFO");
            try {
                OClass edgeClass = db.getClass(EDGE_CLASS);
                if (edgeClass != null && edgeClass.getProperty(prop) == null) {
                    OType oType = getOrientDBType(type);
                    edgeClass.createProperty(prop, oType);
                }
                db.command("CREATE INDEX " + EDGE_CLASS + "." + prop + " IF NOT EXISTS ON " + EDGE_CLASS + " (" + prop + ") NOTUNIQUE");
                progressCallback.sendLogMessage("✅ Created edge property index: " + prop, "INFO");
            } catch (Exception e) {
                progressCallback.sendLogMessage("Failed to create edge index for " + prop + ": " + e.getMessage(), "WARNING");
            }
        }
        progressCallback.sendLogMessage("✅ All property indexes created", "INFO");
    }

    /**
     * Convert Java type to OrientDB OType.
     */
    private OType getOrientDBType(Class<?> javaType) {
        if (javaType == String.class) return OType.STRING;
        if (javaType == Integer.class || javaType == int.class) return OType.INTEGER;
        if (javaType == Long.class || javaType == long.class) return OType.LONG;
        if (javaType == Double.class || javaType == double.class) return OType.DOUBLE;
        if (javaType == Float.class || javaType == float.class) return OType.FLOAT;
        if (javaType == Boolean.class || javaType == boolean.class) return OType.BOOLEAN;
        return OType.STRING; // Default fallback
    }
}