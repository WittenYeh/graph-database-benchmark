package com.graphbench.db.janusgraph;

import com.graphbench.config.DatabaseInstanceConfig;
import com.graphbench.db.DatabaseAdapter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * JanusGraph embedded database adapter using BerkeleyJE storage backend.
 *
 * Uses the TinkerPop Gremlin API for typed operations and
 * GremlinGroovyScriptEngine for compiled query string execution.
 *
 * Supports bulk loading mode for optimized large-scale data ingestion.
 */
public class JanusGraphAdapter implements DatabaseAdapter {

    private String vertexLabel;
    private String edgeLabel;
    private String propertyName;
    private String indexName;
    private int batchSize;

    // Bulk loading configuration
    private boolean bulkLoadingEnabled;
    private int idsBlockSize;
    private int idsAuthorityWaitTime;
    private int idsRenewTimeout;

    private JanusGraph graph;
    private GraphTraversalSource g;
    private ScriptEngine scriptEngine;
    private String storagePath;
    private DatabaseInstanceConfig config;

    @Override
    public void open(String storagePath, DatabaseInstanceConfig config) {
        this.storagePath = storagePath;
        this.config = config;

        // Load configuration
        this.vertexLabel = config.getSchemaString("vertex_label");
        this.edgeLabel = config.getSchemaString("edge_label");
        this.propertyName = config.getSchemaString("property_name");
        this.indexName = config.getSchemaString("index_name");
        this.batchSize = config.getPerformanceInt("batch_size");

        // Load bulk loading configuration
        this.bulkLoadingEnabled = getBulkLoadingBoolean("enabled", false);
        this.idsBlockSize = getBulkLoadingInt("ids_block_size", 100000);
        this.idsAuthorityWaitTime = getBulkLoadingInt("ids_authority_wait_time", 300);
        this.idsRenewTimeout = getBulkLoadingInt("ids_renew_timeout", 120000);

        String storageBackend = config.getStorageString("backend");
        String schemaDefault = config.getSchemaString("default_mode");

        // Build graph with bulk loading configuration
        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.backend", storageBackend)
                .set("storage.directory", storagePath)
                .set("schema.default", schemaDefault)
                .set("storage.batch-loading", bulkLoadingEnabled)  // Set bulk loading based on config
                .set("ids.block-size", idsBlockSize)
                .set("ids.authority.wait-time", idsAuthorityWaitTime)
                .set("ids.renew-timeout", idsRenewTimeout);

        // Add storage optimization settings
        Integer bufferSize = config.getStorageInt("buffer_size");
        if (bufferSize != null) {
            builder.set("storage.buffer-size", bufferSize);
        }
        Integer writeAttempts = config.getStorageInt("write_attempts");
        if (writeAttempts != null) {
            builder.set("storage.write-attempts", writeAttempts);
        }
        Integer readAttempts = config.getStorageInt("read_attempts");
        if (readAttempts != null) {
            builder.set("storage.read-attempts", readAttempts);
        }
        Integer attemptWait = config.getStorageInt("attempt_wait");
        if (attemptWait != null) {
            builder.set("storage.attempt-wait", attemptWait);
        }

        graph = builder.open();

        // Create schema: property with composite index
        JanusGraphManagement mgmt = graph.openManagement();

        // Always create schema (database is fresh each time)
        PropertyKey nodeIdKey = mgmt.makePropertyKey(propertyName).dataType(Long.class).make();
        mgmt.buildIndex(indexName, Vertex.class).addKey(nodeIdKey).buildCompositeIndex();
        mgmt.makeVertexLabel(vertexLabel).make();
        mgmt.makeEdgeLabel(edgeLabel).make();
        mgmt.commit();

        if (bulkLoadingEnabled) {
            System.out.println("Schema and index created successfully (bulk loading mode enabled)");
        } else {
            System.out.println("Schema and index created successfully");
        }

        // Note: Index is automatically ENABLED after creation in embedded mode
        // No need to wait for REGISTERED status

        g = graph.traversal();

        // Initialize Groovy script engine for compiled query execution
        ScriptEngineManager manager = new ScriptEngineManager();
        scriptEngine = manager.getEngineByName("groovy");
    }

    @Override
    public void close() {
        if (g != null) {
            try {
                g.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }
        if (graph != null && graph.isOpen()) {
            graph.close();
        }
    }

    @Override
    public String getName() {
        return "janusgraph";
    }

    @Override
    public void addNode(long nodeId) {
        g.addV(vertexLabel).property(propertyName, nodeId).iterate();
        graph.tx().commit();
    }

    @Override
    public void addEdge(long srcId, long dstId) {
        Vertex src = g.V().has(propertyName, srcId).next();
        Vertex dst = g.V().has(propertyName, dstId).next();
        g.V(src).addE(edgeLabel).to(dst).iterate();
        graph.tx().commit();
    }

    @Override
    public void deleteNode(long nodeId) {
        // Check if node exists before attempting to delete
        if (g.V().has(propertyName, nodeId).hasNext()) {
            g.V().has(propertyName, nodeId).drop().iterate();
            graph.tx().commit();
        } else {
            // Node doesn't exist, just commit empty transaction
            graph.tx().commit();
        }
    }

    @Override
    public void deleteEdge(long srcId, long dstId) {
        // Check if edge exists before attempting to delete
        if (g.V().has(propertyName, srcId).bothE(edgeLabel)
                .where(org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
                        .otherV().has(propertyName, dstId))
                .hasNext()) {
            g.V().has(propertyName, srcId).bothE(edgeLabel)
                    .where(org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
                            .otherV().has(propertyName, dstId))
                    .drop().iterate();
            graph.tx().commit();
        } else {
            // Edge doesn't exist, just commit empty transaction
            graph.tx().commit();
        }
    }

    @Override
    public List<Long> readNeighbors(long nodeId) {
        List<Long> neighbors = new ArrayList<>();
        List<Object> results = g.V().has(propertyName, nodeId)
                .both().values(propertyName).toList();
        for (Object obj : results) {
            neighbors.add(((Number) obj).longValue());
        }
        return neighbors;
    }

    @Override
    public void loadGraph(List<long[]> edges, int numNodes) {
        if (bulkLoadingEnabled) {
            System.out.println("Using bulk loading mode for graph ingestion...");
        }

        // Batch-insert nodes
        System.out.println("Inserting " + numNodes + " nodes...");
        for (int i = 1; i <= numNodes; i += batchSize) {
            int end = Math.min(i + batchSize, numNodes + 1);
            for (int id = i; id < end; id++) {
                g.addV(vertexLabel).property(propertyName, (long) id).iterate();
            }
            graph.tx().commit();

            // Progress reporting for large datasets
            if (bulkLoadingEnabled && i % (batchSize * 10) == 1) {
                System.out.println("  Inserted " + Math.min(i + batchSize - 1, numNodes) + "/" + numNodes + " nodes");
            }
        }

        // Batch-insert edges
        System.out.println("Inserting " + edges.size() + " edges...");
        for (int i = 0; i < edges.size(); i += batchSize) {
            int end = Math.min(i + batchSize, edges.size());
            for (int j = i; j < end; j++) {
                long[] edge = edges.get(j);
                Iterator<Vertex> srcIter = g.V().has(propertyName, edge[0]);
                Iterator<Vertex> dstIter = g.V().has(propertyName, edge[1]);
                if (srcIter.hasNext() && dstIter.hasNext()) {
                    Vertex src = srcIter.next();
                    Vertex dst = dstIter.next();
                    g.V(src).addE(edgeLabel).to(dst).iterate();
                }
            }
            graph.tx().commit();

            // Progress reporting for large datasets
            if (bulkLoadingEnabled && i % (batchSize * 10) == 0) {
                System.out.println("  Inserted " + Math.min(i + batchSize, edges.size()) + "/" + edges.size() + " edges");
            }
        }

        if (bulkLoadingEnabled) {
            System.out.println("Bulk loading complete");
        }
    }

    /**
     * Helper method to get bulk loading boolean configuration.
     */
    private boolean getBulkLoadingBoolean(String key, boolean defaultValue) {
        if (config.getPerformance() == null) {
            return defaultValue;
        }
        Object bulkLoading = config.getPerformance().get("bulk_loading");
        if (bulkLoading instanceof java.util.Map) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> bulkLoadingMap = (java.util.Map<String, Object>) bulkLoading;
            Object value = bulkLoadingMap.get(key);
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
        }
        return defaultValue;
    }

    /**
     * Helper method to get bulk loading integer configuration.
     */
    private int getBulkLoadingInt(String key, int defaultValue) {
        if (config.getPerformance() == null) {
            return defaultValue;
        }
        Object bulkLoading = config.getPerformance().get("bulk_loading");
        if (bulkLoading instanceof java.util.Map) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> bulkLoadingMap = (java.util.Map<String, Object>) bulkLoading;
            Object value = bulkLoadingMap.get(key);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
        }
        return defaultValue;
    }

    @Override
    public void executeQuery(String query) {
        if (scriptEngine == null) {
            throw new IllegalStateException("Script engine not initialized. Call open() first.");
        }
        try {
            Bindings bindings = scriptEngine.createBindings();
            bindings.put("g", g);
            scriptEngine.eval(query, bindings);
            graph.tx().commit();
        } catch (Exception e) {
            // Silently ignore errors (typically from delete operations on non-existent elements)
            // Just commit the transaction and continue
            try {
                graph.tx().commit();
            } catch (Exception ignored) {
                // If commit fails, ignore it
            }
        }
    }
}
