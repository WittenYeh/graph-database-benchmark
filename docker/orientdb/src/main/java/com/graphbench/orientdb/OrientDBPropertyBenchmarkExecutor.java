package com.graphbench.orientdb;

import com.graphbench.api.PropertyBenchmarkExecutor;
import com.graphbench.workload.*;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.*;

/**
 * OrientDB property benchmark executor.
 * Loads graph with all CSV properties stored on nodes/edges, creates indexes dynamically.
 */
public class OrientDBPropertyBenchmarkExecutor extends OrientDBBenchmarkExecutor
        implements PropertyBenchmarkExecutor {

    @Override
    public String getDatabaseName() { return "orientdb-property"; }

    @Override
    public Map<String, Object> loadGraph(String datasetPath) throws Exception {
        OrientDBGraphLoader loader = new OrientDBGraphLoader(db, progressCallback, true);
        Map<String, Object> result = loader.load(datasetPath);
        nodeIdsMap = loader.getNodeIdsMap();
        loader.createPropertyIndexes(loader.getMetadata());
        return result;
    }

    // --- Property operations ---

    @Override
    public List<Double> updateVertexProperty(List<UpdateVertexPropertyParams.VertexUpdate> updates, int batchSize) {
        return transactionalBatchExecute(updates, update -> {
            OVertex vertex = db.load((com.orientechnologies.orient.core.id.ORID) update.getSystemId());
            if (vertex != null) {
                for (Map.Entry<String, Object> e : update.getProperties().entrySet()) {
                    vertex.setProperty(e.getKey(), e.getValue());
                }
                vertex.save();
            }
        }, batchSize);
    }

    @Override
    public List<Double> updateEdgeProperty(String label, List<UpdateEdgePropertyParams.EdgeUpdate> updates, int batchSize) {
        return transactionalBatchExecute(updates, update -> {
            OVertex srcVertex = db.load((com.orientechnologies.orient.core.id.ORID) update.getSrcSystemId());
            OVertex dstVertex = db.load((com.orientechnologies.orient.core.id.ORID) update.getDstSystemId());
            if (srcVertex != null && dstVertex != null) {
                Iterable<OEdge> edges = srcVertex.getEdges(ODirection.OUT, EDGE_CLASS);
                for (OEdge edge : edges) {
                    if (edge.getTo().equals(dstVertex)) {
                        for (Map.Entry<String, Object> e : update.getProperties().entrySet()) {
                            edge.setProperty(e.getKey(), e.getValue());
                        }
                        edge.save();
                        break;
                    }
                }
            }
        }, batchSize);
    }

    @Override
    public List<Double> getVertexByProperty(List<GetVertexByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalBatchExecute(queries, query -> {
            String sql = "SELECT FROM " + VERTEX_CLASS + " WHERE " + query.getKey() + " = ?";
            try (OResultSet rs = db.query(sql, query.getValue())) {
                rs.stream().forEachOrdered(blackhole::consume);
            }
        }, batchSize);
    }

    @Override
    public List<Double> getEdgeByProperty(List<GetEdgeByPropertyParams.PropertyQuery> queries, int batchSize) {
        return transactionalBatchExecute(queries, query -> {
            String sql = "SELECT FROM " + EDGE_CLASS + " WHERE " + query.getKey() + " = ?";
            try (OResultSet rs = db.query(sql, query.getValue())) {
                rs.stream().forEachOrdered(blackhole::consume);
            }
        }, batchSize);
    }
}