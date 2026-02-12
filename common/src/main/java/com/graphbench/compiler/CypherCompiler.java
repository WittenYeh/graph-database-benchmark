package com.graphbench.compiler;

import com.graphbench.config.DatabaseInstanceConfig;
import com.graphbench.workload.Operation;
import com.graphbench.workload.Workload;
import com.graphbench.workload.WorkloadOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * Compiles abstract workload operations into Cypher queries.
 *
 * <p>This compiler is <b>language-specific</b> (not database-specific) and can be used
 * by any graph database that supports the Cypher query language.
 *
 * <h3>Compatible Databases:</h3>
 * <ul>
 *   <li><b>Neo4j</b> - Native Cypher support</li>
 *   <li><b>Memgraph</b> - Cypher-compatible in-memory graph database</li>
 *   <li><b>RedisGraph</b> - Redis module with Cypher support</li>
 *   <li><b>AgensGraph</b> - PostgreSQL-based graph database with Cypher</li>
 * </ul>
 *
 * <h3>Undirected Graph Semantics:</h3>
 * <p>This compiler generates queries for <b>undirected graphs</b>:
 * <ul>
 *   <li><b>ADD_EDGE:</b> Creates directed edge {@code (a)-[:TYPE]->(b)} for storage efficiency</li>
 *   <li><b>DELETE_EDGE:</b> Uses undirected pattern {@code -[r]-} to match edges in both directions</li>
 *   <li><b>READ_NBRS:</b> Uses undirected pattern {@code --} to traverse edges bidirectionally</li>
 * </ul>
 *
 * <h3>Error Handling:</h3>
 * <p>Cypher's MATCH clause returns an empty result set when nodes or edges are not found,
 * so delete operations never throw errors for missing entities. This provides graceful
 * handling of concurrent deletions or non-existent elements.
 *
 * <h3>Configuration:</h3>
 * <p>Schema details are loaded from {@link DatabaseInstanceConfig}:
 * <ul>
 *   <li>{@code node_label} - Cypher node label (e.g., "Node", "Vertex")</li>
 *   <li>{@code edge_type} - Cypher relationship type (e.g., "EDGE", "LINK")</li>
 *   <li>{@code id_property} - Property name for node IDs (e.g., "id", "nodeId")</li>
 * </ul>
 *
 * @see WorkloadCompiler
 * @see GremlinCompiler
 */
public class CypherCompiler implements WorkloadCompiler {

    private String nodeLabel;
    private String edgeType;
    private String idProperty;
    private String databaseName;

    @Override
    public void initialize(String databaseName, DatabaseInstanceConfig config) {
        this.nodeLabel = config.getSchemaString("node_label");
        this.edgeType = config.getSchemaString("edge_type");
        this.idProperty = config.getSchemaString("id_property");
        this.databaseName = databaseName;
    }

    @Override
    public QueryLanguage getTargetLanguage() {
        return QueryLanguage.CYPHER;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String compile(WorkloadOperation op) {
        switch (op.getType()) {
            case ADD_NODE:
                return String.format("CREATE (n:%s {%s: %d})", nodeLabel, idProperty, op.getNodeId());

            case ADD_EDGE:
                return String.format(
                    "MATCH (a:%s {%s: %d}), (b:%s {%s: %d}) CREATE (a)-[:%s]->(b)",
                    nodeLabel, idProperty, op.getSrcId(),
                    nodeLabel, idProperty, op.getDstId(),
                    edgeType
                );

            case DELETE_NODE:
                // MATCH returns empty if node not found, so no error occurs
                return String.format("MATCH (n:%s {%s: %d}) DETACH DELETE n", nodeLabel, idProperty, op.getNodeId());

            case DELETE_EDGE:
                // Uses undirected pattern to match edges in both directions
                // MATCH returns empty if edge not found, so no error occurs
                return String.format(
                    "MATCH (a:%s {%s: %d})-[r:%s]-(b:%s {%s: %d}) DELETE r",
                    nodeLabel, idProperty, op.getSrcId(),
                    edgeType,
                    nodeLabel, idProperty, op.getDstId()
                );

            case READ_NBRS:
                // Uses undirected pattern to get neighbors in both directions
                return String.format("MATCH (n:%s {%s: %d})--(m) RETURN m.%s", nodeLabel, idProperty, op.getNodeId(), idProperty);

            case LOAD_GRAPH:
                return "LOAD_GRAPH";

            default:
                throw new IllegalArgumentException("Unknown operation type: " + op.getType());
        }
    }

    @Override
    public CompiledWorkload compile(Workload workload) {
        List<CompiledQuery> queries = new ArrayList<>();
        boolean isBulkLoad = false;

        for (WorkloadOperation op : workload.getOperations()) {
            if (op.getType() == Operation.LOAD_GRAPH) {
                isBulkLoad = true;
                break;
            }
            String queryStr = compile(op);
            queries.add(new CompiledQuery(op.getType(), queryStr));
        }

        CompiledWorkload compiled = new CompiledWorkload();
        compiled.setTaskName(workload.getTaskName());
        compiled.setDatabase(getDatabaseName());
        compiled.setQueryLanguage(getTargetLanguage());
        compiled.setTotalOps(workload.getTotalOps());
        compiled.setClientThreads(workload.getClientThreads());
        compiled.setBulkLoad(isBulkLoad);
        compiled.setCopyMode(workload.isCopyMode());
        compiled.setDataset(workload.getDataset());
        compiled.setDatasetPath(workload.getDatasetPath());
        compiled.setQueries(queries);

        return compiled;
    }
}
