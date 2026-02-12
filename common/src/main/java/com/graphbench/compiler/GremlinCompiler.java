package com.graphbench.compiler;

import com.graphbench.config.DatabaseInstanceConfig;
import com.graphbench.workload.Operation;
import com.graphbench.workload.Workload;
import com.graphbench.workload.WorkloadOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * Compiles abstract workload operations into Gremlin queries.
 *
 * <p>This compiler is <b>language-specific</b> (not database-specific) and can be used
 * by any graph database that supports the Apache TinkerPop Gremlin query language.
 *
 * <h3>Compatible Databases:</h3>
 * <ul>
 *   <li><b>JanusGraph</b> - Distributed graph database with Gremlin support</li>
 *   <li><b>TinkerGraph</b> - In-memory reference implementation</li>
 *   <li><b>Amazon Neptune</b> - Fully managed graph database service</li>
 *   <li><b>Azure Cosmos DB</b> - Gremlin API for graph data</li>
 *   <li><b>DataStax Graph</b> - Enterprise graph database</li>
 * </ul>
 *
 * <h3>Undirected Graph Semantics:</h3>
 * <p>This compiler generates queries for <b>undirected graphs</b>:
 * <ul>
 *   <li><b>ADD_EDGE:</b> Creates edge using {@code addE().from()} (stored with direction)</li>
 *   <li><b>DELETE_EDGE:</b> Uses {@code bothE()} to match edges in both directions</li>
 *   <li><b>READ_NBRS:</b> Uses {@code both()} to traverse edges bidirectionally</li>
 * </ul>
 *
 * <h3>Error Handling:</h3>
 * <p>Gremlin's {@code drop()} operation throws an error if the element doesn't exist,
 * so this compiler uses explicit {@code hasNext()} checks before delete operations.
 * This provides graceful handling of concurrent deletions or non-existent elements,
 * matching the semantics of Cypher's MATCH behavior.
 *
 * <h3>Query Execution:</h3>
 * <p>Generated queries are Groovy script strings that will be executed via a
 * {@code ScriptEngine} with the graph traversal source {@code g} bound to the context.
 * All queries use {@code iterate()} or {@code toList()} to force execution.
 *
 * <h3>Configuration:</h3>
 * <p>Schema details are loaded from {@link DatabaseInstanceConfig}:
 * <ul>
 *   <li>{@code vertex_label} - Gremlin vertex label (e.g., "Node", "Vertex")</li>
 *   <li>{@code edge_label} - Gremlin edge label (e.g., "link", "edge")</li>
 *   <li>{@code property_name} - Property name for vertex IDs (e.g., "nodeId", "id")</li>
 * </ul>
 *
 * @see WorkloadCompiler
 * @see CypherCompiler
 */
public class GremlinCompiler implements WorkloadCompiler {

    private String vertexLabel;
    private String edgeLabel;
    private String propertyName;
    private String databaseName;

    @Override
    public void initialize(String databaseName, DatabaseInstanceConfig config) {
        this.vertexLabel = config.getSchemaString("vertex_label");
        this.edgeLabel = config.getSchemaString("edge_label");
        this.propertyName = config.getSchemaString("property_name");
        this.databaseName = databaseName;
    }

    @Override
    public QueryLanguage getTargetLanguage() {
        return QueryLanguage.GREMLIN;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String compile(WorkloadOperation op) {
        switch (op.getType()) {
            case ADD_NODE:
                return String.format("g.addV('%s').property('%s', %dL).iterate()", vertexLabel, propertyName, op.getNodeId());

            case ADD_EDGE:
                return String.format(
                    "g.V().has('%s', %dL).as('a').V().has('%s', %dL).addE('%s').from('a').iterate()",
                    propertyName, op.getSrcId(),
                    propertyName, op.getDstId(),
                    edgeLabel
                );

            case DELETE_NODE:
                // Only delete if node exists (same semantics as Cypher MATCH)
                // Explicit check prevents errors when node doesn't exist
                return String.format(
                    "if (g.V().has('%s', %dL).hasNext()) { g.V().has('%s', %dL).drop().iterate() }",
                    propertyName, op.getNodeId(),
                    propertyName, op.getNodeId()
                );

            case DELETE_EDGE:
                // Only delete if edge exists (same semantics as Cypher MATCH)
                // Uses bothE() to match edges in both directions (undirected)
                // Explicit check prevents errors when edge doesn't exist
                return String.format(
                    "if (g.V().has('%s', %dL).bothE('%s').where(__.otherV().has('%s', %dL)).hasNext()) { " +
                    "g.V().has('%s', %dL).bothE('%s').where(__.otherV().has('%s', %dL)).drop().iterate() }",
                    propertyName, op.getSrcId(),
                    edgeLabel,
                    propertyName, op.getDstId(),
                    propertyName, op.getSrcId(),
                    edgeLabel,
                    propertyName, op.getDstId()
                );

            case READ_NBRS:
                // Uses both() to get neighbors in both directions (undirected)
                return String.format("g.V().has('%s', %dL).both().values('%s').toList()", propertyName, op.getNodeId(), propertyName);

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
