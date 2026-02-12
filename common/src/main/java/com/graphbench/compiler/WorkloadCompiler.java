package com.graphbench.compiler;

import com.graphbench.config.DatabaseInstanceConfig;
import com.graphbench.workload.Workload;
import com.graphbench.workload.WorkloadOperation;

/**
 * Interface for compiling abstract workload operations into query language-specific strings.
 *
 * <p>This interface defines the contract for all query language compilers in the benchmark
 * framework. Implementations are <b>language-specific</b> (not database-specific), enabling
 * code reuse across multiple databases that share the same query language.
 *
 * <h3>Design Philosophy:</h3>
 * <p>The compiler abstraction separates the logical workload definition from the physical
 * query representation. This allows:
 * <ul>
 *   <li>Database-agnostic workload generation</li>
 *   <li>Query language reuse across compatible databases</li>
 *   <li>Consistent semantic behavior across different query languages</li>
 *   <li>Easy addition of new query languages without modifying workload logic</li>
 * </ul>
 *
 * <h3>Implementation Guidelines:</h3>
 * <p>When implementing this interface for a new query language:
 * <ol>
 *   <li>Ensure all 6 operations are supported: ADD_NODE, ADD_EDGE, DELETE_NODE,
 *       DELETE_EDGE, READ_NBRS, LOAD_GRAPH</li>
 *   <li>Implement undirected graph semantics (edges traversable in both directions)</li>
 *   <li>Handle missing nodes/edges gracefully without throwing errors</li>
 *   <li>Load schema configuration (labels, property names) from DatabaseInstanceConfig</li>
 *   <li>Generate queries that are semantically consistent with existing compilers</li>
 * </ol>
 *
 * <h3>Lifecycle:</h3>
 * <pre>
 * 1. Instantiate compiler via CompilerFactory
 * 2. Call initialize(config) to load schema configuration
 * 3. Call compile(workload) to generate CompiledWorkload
 * 4. Execute queries via DatabaseAdapter
 * </pre>
 *
 * @see CompilerFactory
 * @see CypherCompiler
 * @see GremlinCompiler
 * @see CompiledWorkload
 */
public interface WorkloadCompiler {

    /**
     * Initialize the compiler with database name and configuration.
     *
     * <p>This method loads schema details (node labels, edge types, property names)
     * from the configuration and prepares the compiler for query generation.
     * Must be called before any compile() methods.
     *
     * @param databaseName Name of the database (e.g., "neo4j", "janusgraph", "memgraph")
     * @param config Database configuration containing schema and query language details
     */
    void initialize(String databaseName, DatabaseInstanceConfig config);

    /**
     * Returns the target query language for this compiler.
     *
     * @return QueryLanguage enum (CYPHER, GREMLIN, etc.)
     */
    QueryLanguage getTargetLanguage();

    /**
     * Returns the target database name.
     *
     * <p>For language-specific compilers, this returns the database name from config.
     * This allows the same compiler to be used by multiple databases.
     *
     * @return Database name (e.g., "neo4j", "janusgraph", "memgraph")
     */
    String getDatabaseName();

    /**
     * Compiles a single WorkloadOperation into a query string.
     *
     * <p>Converts an abstract operation (ADD_NODE, ADD_EDGE, etc.) with parameters
     * (nodeId, srcId, dstId) into a concrete query string in the target language.
     *
     * @param operation Abstract workload operation to compile
     * @return Query string in the target language
     * @throws IllegalArgumentException if operation type is unsupported
     */
    String compile(WorkloadOperation operation);

    /**
     * Compiles an entire Workload into a CompiledWorkload.
     *
     * <p>Iterates through all operations in the workload, compiles each to a query string,
     * and packages them into a CompiledWorkload with metadata (task name, database,
     * query language, operation counts, etc.).
     *
     * @param workload Abstract workload containing multiple operations
     * @return CompiledWorkload ready for execution
     */
    CompiledWorkload compile(Workload workload);
}
