package com.graphbench.compiler;

import com.graphbench.config.DatabaseInstanceConfig;

/**
 * Factory for creating workload compilers based on query language.
 *
 * <p>This factory implements a <b>language-first design</b> where compilers are organized
 * by query language (Cypher, Gremlin, AQL, SQL) rather than by specific database products.
 * This design enables code reuse across multiple databases that share the same query language.
 *
 * <h3>Language-to-Database Mapping:</h3>
 * <ul>
 *   <li><b>Cypher</b> → Neo4j, Memgraph, RedisGraph, AgensGraph</li>
 *   <li><b>Gremlin</b> → JanusGraph, TinkerGraph, Amazon Neptune, Azure Cosmos DB</li>
 *   <li><b>AQL</b> → ArangoDB</li>
 *   <li><b>SQL</b> → OrientDB (graph extensions)</li>
 * </ul>
 *
 * <h3>Design Rationale:</h3>
 * <p>By abstracting compilers at the query language level, we achieve:
 * <ul>
 *   <li>Code reuse: One compiler serves multiple databases</li>
 *   <li>Extensibility: New databases can leverage existing compilers</li>
 *   <li>Maintainability: Query logic centralized per language</li>
 *   <li>Flexibility: Database-specific optimizations can be added via subclassing if needed</li>
 * </ul>
 *
 * <h3>Configuration-Driven Routing:</h3>
 * <p>The factory reads the {@code query_language} field from {@link DatabaseInstanceConfig}
 * to determine which compiler to instantiate. Database-specific schema details (labels,
 * property names, etc.) are passed to the compiler during initialization.
 *
 * @see WorkloadCompiler
 * @see CypherCompiler
 * @see GremlinCompiler
 */
public class CompilerFactory {

    /**
     * Create a compiler for the specified database based on its query language.
     *
     * <p>This method routes to language-specific compilers (not database-specific),
     * allowing multiple databases to share the same compiler implementation.
     *
     * <p><b>Example:</b> Both Neo4j and Memgraph use {@code query_language: "cypher"}
     * in their configuration, so both will receive a {@link CypherCompiler} instance.
     * The compiler is then initialized with database-specific schema configuration
     * (node labels, edge types, property names) to generate appropriate queries.
     *
     * @param databaseName Name of the database (used for logging/debugging)
     * @param config Database configuration containing query_language and schema details
     * @return WorkloadCompiler instance initialized with database-specific configuration
     * @throws IllegalArgumentException if query_language is missing or unsupported
     */
    public static WorkloadCompiler create(String databaseName, DatabaseInstanceConfig config) {
        String queryLanguage = config.getQueryLanguage();
        if (queryLanguage == null) {
            throw new IllegalArgumentException("Query language not specified for database: " + databaseName);
        }

        WorkloadCompiler compiler;
        switch (queryLanguage.toLowerCase()) {
            case "cypher":
                // CypherCompiler can be used by Neo4j, Memgraph, RedisGraph, etc.
                compiler = new CypherCompiler();
                break;
            case "gremlin":
                // GremlinCompiler can be used by JanusGraph, TinkerGraph, Amazon Neptune, etc.
                compiler = new GremlinCompiler();
                break;
            case "aql":
                compiler = createCompiler("com.graphbench.compiler.ArangoCompiler");
                break;
            case "sql":
                compiler = createCompiler("com.graphbench.compiler.OrientDBCompiler");
                break;
            default:
                throw new IllegalArgumentException("Unsupported query language: " + queryLanguage);
        }

        compiler.initialize(databaseName, config);
        return compiler;
    }

    private static WorkloadCompiler createCompiler(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (WorkloadCompiler) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to instantiate compiler: " + className, e);
        }
    }
}
