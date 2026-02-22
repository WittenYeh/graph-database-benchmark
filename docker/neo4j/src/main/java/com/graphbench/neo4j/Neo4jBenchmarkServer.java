package com.graphbench.neo4j;

import com.graphbench.api.BenchmarkServer;

/**
 * Neo4j Benchmark Server main class.
 * Entry point for Neo4j benchmark execution.
 */
public class Neo4jBenchmarkServer {
    private static final String DB_PATH = "/tmp/neo4j-benchmark-db";
    private static final int DEFAULT_PORT = 50080;

    public static void main(String[] args) throws Exception {
        String dbType = System.getenv().getOrDefault("DB_TYPE", "neo4j");
        int port = Integer.parseInt(System.getenv().getOrDefault("API_PORT", String.valueOf(DEFAULT_PORT)));

        // Determine which executor to use based on DB_TYPE
        String displayName;
        Class<?> executorClass;

        if ("neo4j-property".equals(dbType.toLowerCase())) {
            displayName = "Neo4j (Property)";
            executorClass = Neo4jPropertyBenchmarkExecutor.class;
        } else {
            displayName = "Neo4j";
            executorClass = Neo4jBenchmarkExecutor.class;
        }

        // Create executor supplier
        BenchmarkServer server = new BenchmarkServer(
            port,
            displayName,
            DB_PATH,
            () -> {
                try {
                    return (com.graphbench.api.BenchmarkExecutor) executorClass.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create executor: " + e.getMessage(), e);
                }
            }
        );

        server.start();
    }
}