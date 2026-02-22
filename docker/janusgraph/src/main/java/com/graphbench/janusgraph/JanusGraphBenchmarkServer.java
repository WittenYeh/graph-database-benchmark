package com.graphbench.janusgraph;

import com.graphbench.api.BenchmarkServer;

/**
 * JanusGraph Benchmark Server main class.
 * Entry point for JanusGraph benchmark execution.
 */
public class JanusGraphBenchmarkServer {
    private static final String DB_PATH = "/tmp/janusgraph-benchmark-db";
    private static final int DEFAULT_PORT = 50081;

    public static void main(String[] args) throws Exception {
        String dbType = System.getenv().getOrDefault("DB_TYPE", "janusgraph");
        int port = Integer.parseInt(System.getenv().getOrDefault("API_PORT", String.valueOf(DEFAULT_PORT)));

        // Determine which executor to use based on DB_TYPE
        String displayName;
        Class<?> executorClass;

        if ("janusgraph-property".equals(dbType.toLowerCase())) {
            displayName = "JanusGraph (Property)";
            executorClass = JanusGraphPropertyBenchmarkExecutor.class;
        } else {
            displayName = "JanusGraph";
            executorClass = JanusGraphBenchmarkExecutor.class;
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