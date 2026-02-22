package com.graphbench.orientdb;

import com.graphbench.api.BenchmarkServer;

/**
 * OrientDB Benchmark Server main class.
 * Entry point for OrientDB benchmark execution.
 */
public class OrientDBBenchmarkServer {
    private static final String DB_PATH = "/tmp/orientdb-benchmark-db";
    private static final int DEFAULT_PORT = 50083;

    public static void main(String[] args) throws Exception {
        String dbType = System.getenv().getOrDefault("DB_TYPE", "orientdb");
        int port = Integer.parseInt(System.getenv().getOrDefault("API_PORT", String.valueOf(DEFAULT_PORT)));

        // Determine which executor to use based on DB_TYPE
        String displayName;
        Class<?> executorClass;

        if ("orientdb-property".equals(dbType.toLowerCase())) {
            displayName = "OrientDB (Property)";
            executorClass = OrientDBPropertyBenchmarkExecutor.class;
        } else {
            displayName = "OrientDB";
            executorClass = OrientDBBenchmarkExecutor.class;
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