package com.graphbench.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Generic HTTP server for benchmark execution.
 * Can be used by any database implementation by providing an executor supplier.
 */
public class BenchmarkServer {
    private final int port;
    private final String databaseName;
    private final String dbPath;
    private final Supplier<BenchmarkExecutor> executorSupplier;
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    // Registry of database executors
    private static final Map<String, DatabaseConfig> DATABASE_REGISTRY = new HashMap<>();

    static {
        // Structural executors
        DATABASE_REGISTRY.put("neo4j", new DatabaseConfig(
            "Neo4j",
            "/tmp/neo4j-benchmark-db",
            50080,
            "com.graphbench.neo4j.Neo4jBenchmarkExecutor"
        ));
        DATABASE_REGISTRY.put("janusgraph", new DatabaseConfig(
            "JanusGraph",
            "/tmp/janusgraph-benchmark-db",
            50081,
            "com.graphbench.janusgraph.JanusGraphBenchmarkExecutor"
        ));

        // Property executors
        DATABASE_REGISTRY.put("neo4j-property", new DatabaseConfig(
            "Neo4j (Property)",
            "/tmp/neo4j-benchmark-db",
            50080,
            "com.graphbench.neo4j.Neo4jPropertyBenchmarkExecutor"
        ));
        DATABASE_REGISTRY.put("janusgraph-property", new DatabaseConfig(
            "JanusGraph (Property)",
            "/tmp/janusgraph-benchmark-db",
            50081,
            "com.graphbench.janusgraph.JanusGraphPropertyBenchmarkExecutor"
        ));
    }

    public BenchmarkServer(int port, String databaseName, String dbPath,
                          Supplier<BenchmarkExecutor> executorSupplier) {
        this.port = port;
        this.databaseName = databaseName;
        this.dbPath = dbPath;
        this.executorSupplier = executorSupplier;
    }

    public static void main(String[] args) throws Exception {
        String dbType = System.getenv().getOrDefault("DB_TYPE", "neo4j");
        DatabaseConfig config = DATABASE_REGISTRY.get(dbType.toLowerCase());

        if (config == null) {
            System.err.println("Unknown database type: " + dbType);
            System.err.println("Available types: " + DATABASE_REGISTRY.keySet());
            System.exit(1);
        }

        int port = Integer.parseInt(System.getenv().getOrDefault("API_PORT", String.valueOf(config.defaultPort)));

        // Create executor supplier using reflection
        Supplier<BenchmarkExecutor> executorSupplier = () -> {
            try {
                Class<?> clazz = Class.forName(config.executorClassName);
                return (BenchmarkExecutor) clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create executor: " + e.getMessage(), e);
            }
        };

        BenchmarkServer server = new BenchmarkServer(port, config.displayName, config.dbPath, executorSupplier);
        server.start();
    }

    public void start() throws IOException {
        System.out.println("Starting " + databaseName + " Benchmark Server on port " + port);

        // Clean up old database directory on startup
        cleanupOldDatabase();

        // Create HTTP server
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", new HealthHandler());
        server.createContext("/execute", new ExecuteHandler());
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();

        System.out.println("Server started successfully");

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            server.stop(0);
        }));
    }

    private void cleanupOldDatabase() {
        File dbDir = new File(dbPath);
        if (dbDir.exists()) {
            System.out.println("Cleaning up old database directory: " + dbPath);
            try {
                deleteDirectory(dbDir);
                System.out.println("Old database cleaned up successfully");
            } catch (IOException e) {
                System.err.println("Warning: Failed to clean up old database: " + e.getMessage());
            }
        }
    }

    private void deleteDirectory(File directory) throws IOException {
        if (directory.exists()) {
            java.nio.file.Files.walk(directory.toPath())
                .sorted(java.util.Comparator.reverseOrder())
                .map(java.nio.file.Path::toFile)
                .forEach(File::delete);
        }
    }

    static class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response = "{\"status\": \"ok\"}";
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    class ExecuteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendError(exchange, 405, "Method not allowed");
                return;
            }

            try {
                // Parse request
                String requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Map<String, Object> request = gson.fromJson(requestBody, Map.class);

                String datasetName = (String) request.get("dataset_name");
                String datasetPath = (String) request.get("dataset_path");

                System.out.println("Executing benchmark for dataset: " + datasetName);

                // Create executor and dispatcher
                BenchmarkExecutor executor = executorSupplier.get();
                WorkloadDispatcher dispatcher = new WorkloadDispatcher(executor, datasetPath);
                Map<String, Object> results = dispatcher.executeBenchmark("/data/workloads");

                // Send response
                String response = gson.toJson(results);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }

            } catch (Exception e) {
                e.printStackTrace();
                sendError(exchange, 500, "Execution failed: " + e.getMessage());
            }
        }

        private void sendError(HttpExchange exchange, int code, String message) throws IOException {
            String response = gson.toJson(Map.of("error", message));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(code, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    static class DatabaseConfig {
        final String displayName;
        final String dbPath;
        final int defaultPort;
        final String executorClassName;

        DatabaseConfig(String displayName, String dbPath, int defaultPort, String executorClassName) {
            this.displayName = displayName;
            this.dbPath = dbPath;
            this.defaultPort = defaultPort;
            this.executorClassName = executorClassName;
        }
    }
}

