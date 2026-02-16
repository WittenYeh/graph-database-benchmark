package com.graphbench.janusgraph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.graphbench.api.WorkloadDispatcher;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

public class BenchmarkServer {
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("API_PORT", "50081"));
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) throws IOException {
        System.out.println("Starting JanusGraph Benchmark Server on port " + PORT);

        // Clean up old database directory on startup
        cleanupOldDatabase();

        // Create HTTP server
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
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

    private static void cleanupOldDatabase() {
        String dbPath = "/tmp/janusgraph-benchmark-db";
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

    private static void deleteDirectory(File directory) throws IOException {
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

    static class ExecuteHandler implements HttpHandler {
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
                JanusGraphBenchmarkExecutor executor = new JanusGraphBenchmarkExecutor();
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
}
