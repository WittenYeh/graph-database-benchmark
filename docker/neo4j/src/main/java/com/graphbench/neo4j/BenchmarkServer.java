package com.graphbench.neo4j;

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

public class BenchmarkServer {
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("API_PORT", "50080"));
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static Neo4jBenchmarkExecutor executor;

    public static void main(String[] args) throws IOException {
        System.out.println("Starting Neo4j Benchmark Server on port " + PORT);

        // Initialize executor
        executor = new Neo4jBenchmarkExecutor();

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
            executor.shutdown();
        }));
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
                int serverThreads = ((Double) request.getOrDefault("server_threads", 8.0)).intValue();
                String callbackUrl = (String) request.get("callback_url");

                System.out.println("Executing benchmark for dataset: " + datasetName);

                // Execute benchmark
                Map<String, Object> results = executor.executeBenchmark(datasetPath, serverThreads, callbackUrl);

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
