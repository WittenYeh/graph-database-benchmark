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
 * Generic main class for benchmark servers.
 * Provides HTTP server functionality and executor management.
 */
public class BenchmarkServer {
    private final int port;
    private final String databaseName;
    private final String dbPath;
    private final Supplier<BenchmarkExecutor> executorSupplier;
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public BenchmarkServer(int port, String databaseName, String dbPath,
                           Supplier<BenchmarkExecutor> executorSupplier) {
        this.port = port;
        this.databaseName = databaseName;
        this.dbPath = dbPath;
        this.executorSupplier = executorSupplier;
    }

    public void start() throws IOException {
        System.out.println("Starting " + databaseName + " Benchmark Server on port " + port);

        // Clean up old database directory on startup
        BenchmarkUtils.cleanupOldDatabase(dbPath);

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

    class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response = gson.toJson(Map.of("status", "healthy"));
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
                // Read request body
                String requestBody;
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        sb.append(line);
                    }
                    requestBody = sb.toString();
                }

                // Parse request
                @SuppressWarnings("unchecked")
                Map<String, Object> request = gson.fromJson(requestBody, Map.class);
                String datasetName = (String) request.get("dataset_name");
                String datasetPath = (String) request.get("dataset_path");

                if (datasetPath == null) {
                    sendError(exchange, 400, "Missing required parameter: dataset_path");
                    return;
                }

                // Create executor and dispatcher
                BenchmarkExecutor executor = executorSupplier.get();
                WorkloadDispatcher dispatcher = new WorkloadDispatcher(executor, datasetPath);

                // Send log message via progress callback
                String callbackUrl = (String) request.get("callback_url");
                if (callbackUrl != null) {
                    ProgressCallback callback = new ProgressCallback(callbackUrl);
                    callback.sendLogMessage("Executing benchmark for dataset: " + datasetName, "INFO");
                }

                // Execute benchmark with workload directory (hardcoded to /data/workloads in container)
                Map<String, Object> result = dispatcher.executeBenchmark("/data/workloads");

                // Send response
                String response = gson.toJson(result);
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