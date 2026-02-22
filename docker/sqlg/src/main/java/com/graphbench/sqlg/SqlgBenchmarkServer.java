package com.graphbench.sqlg;

import com.graphbench.api.BenchmarkExecutor;
import com.graphbench.api.BenchmarkServer;

import java.util.function.Supplier;

/**
 * SQLG Benchmark Server - Entry point for SQLG benchmarks.
 * Supports two modes:
 * - "sqlg": Structural benchmark (topology only)
 * - "sqlg-property": Property benchmark (with properties)
 */
public class SqlgBenchmarkServer {
    public static void main(String[] args) throws Exception {
        String dbType = System.getenv("DB_TYPE");
        if (dbType == null) {
            dbType = "sqlg";
        }

        int port = Integer.parseInt(System.getenv().getOrDefault("API_PORT", "50085"));
        String dbPath = "/tmp/sqlg-benchmark-db";

        Supplier<BenchmarkExecutor> executorSupplier;
        if ("sqlg-property".equals(dbType)) {
            executorSupplier = SqlgPropertyBenchmarkExecutor::new;
        } else {
            executorSupplier = SqlgBenchmarkExecutor::new;
        }

        BenchmarkServer server = new BenchmarkServer(port, dbType, dbPath, executorSupplier);
        server.start();

        System.out.println("SQLG Benchmark Server started on port " + port);
        System.out.println("Database type: " + dbType);
        System.out.println("Database path: " + dbPath);

        // Keep the server running
        Thread.currentThread().join();
    }
}
