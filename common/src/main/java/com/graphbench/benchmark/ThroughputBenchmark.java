package com.graphbench.benchmark;

import com.graphbench.compiler.CompiledQuery;
import com.graphbench.compiler.CompiledWorkload;
import com.graphbench.db.DatabaseAdapter;
import com.graphbench.workload.Operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi-threaded throughput benchmark.
 *
 * Partitions queries across a fixed thread pool and measures aggregate
 * operations per second over the total wall-clock time.
 */
public class ThroughputBenchmark {

    /**
     * Runs the throughput benchmark for the given compiled workload.
     */
    public BenchmarkResult run(CompiledWorkload compiled, DatabaseAdapter db) {
        List<CompiledQuery> queries = compiled.getQueries();
        int totalOps = queries.size();
        int threadCount = compiled.getClientThreads();

        // Partition queries into chunks for each thread
        List<List<CompiledQuery>> partitions = partition(queries, threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger completedOps = new AtomicInteger(0);

        long wallStart = System.nanoTime();

        List<Future<?>> futures = new ArrayList<>(threadCount);
        for (List<CompiledQuery> chunk : partitions) {
            futures.add(executor.submit(() -> {
                for (CompiledQuery cq : chunk) {
                    try {
                        db.executeQuery(cq.getQuery());
                    } catch (Exception e) {
                        System.err.println("Warning: query failed: " + e.getMessage());
                    }
                    completedOps.incrementAndGet();
                }
            }));
        }

        // Wait for all threads to complete
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Thread execution error: " + e.getMessage());
            }
        }

        long wallEnd = System.nanoTime();
        executor.shutdown();

        double durationSeconds = (wallEnd - wallStart) / 1e9;
        double opsPerSecond = completedOps.get() / durationSeconds;

        BenchmarkResult result = BenchmarkResult.success(compiled.getTaskName(), durationSeconds);
        result.setTotalOps(completedOps.get());
        result.setClientThreads(threadCount);
        result.setThroughput(new BenchmarkResult.ThroughputStats(opsPerSecond));

        // Build operation breakdown for mixed workloads
        Map<String, Integer> breakdown = buildBreakdown(queries);
        if (breakdown.size() > 1) {
            result.setOperationBreakdown(breakdown);
        }

        return result;
    }

    /**
     * Partitions a list into roughly equal-sized sublists.
     */
    private <T> List<List<T>> partition(List<T> list, int n) {
        List<List<T>> partitions = new ArrayList<>(n);
        int size = list.size();
        int chunkSize = (size + n - 1) / n;
        for (int i = 0; i < size; i += chunkSize) {
            partitions.add(list.subList(i, Math.min(i + chunkSize, size)));
        }
        return partitions;
    }

    /**
     * Builds a count of operations by type.
     */
    private Map<String, Integer> buildBreakdown(List<CompiledQuery> queries) {
        Map<String, Integer> breakdown = new HashMap<>();
        for (CompiledQuery cq : queries) {
            Operation op = cq.getOperationType();
            String key = op.name().toLowerCase();
            breakdown.merge(key, 1, Integer::sum);
        }
        return breakdown;
    }
}
