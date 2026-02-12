package com.graphbench.benchmark;

import com.graphbench.compiler.CompiledQuery;
import com.graphbench.compiler.CompiledWorkload;
import com.graphbench.db.DatabaseAdapter;
import com.graphbench.workload.Operation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single-threaded latency benchmark.
 *
 * Executes each query sequentially and records per-operation latency.
 * Reports min, max, mean, median, p50, p90, p95, p99 in microseconds.
 */
public class LatencyBenchmark {

    /**
     * Runs the latency benchmark for the given compiled workload.
     */
    public BenchmarkResult run(CompiledWorkload compiled, DatabaseAdapter db) {
        List<CompiledQuery> queries = compiled.getQueries();
        int totalOps = queries.size();
        long[] latencies = new long[totalOps]; // nanoseconds per operation

        long wallStart = System.nanoTime();

        for (int i = 0; i < totalOps; i++) {
            CompiledQuery cq = queries.get(i);
            long start = System.nanoTime();
            try {
                db.executeQuery(cq.getQuery());
            } catch (Exception e) {
                // Record the latency even on failure; log and continue
                System.err.println("Warning: query failed at index " + i
                        + ": " + e.getMessage());
            }
            long end = System.nanoTime();
            latencies[i] = end - start;
        }

        long wallEnd = System.nanoTime();
        double durationSeconds = (wallEnd - wallStart) / 1e9;

        // Compute latency statistics
        Arrays.sort(latencies);
        double minUs = latencies[0] / 1000.0;
        double maxUs = latencies[totalOps - 1] / 1000.0;
        double meanUs = Arrays.stream(latencies).average().orElse(0) / 1000.0;
        double medianUs = percentile(latencies, 50) / 1000.0;
        double p90Us = percentile(latencies, 90) / 1000.0;
        double p95Us = percentile(latencies, 95) / 1000.0;
        double p99Us = percentile(latencies, 99) / 1000.0;

        BenchmarkResult result = BenchmarkResult.success(compiled.getTaskName(), durationSeconds);
        result.setTotalOps(totalOps);
        result.setClientThreads(1);
        result.setLatency(new BenchmarkResult.LatencyStats(
                minUs, maxUs, meanUs, medianUs, p90Us, p95Us, p99Us));

        // Build operation breakdown for mixed workloads
        Map<String, Integer> breakdown = buildBreakdown(queries);
        if (breakdown.size() > 1) {
            result.setOperationBreakdown(breakdown);
        }

        return result;
    }

    /**
     * Computes the given percentile from a sorted array of nanosecond values.
     */
    private double percentile(long[] sorted, int pct) {
        int index = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
        index = Math.max(0, Math.min(index, sorted.length - 1));
        return sorted[index];
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
