package com.graphbench.benchmark;

import com.graphbench.compiler.CompiledWorkload;
import com.graphbench.dataset.MatrixMarketReader;
import com.graphbench.db.DatabaseAdapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates the execution of compiled workload tasks against a database.
 * Dispatches each task to either LatencyBenchmark or ThroughputBenchmark
 * based on the task name suffix.
 */
public class BenchmarkExecutor {

    private final DatabaseAdapter db;
    private final LatencyBenchmark latencyBench;
    private final ThroughputBenchmark throughputBench;
    private final int serverThreads;

    // Store the initial graph data for copy_mode
    private List<long[]> initialEdges;
    private int initialNumNodes;

    public BenchmarkExecutor(DatabaseAdapter db, int serverThreads) {
        this.db = db;
        this.serverThreads = serverThreads;
        this.latencyBench = new LatencyBenchmark();
        this.throughputBench = new ThroughputBenchmark();
    }

    /**
     * Executes all compiled workloads in order and returns the results.
     *
     * @param compiledWorkloads ordered list of compiled workloads to execute
     * @return list of benchmark results, one per workload
     */
    public List<BenchmarkResult> executeAll(List<CompiledWorkload> compiledWorkloads) {
        List<BenchmarkResult> results = new ArrayList<>();

        for (CompiledWorkload compiled : compiledWorkloads) {
            String taskName = compiled.getTaskName();
            System.out.println("Running task: " + taskName);

            try {
                BenchmarkResult result;

                if (compiled.isBulkLoad()) {
                    result = executeBulkLoad(compiled);
                } else {
                    // If copy_mode is enabled, reload the graph before executing this task
                    if (compiled.isCopyMode() && initialEdges != null) {
                        System.out.println("  Copy mode enabled: reloading graph data...");
                        long reloadStart = System.nanoTime();
                        db.loadGraph(initialEdges, initialNumNodes);
                        long reloadEnd = System.nanoTime();
                        double reloadTime = (reloadEnd - reloadStart) / 1e9;
                        System.out.println("  Graph reloaded in " + String.format("%.3f", reloadTime) + "s");
                    }

                    // Execute the benchmark
                    if (taskName.endsWith("_throughput")) {
                        result = throughputBench.run(compiled, db);
                    } else {
                        // Default to latency mode (includes _latency suffix)
                        result = latencyBench.run(compiled, db);
                    }
                }

                System.out.println("  Completed: " + taskName
                        + " in " + String.format("%.3f", result.getDurationSeconds()) + "s");
                results.add(result);

            } catch (Exception e) {
                System.err.println("  Failed: " + taskName + " - " + e.getMessage());
                results.add(BenchmarkResult.error(taskName, e.getMessage()));
            }
        }

        return results;
    }

    /**
     * Executes a bulk graph load from the dataset .mtx file.
     */
    private BenchmarkResult executeBulkLoad(CompiledWorkload compiled) throws IOException {
        String datasetPath = compiled.getDatasetPath();
        System.out.println("  Loading graph from: " + datasetPath);

        MatrixMarketReader reader = new MatrixMarketReader();
        reader.read(datasetPath);

        // Store the initial graph data for potential copy_mode usage
        this.initialEdges = reader.getEdges();
        this.initialNumNodes = reader.getNumNodes();

        long start = System.nanoTime();
        db.loadGraph(initialEdges, initialNumNodes);
        long end = System.nanoTime();

        double durationSeconds = (end - start) / 1e9;

        BenchmarkResult result = BenchmarkResult.success(compiled.getTaskName(), durationSeconds);
        result.setTotalOps(reader.getNumEdges() + reader.getNumNodes());
        return result;
    }
}
