package com.graphbench.benchmark;

import java.util.Map;

/**
 * Result data for a single benchmark task execution.
 * Contains either latency statistics or throughput metrics depending on the task type.
 */
public class BenchmarkResult {

    private String task;
    private String status;       // "success" or "error"
    private String error;
    private double durationSeconds;
    private int totalOps;
    private int clientThreads;

    // Latency-specific metrics (null for throughput tasks)
    private LatencyStats latency;

    // Throughput-specific metrics (null for latency tasks)
    private ThroughputStats throughput;

    // Operation breakdown for mixed workloads
    private Map<String, Integer> operationBreakdown;

    public BenchmarkResult() {
    }

    public static BenchmarkResult success(String task, double durationSeconds) {
        BenchmarkResult r = new BenchmarkResult();
        r.task = task;
        r.status = "success";
        r.durationSeconds = durationSeconds;
        return r;
    }

    public static BenchmarkResult error(String task, String errorMessage) {
        BenchmarkResult r = new BenchmarkResult();
        r.task = task;
        r.status = "error";
        r.error = errorMessage;
        return r;
    }

    public String getTask() {
        return task;
    }

    public void setTask(String task) {
        this.task = task;
    }

    public String getStatus() {
        return status;
    }

    public String getError() {
        return error;
    }

    public double getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(double durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public int getTotalOps() {
        return totalOps;
    }

    public void setTotalOps(int totalOps) {
        this.totalOps = totalOps;
    }

    public int getClientThreads() {
        return clientThreads;
    }

    public void setClientThreads(int clientThreads) {
        this.clientThreads = clientThreads;
    }

    public LatencyStats getLatency() {
        return latency;
    }

    public void setLatency(LatencyStats latency) {
        this.latency = latency;
    }

    public ThroughputStats getThroughput() {
        return throughput;
    }

    public void setThroughput(ThroughputStats throughput) {
        this.throughput = throughput;
    }

    public Map<String, Integer> getOperationBreakdown() {
        return operationBreakdown;
    }

    public void setOperationBreakdown(Map<String, Integer> operationBreakdown) {
        this.operationBreakdown = operationBreakdown;
    }

    /**
     * Latency statistics in microseconds.
     */
    public static class LatencyStats {
        private double minUs;
        private double maxUs;
        private double meanUs;
        private double medianUs;
        private double p50Us;
        private double p90Us;
        private double p95Us;
        private double p99Us;

        public LatencyStats(double minUs, double maxUs, double meanUs,
                            double medianUs, double p90Us, double p95Us, double p99Us) {
            this.minUs = minUs;
            this.maxUs = maxUs;
            this.meanUs = meanUs;
            this.medianUs = medianUs;
            this.p50Us = medianUs; // p50 == median
            this.p90Us = p90Us;
            this.p95Us = p95Us;
            this.p99Us = p99Us;
        }

        public double getMinUs() { return minUs; }
        public double getMaxUs() { return maxUs; }
        public double getMeanUs() { return meanUs; }
        public double getMedianUs() { return medianUs; }
        public double getP50Us() { return p50Us; }
        public double getP90Us() { return p90Us; }
        public double getP95Us() { return p95Us; }
        public double getP99Us() { return p99Us; }
    }

    /**
     * Throughput statistics.
     */
    public static class ThroughputStats {
        private double opsPerSecond;

        public ThroughputStats(double opsPerSecond) {
            this.opsPerSecond = opsPerSecond;
        }

        public double getOpsPerSecond() { return opsPerSecond; }
    }
}
