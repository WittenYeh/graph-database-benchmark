package com.graphbench.config;

import com.google.gson.annotations.SerializedName;
import java.util.Map;

/**
 * Configuration for a single benchmark task.
 */
public class TaskConfig {

    private String name;
    private int ops;
    private int client_threads = 1;
    private Map<String, Double> ratios;

    @SerializedName("copy_mode")
    private boolean copyMode = false;

    public TaskConfig() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getOps() {
        return ops;
    }

    public void setOps(int ops) {
        this.ops = ops;
    }

    public int getClientThreads() {
        return client_threads;
    }

    public void setClientThreads(int clientThreads) {
        this.client_threads = clientThreads;
    }

    public Map<String, Double> getRatios() {
        return ratios;
    }

    public void setRatios(Map<String, Double> ratios) {
        this.ratios = ratios;
    }

    public boolean isCopyMode() {
        return copyMode;
    }

    public void setCopyMode(boolean copyMode) {
        this.copyMode = copyMode;
    }

    /**
     * Returns true if this task is a throughput benchmark (multi-threaded).
     */
    public boolean isThroughput() {
        return name != null && name.endsWith("_throughput");
    }

    /**
     * Returns true if this task is a latency benchmark (single-threaded).
     */
    public boolean isLatency() {
        return name != null && name.endsWith("_latency");
    }
}
