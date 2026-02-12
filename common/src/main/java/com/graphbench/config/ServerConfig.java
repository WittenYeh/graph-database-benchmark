package com.graphbench.config;

/**
 * Configuration for the embedded database server.
 */
public class ServerConfig {

    private int threads;

    public ServerConfig() {
    }

    public ServerConfig(int threads) {
        this.threads = threads;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }
}
