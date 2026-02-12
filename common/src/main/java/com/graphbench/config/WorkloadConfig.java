package com.graphbench.config;

import java.util.List;

/**
 * Top-level configuration loaded from workload_config.json.
 */
public class WorkloadConfig {

    private String dataset;
    private ServerConfig server_config;
    private List<TaskConfig> tasks;

    public WorkloadConfig() {
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public ServerConfig getServerConfig() {
        return server_config;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.server_config = serverConfig;
    }

    public List<TaskConfig> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskConfig> tasks) {
        this.tasks = tasks;
    }
}
