package com.graphbench.workload;

import java.util.List;

/**
 * A complete generated workload: metadata plus an ordered list of operations.
 */
public class Workload {

    private String taskName;
    private String dataset;
    private String datasetPath;
    private int totalOps;
    private int clientThreads;
    private boolean copyMode;
    private List<WorkloadOperation> operations;

    public Workload() {
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public void setDatasetPath(String datasetPath) {
        this.datasetPath = datasetPath;
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

    public boolean isCopyMode() {
        return copyMode;
    }

    public void setCopyMode(boolean copyMode) {
        this.copyMode = copyMode;
    }

    public List<WorkloadOperation> getOperations() {
        return operations;
    }

    public void setOperations(List<WorkloadOperation> operations) {
        this.operations = operations;
    }
}
