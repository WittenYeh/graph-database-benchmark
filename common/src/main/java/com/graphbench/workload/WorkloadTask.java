package com.graphbench.workload;

import java.util.Map;

/**
 * Base class representing a workload task loaded from JSON file.
 */
public class WorkloadTask {
    private String taskType;
    private int opsCount;
    private Map<String, Object> parameters;

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public int getOpsCount() {
        return opsCount;
    }

    public void setOpsCount(int opsCount) {
        this.opsCount = opsCount;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }
}
