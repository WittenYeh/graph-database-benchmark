package com.graphbench.workload;

import com.google.gson.annotations.SerializedName;
import java.util.List;
import java.util.Map;

/**
 * Base class representing a workload task loaded from JSON file.
 */
public class WorkloadTask {
    @SerializedName("task_type")
    private String taskType;

    @SerializedName("ops_count")
    private int opsCount;

    @SerializedName("batch_sizes")
    private List<Integer> batchSizes;

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

    public List<Integer> getBatchSizes() {
        return batchSizes;
    }

    public void setBatchSizes(List<Integer> batchSizes) {
        this.batchSizes = batchSizes;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }
}
