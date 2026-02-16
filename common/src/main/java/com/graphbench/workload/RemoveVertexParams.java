package com.graphbench.workload;

import java.util.List;

/**
 * Parameters for REMOVE_VERTEX task.
 */
public class RemoveVertexParams {
    private List<Long> ids;

    public List<Long> getIds() {
        return ids;
    }

    public void setIds(List<Long> ids) {
        this.ids = ids;
    }
}
