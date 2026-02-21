package com.graphbench.workload;

import java.util.List;

/**
 * Parameters for ADD_VERTEX task.
 */
public class AddVertexParams {
    private List<Long> ids;

    public List<Long> getIds() {
        return ids;
    }

    public void setIds(List<Long> ids) {
        this.ids = ids;
    }
}
