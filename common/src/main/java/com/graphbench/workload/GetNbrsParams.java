package com.graphbench.workload;

import java.util.List;

/**
 * Parameters for GET_NBRS task.
 */
public class GetNbrsParams {
    private String direction;
    private List<Long> ids;

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public List<Long> getIds() {
        return ids;
    }

    public void setIds(List<Long> ids) {
        this.ids = ids;
    }
}
