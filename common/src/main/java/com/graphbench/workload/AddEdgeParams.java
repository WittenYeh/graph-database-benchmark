package com.graphbench.workload;

import java.util.List;

/**
 * Parameters for ADD_EDGE task.
 */
public class AddEdgeParams {
    private String label;
    private List<EdgePair> pairs;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<EdgePair> getPairs() {
        return pairs;
    }

    public void setPairs(List<EdgePair> pairs) {
        this.pairs = pairs;
    }

    public static class EdgePair {
        private long src;
        private long dst;

        public long getSrc() {
            return src;
        }

        public void setSrc(long src) {
            this.src = src;
        }

        public long getDst() {
            return dst;
        }

        public void setDst(long dst) {
            this.dst = dst;
        }
    }
}
