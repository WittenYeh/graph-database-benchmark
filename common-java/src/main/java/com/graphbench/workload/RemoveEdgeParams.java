package com.graphbench.workload;

import java.util.List;

/**
 * Parameters for REMOVE_EDGE task.
 */
public class RemoveEdgeParams {
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
        private Object srcSystemId;
        private Object dstSystemId;

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

        public Object getSrcSystemId() {
            return srcSystemId;
        }

        public void setSrcSystemId(Object srcSystemId) {
            this.srcSystemId = srcSystemId;
        }

        public Object getDstSystemId() {
            return dstSystemId;
        }

        public void setDstSystemId(Object dstSystemId) {
            this.dstSystemId = dstSystemId;
        }
    }
}
