package com.graphbench.workload;

import java.util.List;
import java.util.Map;

/**
 * Parameters for UPDATE_EDGE_PROPERTY task.
 */
public class UpdateEdgePropertyParams {
    private String label;
    private List<EdgeUpdate> updates;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<EdgeUpdate> getUpdates() {
        return updates;
    }

    public void setUpdates(List<EdgeUpdate> updates) {
        this.updates = updates;
    }

    public static class EdgeUpdate {
        private long src;
        private long dst;
        private Object srcSystemId;
        private Object dstSystemId;
        private Map<String, Object> properties;

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

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }
    }
}
