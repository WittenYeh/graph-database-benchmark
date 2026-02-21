package com.graphbench.workload;

import java.util.List;
import java.util.Map;

/**
 * Parameters for UPDATE_VERTEX_PROPERTY task.
 */
public class UpdateVertexPropertyParams {
    private List<VertexUpdate> updates;

    public List<VertexUpdate> getUpdates() {
        return updates;
    }

    public void setUpdates(List<VertexUpdate> updates) {
        this.updates = updates;
    }

    public static class VertexUpdate {
        private long id;
        private Object systemId;
        private Map<String, Object> properties;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public Object getSystemId() {
            return systemId;
        }

        public void setSystemId(Object systemId) {
            this.systemId = systemId;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }
    }
}
