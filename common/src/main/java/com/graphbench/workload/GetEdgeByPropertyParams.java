package com.graphbench.workload;

import java.util.List;

/**
 * Parameters for GET_EDGE_BY_PROPERTY task.
 */
public class GetEdgeByPropertyParams {
    private String label;
    private List<PropertyQuery> queries;

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<PropertyQuery> getQueries() {
        return queries;
    }

    public void setQueries(List<PropertyQuery> queries) {
        this.queries = queries;
    }

    public static class PropertyQuery {
        private String key;
        private Object value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }
}
