package com.graphbench.workload;

import java.util.List;

/**
 * Parameters for GET_VERTEX_BY_PROPERTY task.
 */
public class GetVertexByPropertyParams {
    private List<PropertyQuery> queries;

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
