package com.graphbench.compiler;

import com.graphbench.workload.Operation;

/**
 * A single compiled query string with its associated operation type.
 */
public class CompiledQuery {

    private Operation operationType;
    private String query;

    public CompiledQuery() {
    }

    public CompiledQuery(Operation operationType, String query) {
        this.operationType = operationType;
        this.query = query;
    }

    public Operation getOperationType() {
        return operationType;
    }

    public void setOperationType(Operation operationType) {
        this.operationType = operationType;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
