package com.graphbench.compiler;

import java.util.List;

/**
 * A fully compiled workload: metadata plus a list of database-specific query strings.
 */
public class CompiledWorkload {

    private String taskName;
    private String database;
    private QueryLanguage queryLanguage;
    private int totalOps;
    private int clientThreads;
    private boolean bulkLoad;
    private boolean copyMode;
    private String datasetPath;
    private String dataset;
    private List<CompiledQuery> queries;

    public CompiledWorkload() {
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public QueryLanguage getQueryLanguage() {
        return queryLanguage;
    }

    public void setQueryLanguage(QueryLanguage queryLanguage) {
        this.queryLanguage = queryLanguage;
    }

    public int getTotalOps() {
        return totalOps;
    }

    public void setTotalOps(int totalOps) {
        this.totalOps = totalOps;
    }

    public int getClientThreads() {
        return clientThreads;
    }

    public void setClientThreads(int clientThreads) {
        this.clientThreads = clientThreads;
    }

    public boolean isBulkLoad() {
        return bulkLoad;
    }

    public void setBulkLoad(boolean bulkLoad) {
        this.bulkLoad = bulkLoad;
    }

    public boolean isCopyMode() {
        return copyMode;
    }

    public void setCopyMode(boolean copyMode) {
        this.copyMode = copyMode;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public void setDatasetPath(String datasetPath) {
        this.datasetPath = datasetPath;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public List<CompiledQuery> getQueries() {
        return queries;
    }

    public void setQueries(List<CompiledQuery> queries) {
        this.queries = queries;
    }
}
