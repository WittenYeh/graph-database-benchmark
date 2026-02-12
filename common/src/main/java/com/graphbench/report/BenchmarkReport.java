package com.graphbench.report;

import com.graphbench.benchmark.BenchmarkResult;

import java.util.List;
import java.util.Map;

/**
 * Full benchmark report containing metadata and per-task results.
 */
public class BenchmarkReport {

    private Metadata metadata;
    private List<BenchmarkResult> results;

    public BenchmarkReport() {
    }

    public BenchmarkReport(Metadata metadata, List<BenchmarkResult> results) {
        this.metadata = metadata;
        this.results = results;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public List<BenchmarkResult> getResults() {
        return results;
    }

    public void setResults(List<BenchmarkResult> results) {
        this.results = results;
    }

    /**
     * Report metadata: database, dataset, environment info.
     */
    public static class Metadata {
        private String database;
        private String dataset;
        private String datasetPath;
        private String timestamp;
        private int serverThreads;
        private String javaVersion;
        private String osName;
        private String osVersion;

        public Metadata() {
        }

        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }

        public String getDataset() { return dataset; }
        public void setDataset(String dataset) { this.dataset = dataset; }

        public String getDatasetPath() { return datasetPath; }
        public void setDatasetPath(String datasetPath) { this.datasetPath = datasetPath; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

        public int getServerThreads() { return serverThreads; }
        public void setServerThreads(int serverThreads) { this.serverThreads = serverThreads; }

        public String getJavaVersion() { return javaVersion; }
        public void setJavaVersion(String javaVersion) { this.javaVersion = javaVersion; }

        public String getOsName() { return osName; }
        public void setOsName(String osName) { this.osName = osName; }

        public String getOsVersion() { return osVersion; }
        public void setOsVersion(String osVersion) { this.osVersion = osVersion; }
    }
}
