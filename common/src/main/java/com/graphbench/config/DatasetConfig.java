package com.graphbench.config;

import java.util.Map;

/**
 * Configuration loaded from datasets.json.
 * Maps dataset names to their .mtx file paths relative to root_dir.
 */
public class DatasetConfig {

    private String root_dir;
    private Map<String, String> datasets;

    public DatasetConfig() {
    }

    public String getRootDir() {
        return root_dir;
    }

    public void setRootDir(String rootDir) {
        this.root_dir = rootDir;
    }

    public Map<String, String> getDatasets() {
        return datasets;
    }

    public void setDatasets(Map<String, String> datasets) {
        this.datasets = datasets;
    }

    /**
     * Resolves the full path for a dataset by name.
     */
    public String resolvePath(String datasetName) {
        String relativePath = datasets.get(datasetName);
        if (relativePath == null) {
            throw new IllegalArgumentException("Unknown dataset: " + datasetName);
        }
        String root = root_dir.endsWith("/") ? root_dir : root_dir + "/";
        return root + relativePath;
    }
}
