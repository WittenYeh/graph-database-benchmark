package com.graphbench.config;

import com.google.gson.annotations.SerializedName;
import java.util.Map;

/**
 * Configuration for a specific database instance.
 * Loaded from database-config.json.
 */
public class DatabaseInstanceConfig {
    @SerializedName("adapter_class")
    private String adapterClass;

    @SerializedName("query_language")
    private String queryLanguage;

    private Map<String, Object> schema;
    private Map<String, Object> storage;
    private Map<String, Object> indexing;
    private Map<String, Object> performance;
    private Map<String, Object> connection;

    public String getAdapterClass() {
        return adapterClass;
    }

    public void setAdapterClass(String adapterClass) {
        this.adapterClass = adapterClass;
    }

    public String getQueryLanguage() {
        return queryLanguage;
    }

    public void setQueryLanguage(String queryLanguage) {
        this.queryLanguage = queryLanguage;
    }

    public Map<String, Object> getSchema() {
        return schema;
    }

    public void setSchema(Map<String, Object> schema) {
        this.schema = schema;
    }

    public Map<String, Object> getStorage() {
        return storage;
    }

    public void setStorage(Map<String, Object> storage) {
        this.storage = storage;
    }

    public Map<String, Object> getIndexing() {
        return indexing;
    }

    public void setIndexing(Map<String, Object> indexing) {
        this.indexing = indexing;
    }

    public Map<String, Object> getPerformance() {
        return performance;
    }

    public void setPerformance(Map<String, Object> performance) {
        this.performance = performance;
    }

    public Map<String, Object> getConnection() {
        return connection;
    }

    public void setConnection(Map<String, Object> connection) {
        this.connection = connection;
    }

    /**
     * Get a schema property as a String.
     */
    public String getSchemaString(String key) {
        if (schema == null) return null;
        Object value = schema.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Get a schema property as an Integer.
     */
    public Integer getSchemaInt(String key) {
        if (schema == null) return null;
        Object value = schema.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    /**
     * Get a storage property as a String.
     */
    public String getStorageString(String key) {
        if (storage == null) return null;
        Object value = storage.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Get a storage property as an Integer.
     */
    public Integer getStorageInt(String key) {
        if (storage == null) return null;
        Object value = storage.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    /**
     * Get an indexing property as an Integer.
     */
    public Integer getIndexingInt(String key) {
        if (indexing == null) return null;
        Object value = indexing.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    /**
     * Get an indexing property as a Boolean.
     */
    public Boolean getIndexingBoolean(String key) {
        if (indexing == null) return null;
        Object value = indexing.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return null;
    }

    /**
     * Get a performance property as an Integer.
     */
    public Integer getPerformanceInt(String key) {
        if (performance == null) return null;
        Object value = performance.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    /**
     * Get a connection property as a String.
     */
    public String getConnectionString(String key) {
        if (connection == null) return null;
        Object value = connection.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Get a connection property as a Boolean.
     */
    public Boolean getConnectionBoolean(String key) {
        if (connection == null) return null;
        Object value = connection.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return null;
    }
}
