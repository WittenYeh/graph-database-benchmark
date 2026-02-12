package com.graphbench.config;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Loads database configurations from database-config.json.
 */
public class DatabaseConfigLoader {
    private final Map<String, DatabaseInstanceConfig> databases;

    /**
     * Load database configurations from a JSON file.
     *
     * @param configPath Path to database-config.json
     * @throws IOException if the file cannot be read
     * @throws IllegalArgumentException if the JSON is invalid
     */
    public DatabaseConfigLoader(String configPath) throws IOException {
        Gson gson = new Gson();
        try (FileReader reader = new FileReader(configPath)) {
            JsonObject root = gson.fromJson(reader, JsonObject.class);
            if (root == null || !root.has("databases")) {
                throw new IllegalArgumentException("Invalid database-config.json: missing 'databases' key");
            }

            JsonObject databasesJson = root.getAsJsonObject("databases");
            this.databases = new java.util.HashMap<>();

            for (String dbName : databasesJson.keySet()) {
                JsonObject dbConfig = databasesJson.getAsJsonObject(dbName);
                DatabaseInstanceConfig config = gson.fromJson(dbConfig, DatabaseInstanceConfig.class);
                databases.put(dbName, config);
            }
        }
    }

    /**
     * Get configuration for a specific database.
     *
     * @param databaseName Name of the database (e.g., "neo4j", "janusgraph")
     * @return DatabaseInstanceConfig for the specified database
     * @throws IllegalArgumentException if the database name is not registered
     */
    public DatabaseInstanceConfig getDatabaseConfig(String databaseName) {
        if (!databases.containsKey(databaseName)) {
            throw new IllegalArgumentException(
                "Database '" + databaseName + "' not found in configuration. " +
                "Available databases: " + String.join(", ", databases.keySet())
            );
        }
        return databases.get(databaseName);
    }

    /**
     * Check if a database is registered in the configuration.
     *
     * @param databaseName Name of the database
     * @return true if the database is registered, false otherwise
     */
    public boolean hasDatabase(String databaseName) {
        return databases.containsKey(databaseName);
    }

    /**
     * Get all registered database names.
     *
     * @return Set of database names
     */
    public java.util.Set<String> getDatabaseNames() {
        return databases.keySet();
    }
}
