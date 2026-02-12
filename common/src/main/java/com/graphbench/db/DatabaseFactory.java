package com.graphbench.db;

import com.graphbench.config.DatabaseInstanceConfig;

/**
 * Factory for creating database adapters by name.
 * Uses reflection to instantiate adapters based on configuration.
 */
public class DatabaseFactory {

    /**
     * Create a database adapter instance from configuration.
     *
     * @param databaseName Name of the database
     * @param config Database configuration
     * @return DatabaseAdapter instance
     * @throws IllegalArgumentException if the adapter class cannot be instantiated
     */
    public static DatabaseAdapter create(String databaseName, DatabaseInstanceConfig config) {
        String adapterClass = config.getAdapterClass();
        if (adapterClass == null || adapterClass.isEmpty()) {
            throw new IllegalArgumentException("Adapter class not specified for database: " + databaseName);
        }

        try {
            Class<?> clazz = Class.forName(adapterClass);
            return (DatabaseAdapter) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to instantiate adapter class '" + adapterClass + "' for database '" + databaseName + "'", e);
        }
    }
}
