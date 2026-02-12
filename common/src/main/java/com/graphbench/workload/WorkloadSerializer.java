package com.graphbench.workload;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Serializes and deserializes Workload objects to/from JSON files.
 */
public class WorkloadSerializer {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Writes a Workload to a JSON file.
     */
    public static void write(Workload workload, String filePath) throws IOException {
        try (FileWriter writer = new FileWriter(filePath)) {
            GSON.toJson(workload, writer);
        }
    }

    /**
     * Reads a Workload from a JSON file.
     */
    public static Workload read(String filePath) throws IOException {
        try (FileReader reader = new FileReader(filePath)) {
            return GSON.fromJson(reader, Workload.class);
        }
    }
}
