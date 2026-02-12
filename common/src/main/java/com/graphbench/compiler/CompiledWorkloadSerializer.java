package com.graphbench.compiler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Serializes and deserializes CompiledWorkload objects to/from JSON files.
 */
public class CompiledWorkloadSerializer {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Writes a CompiledWorkload to a JSON file.
     */
    public static void write(CompiledWorkload compiled, String filePath) throws IOException {
        try (FileWriter writer = new FileWriter(filePath)) {
            GSON.toJson(compiled, writer);
        }
    }

    /**
     * Reads a CompiledWorkload from a JSON file.
     */
    public static CompiledWorkload read(String filePath) throws IOException {
        try (FileReader reader = new FileReader(filePath)) {
            return GSON.fromJson(reader, CompiledWorkload.class);
        }
    }
}
