package com.graphbench.report;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes a BenchmarkReport to a JSON file.
 */
public class ReportWriter {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Writes the report to the specified directory and returns the full file path.
     */
    public static String write(BenchmarkReport report, String outputDir) throws IOException {
        // Generate filename with timestamp
        String timestamp = java.time.LocalDateTime.now()
            .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String database = report.getMetadata().getDatabase();
        String filename = String.format("bench_%s_%s.json", database, timestamp);
        String filePath = new File(outputDir, filename).getAbsolutePath();

        File parent = new File(filePath).getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IOException("Failed to create report directory: " + parent);
        }
        try (FileWriter writer = new FileWriter(filePath)) {
            GSON.toJson(report, writer);
        }
        return filePath;
    }
}
