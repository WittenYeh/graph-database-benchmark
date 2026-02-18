package com.graphbench.api;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;

/**
 * Utility class for common benchmark operations.
 * Contains file system operations and helper methods.
 */
public class BenchmarkUtils {

    /**
     * Check if database directory exists and is not empty, clean it if necessary.
     * @param dbPath Path to the database directory
     * @throws IOException if cleanup fails
     */
    public static void checkAndCleanDatabaseDirectory(String dbPath) throws IOException {
        File dbDir = new File(dbPath);

        // Check if database directory exists and is not empty
        if (dbDir.exists() && dbDir.isDirectory()) {
            File[] files = dbDir.listFiles();
            if (files != null && files.length > 0) {
                System.out.println("⚠️  Warning: Database directory is not empty, cleaning up residual data...");
                deleteDirectory(dbDir);
                System.out.println("✓ Database directory cleaned");
            }
        }
    }

    /**
     * Recursively delete a directory and all its contents.
     * @param directory Directory to delete
     * @throws IOException if deletion fails
     */
    public static void deleteDirectory(File directory) throws IOException {
        Path dirPath = directory.toPath();
        if (Files.exists(dirPath)) {
            Files.walk(dirPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }

    /**
     * Copy a directory recursively from source to target.
     * @param source Source directory path
     * @param target Target directory path
     * @throws IOException if copy fails
     */
    public static void copyDirectory(Path source, Path target) throws IOException {
        Files.walk(source).forEach(sourcePath -> {
            try {
                Path targetPath = target.resolve(source.relativize(sourcePath));
                if (Files.isDirectory(sourcePath)) {
                    if (!Files.exists(targetPath)) {
                        Files.createDirectories(targetPath);
                    }
                } else {
                    Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
    }

    /**
     * Cleanup database data files after benchmark completion.
     * This removes the database directory and snapshot directory.
     * @param dbPath Path to the database directory
     * @param snapshotPath Path to the snapshot directory
     * @throws IOException if cleanup fails
     */
    public static void cleanupDatabaseFiles(String dbPath, String snapshotPath) throws IOException {
        // Delete database directory
        File dbDir = new File(dbPath);
        if (dbDir.exists()) {
            deleteDirectory(dbDir);
        }

        // Delete snapshot directory
        File snapshotDir = new File(snapshotPath);
        if (snapshotDir.exists()) {
            deleteDirectory(snapshotDir);
        }
    }
}
