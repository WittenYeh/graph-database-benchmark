#pragma once

#include <string>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

namespace graphbench {

namespace fs = std::filesystem;

/**
 * Snapshot manager for database state management.
 * Provides snapshot and restore functionality for benchmarks.
 */
class SnapshotManager {
public:
    /**
     * Create a snapshot of the database directory.
     * @param dbPath Database directory path
     * @param snapshotPath Snapshot directory path
     */
    static void createSnapshot(const std::string& dbPath, const std::string& snapshotPath) {
        if (!fs::exists(dbPath)) {
            throw std::runtime_error("Database path does not exist: " + dbPath);
        }

        // Remove old snapshot if exists
        if (fs::exists(snapshotPath)) {
            fs::remove_all(snapshotPath);
        }

        // Create snapshot directory
        fs::create_directories(snapshotPath);

        // Copy database files to snapshot
        copyDirectory(dbPath, snapshotPath);

        std::cout << "✓ Snapshot created: " << snapshotPath << std::endl;
    }

    /**
     * Restore database from snapshot.
     * @param dbPath Database directory path
     * @param snapshotPath Snapshot directory path
     */
    static void restoreSnapshot(const std::string& dbPath, const std::string& snapshotPath) {
        if (!fs::exists(snapshotPath)) {
            throw std::runtime_error("Snapshot path does not exist: " + snapshotPath);
        }

        // Remove current database
        if (fs::exists(dbPath)) {
            fs::remove_all(dbPath);
        }

        // Create database directory
        fs::create_directories(dbPath);

        // Copy snapshot files to database
        copyDirectory(snapshotPath, dbPath);

        std::cout << "✓ Database restored from snapshot" << std::endl;
    }

    /**
     * Check if snapshot exists.
     */
    static bool snapshotExists(const std::string& snapshotPath) {
        return fs::exists(snapshotPath) && !fs::is_empty(snapshotPath);
    }

    /**
     * Delete snapshot.
     */
    static void deleteSnapshot(const std::string& snapshotPath) {
        if (fs::exists(snapshotPath)) {
            fs::remove_all(snapshotPath);
            std::cout << "✓ Snapshot deleted: " << snapshotPath << std::endl;
        }
    }

private:
    /**
     * Recursively copy directory contents.
     */
    static void copyDirectory(const fs::path& source, const fs::path& destination) {
        if (!fs::exists(source) || !fs::is_directory(source)) {
            throw std::runtime_error("Source is not a valid directory: " + source.string());
        }

        if (!fs::exists(destination)) {
            fs::create_directories(destination);
        }

        for (const auto& entry : fs::recursive_directory_iterator(source)) {
            const auto& path = entry.path();
            auto relativePath = fs::relative(path, source);
            auto destPath = destination / relativePath;

            if (fs::is_directory(path)) {
                fs::create_directories(destPath);
            } else if (fs::is_regular_file(path)) {
                fs::copy_file(path, destPath, fs::copy_options::overwrite_existing);
            }
        }
    }
};

} // namespace graphbench
