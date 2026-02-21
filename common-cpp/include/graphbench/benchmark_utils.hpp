#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <nlohmann/json.hpp>

namespace graphbench {

using json = nlohmann::json;
namespace fs = std::filesystem;

/**
 * Utility functions for benchmark operations.
 */
class BenchmarkUtils {
public:
    /**
     * Check and clean database directory if it exists.
     */
    static void checkAndCleanDatabaseDirectory(const std::string& dbPath) {
        fs::path path(dbPath);
        if (fs::exists(path)) {
            fs::remove_all(path);
        }
        fs::create_directories(path);
    }

    /**
     * Delete a directory recursively.
     */
    static void deleteDirectory(const fs::path& path) {
        if (fs::exists(path)) {
            fs::remove_all(path);
        }
    }

    /**
     * Copy directory recursively.
     */
    static void copyDirectory(const fs::path& src, const fs::path& dst) {
        if (!fs::exists(src) || !fs::is_directory(src)) {
            throw std::runtime_error("Source directory does not exist: " + src.string());
        }

        if (fs::exists(dst)) {
            fs::remove_all(dst);
        }

        fs::create_directories(dst);
        fs::copy(src, dst, fs::copy_options::recursive);
    }

    /**
     * Cleanup database files (database and snapshot directories).
     */
    static void cleanupDatabaseFiles(const std::string& dbPath, const std::string& snapshotPath) {
        deleteDirectory(fs::path(dbPath));
        deleteDirectory(fs::path(snapshotPath));
    }

    /**
     * Read JSON file.
     */
    static json readJsonFile(const std::string& filePath) {
        std::ifstream file(filePath);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + filePath);
        }
        json j;
        file >> j;
        return j;
    }

    /**
     * Write JSON file.
     */
    static void writeJsonFile(const std::string& filePath, const json& j) {
        std::ofstream file(filePath);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file for writing: " + filePath);
        }
        file << j.dump(2);
    }

    /**
     * Get environment variable with default value.
     */
    static std::string getEnv(const std::string& key, const std::string& defaultValue = "") {
        const char* val = std::getenv(key.c_str());
        return val ? std::string(val) : defaultValue;
    }

    /**
     * Split string by delimiter.
     */
    static std::vector<std::string> split(const std::string& str, char delimiter) {
        std::vector<std::string> tokens;
        std::stringstream ss(str);
        std::string token;
        while (std::getline(ss, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }

    /**
     * Trim whitespace from string.
     */
    static std::string trim(const std::string& str) {
        size_t first = str.find_first_not_of(" \t\n\r");
        if (first == std::string::npos) return "";
        size_t last = str.find_last_not_of(" \t\n\r");
        return str.substr(first, last - first + 1);
    }
};

} // namespace graphbench
