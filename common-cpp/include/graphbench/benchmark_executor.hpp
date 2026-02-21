#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>
#include <chrono>
#include <any>
#include <filesystem>
#include <graphbench/benchmark_utils.hpp>

namespace graphbench {

namespace fs = std::filesystem;

/**
 * CRTP base class for structural benchmark executors.
 * Handles graph structural operations: addVertex, removeVertex, addEdge, removeEdge, getNbrs.
 * Uses static polymorphism for zero-overhead abstraction.
 */
template<typename Derived>
class BenchmarkExecutor {
public:
    // Core operations - must be implemented by derived class
    void initDatabase() {
        static_cast<Derived*>(this)->initDatabaseImpl();
    }

    void shutdown() {
        static_cast<Derived*>(this)->shutdownImpl();
    }

    std::map<std::string, std::any> loadGraph(const std::string& datasetPath) {
        return static_cast<Derived*>(this)->loadGraphImpl(datasetPath);
    }

    std::vector<double> addVertex(int count, int batchSize) {
        return static_cast<Derived*>(this)->addVertexImpl(count, batchSize);
    }

    std::vector<double> removeVertex(const std::vector<std::any>& systemIds, int batchSize) {
        return static_cast<Derived*>(this)->removeVertexImpl(systemIds, batchSize);
    }

    std::vector<double> addEdge(const std::string& label,
                                const std::vector<std::pair<std::any, std::any>>& pairs,
                                int batchSize) {
        return static_cast<Derived*>(this)->addEdgeImpl(label, pairs, batchSize);
    }

    std::vector<double> removeEdge(const std::string& label,
                                   const std::vector<std::pair<std::any, std::any>>& pairs,
                                   int batchSize) {
        return static_cast<Derived*>(this)->removeEdgeImpl(label, pairs, batchSize);
    }

    std::vector<double> getNbrs(const std::string& direction,
                                const std::vector<std::any>& systemIds,
                                int batchSize) {
        return static_cast<Derived*>(this)->getNbrsImpl(direction, systemIds, batchSize);
    }

    // Default methods with batch size = 1
    std::vector<double> addVertex(int count) {
        return addVertex(count, 1);
    }

    std::vector<double> removeVertex(const std::vector<std::any>& systemIds) {
        return removeVertex(systemIds, 1);
    }

    std::vector<double> addEdge(const std::string& label,
                                const std::vector<std::pair<std::any, std::any>>& pairs) {
        return addEdge(label, pairs, 1);
    }

    std::vector<double> removeEdge(const std::string& label,
                                   const std::vector<std::pair<std::any, std::any>>& pairs) {
        return removeEdge(label, pairs, 1);
    }

    std::vector<double> getNbrs(const std::string& direction,
                                const std::vector<std::any>& systemIds) {
        return getNbrs(direction, systemIds, 1);
    }

    std::string getDatabaseName() const {
        return static_cast<const Derived*>(this)->getDatabaseNameImpl();
    }

    std::string getDatabasePath() const {
        return static_cast<const Derived*>(this)->getDatabasePathImpl();
    }

    std::string getSnapshotPath() const {
        return static_cast<const Derived*>(this)->getSnapshotPathImpl();
    }

    void closeDatabase() {
        static_cast<Derived*>(this)->closeDatabaseImpl();
    }

    void openDatabase() {
        static_cast<Derived*>(this)->openDatabaseImpl();
    }

    void snapGraph() {
        // Close the database
        closeDatabase();

        // Delete old snapshot if exists
        fs::path snapshotPath(getSnapshotPath());
        if (fs::exists(snapshotPath)) {
            BenchmarkUtils::deleteDirectory(snapshotPath);
        }

        // Copy database to snapshot location
        fs::path dbPath(getDatabasePath());
        if (!fs::exists(dbPath)) {
            throw std::runtime_error("Database directory does not exist: " + dbPath.string());
        }

        BenchmarkUtils::copyDirectory(dbPath, snapshotPath);

        // Reopen the database
        openDatabase();
    }

    void restoreGraph() {
        // Close the database
        closeDatabase();

        // Delete current database directory
        fs::path dbPath(getDatabasePath());
        if (fs::exists(dbPath)) {
            BenchmarkUtils::deleteDirectory(dbPath);
        }

        // Copy snapshot back to database location
        fs::path snapshotPath(getSnapshotPath());
        if (!fs::exists(snapshotPath)) {
            throw std::runtime_error("Snapshot does not exist at: " + snapshotPath.string());
        }

        BenchmarkUtils::copyDirectory(snapshotPath, dbPath);

        // Reopen the database
        openDatabase();
    }

    int getErrorCount() const {
        return static_cast<const Derived*>(this)->getErrorCountImpl();
    }

    void resetErrorCount() {
        static_cast<Derived*>(this)->resetErrorCountImpl();
    }

    std::any getSystemId(int64_t originId) const {
        return static_cast<const Derived*>(this)->getSystemIdImpl(originId);
    }

protected:
    ~BenchmarkExecutor() = default;
};

} // namespace graphbench
