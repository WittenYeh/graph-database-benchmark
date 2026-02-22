#pragma once

#include "arangodb_client.hpp"
#include "arangodb_graph_loader.hpp"
#include <graphbench/benchmark_executor.hpp>
#include <graphbench/progress_callback.hpp>
#include <graphbench/benchmark_utils.hpp>
#include <nlohmann/json.hpp>
#include <map>
#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <random>

namespace graphbench {

using json = nlohmann::json;

/**
 * ArangoDB structural benchmark executor using REST API.
 * Implements graph structural operations (add/remove vertices/edges, get neighbors).
 * Uses CRTP pattern for zero-overhead abstraction.
 */
class ArangoDBBenchmarkExecutor : public BenchmarkExecutor<ArangoDBBenchmarkExecutor> {
public:
    ArangoDBBenchmarkExecutor()
        : dbPath_(DB_PATH),
          snapshotPath_(SNAPSHOT_PATH),
          errorCount_(0) {
        std::string callbackUrl = BenchmarkUtils::getEnv("PROGRESS_CALLBACK_URL", "");
        progressCallback_ = std::make_shared<ProgressCallback>(callbackUrl);
    }

    /**
     * Initialize database: create connection, database, and collections.
     */
    void initDatabaseImpl() {
        BenchmarkUtils::checkAndCleanDatabaseDirectory(dbPath_);

        // Create ArangoDB utility instance
        arangoUtils_ = std::make_shared<ArangoDBClient>("localhost", 8529, "root", "");

        // Create database
        try {
            arangoUtils_->createDatabase(DB_NAME);
        } catch (const std::exception& e) {
            progressCallback_->sendLogMessage("Database creation: " + std::string(e.what()), "WARN");
        }

        // Switch to the benchmark database
        arangoUtils_->useDatabase(DB_NAME);

        // Create collections
        arangoUtils_->createCollection(DB_NAME, VERTEX_COLLECTION, false);
        arangoUtils_->createCollection(DB_NAME, EDGE_COLLECTION, true);

        progressCallback_->sendLogMessage("ArangoDB database initialized", "INFO");
    }

    /**
     * Shutdown database: drop database and close connection.
     */
    void shutdownImpl() {
        if (arangoUtils_) {
            try {
                arangoUtils_->dropDatabase(DB_NAME);
            } catch (const std::exception& e) {
                progressCallback_->sendLogMessage("Database drop: " + std::string(e.what()), "WARN");
            }
            arangoUtils_.reset();
        }
    }

    /**
     * Close database connection (for snapshot/restore operations).
     */
    void closeDatabaseImpl() {
        if (arangoUtils_) {
            arangoUtils_.reset();
        }
    }

    /**
     * Open database connection (after snapshot/restore operations).
     */
    void openDatabaseImpl() {
        arangoUtils_ = std::make_shared<ArangoDBClient>("localhost", 8529, "root", "");
        arangoUtils_->useDatabase(DB_NAME);
    }

    /**
     * Load graph from CSV files (structural benchmark: no properties).
     * Uses ArangoDBGraphLoader to batch-load nodes and edges.
     */
    std::map<std::string, std::any> loadGraphImpl(const std::string& datasetPath) {
        ArangoDBGraphLoader loader(arangoUtils_, DB_NAME, progressCallback_, false);
        auto result = loader.load(datasetPath);
        nodeIdsMap_ = loader.getNodeIdsMap();
        return result;
    }

    /**
     * Add vertices in batches.
     * Uses batch AQL INSERT to add multiple vertices in one query.
     */
    std::vector<double> addVertexImpl(int count, int batchSize) {
        std::random_device rd;
        std::mt19937 gen(rd());

        return batchExecute(count, batchSize, [this, &gen](int batchCount) {
            // Build batch of vertex documents
            json docs = json::array();
            for (int i = 0; i < batchCount; i++) {
                docs.push_back({
                    {"_key", "new_v" + std::to_string(gen())}
                });
            }

            // Single batch INSERT query
            std::string query = "FOR doc IN @docs INSERT doc INTO " + std::string(VERTEX_COLLECTION);
            json bindVars = {{"docs", docs}};
            arangoUtils_->executeAQL(query, bindVars);
        });
    }

    /**
     * Remove vertices in batches.
     * Uses batch AQL REMOVE to delete multiple vertices in one query.
     */
    std::vector<double> removeVertexImpl(const std::vector<std::any>& systemIds, int batchSize) {
        return batchExecute(systemIds, batchSize, [this](const std::vector<std::any>& batch) {
            // Build array of vertex keys
            json keys = json::array();
            for (const auto& id : batch) {
                keys.push_back(std::any_cast<std::string>(id));
            }

            // Single batch REMOVE query
            std::string query = "FOR key IN @keys REMOVE key IN " + std::string(VERTEX_COLLECTION);
            json bindVars = {{"keys", keys}};
            arangoUtils_->executeAQL(query, bindVars);
        });
    }

    /**
     * Add edges in batches.
     * Uses batch AQL INSERT to add multiple edges in one query.
     */
    std::vector<double> addEdgeImpl(const std::string& label,
                                    const std::vector<std::pair<std::any, std::any>>& pairs,
                                    int batchSize) {
        return batchExecute(pairs, batchSize, [this, &label](const std::vector<std::pair<std::any, std::any>>& batch) {
            // Build batch of edge documents
            json docs = json::array();
            for (const auto& [src, dst] : batch) {
                docs.push_back({
                    {"_from", std::string(VERTEX_COLLECTION) + "/" + std::any_cast<std::string>(src)},
                    {"_to", std::string(VERTEX_COLLECTION) + "/" + std::any_cast<std::string>(dst)},
                    {"label", label}
                });
            }

            // Single batch INSERT query
            std::string query = "FOR doc IN @docs INSERT doc INTO " + std::string(EDGE_COLLECTION);
            json bindVars = {{"docs", docs}};
            arangoUtils_->executeAQL(query, bindVars);
        });
    }

    /**
     * Remove edges in batches.
     * Uses batch AQL to find and remove multiple edges in one query.
     */
    std::vector<double> removeEdgeImpl(const std::string& label,
                                       const std::vector<std::pair<std::any, std::any>>& pairs,
                                       int batchSize) {
        return batchExecute(pairs, batchSize, [this, &label](const std::vector<std::pair<std::any, std::any>>& batch) {
            // Build array of edge specifications
            json edgeSpecs = json::array();
            for (const auto& [src, dst] : batch) {
                edgeSpecs.push_back({
                    {"from", std::string(VERTEX_COLLECTION) + "/" + std::any_cast<std::string>(src)},
                    {"to", std::string(VERTEX_COLLECTION) + "/" + std::any_cast<std::string>(dst)}
                });
            }

            // Single batch REMOVE query using FOR loop
            std::string query =
                "FOR spec IN @specs "
                "  FOR e IN " + std::string(EDGE_COLLECTION) + " "
                "    FILTER e._from == spec.from AND e._to == spec.to AND e.label == @label "
                "    REMOVE e IN " + std::string(EDGE_COLLECTION);
            json bindVars = {{"specs", edgeSpecs}, {"label", label}};
            arangoUtils_->executeAQL(query, bindVars);
        });
    }

    /**
     * Get neighbors in batches.
     * Uses AQL graph traversal to find neighbors.
     */
    std::vector<double> getNbrsImpl(const std::string& direction,
                                    const std::vector<std::any>& systemIds,
                                    int batchSize) {
        return batchExecute(systemIds, batchSize, [this, &direction](const std::vector<std::any>& batch) {
            // Build array of vertex IDs
            json vertexIds = json::array();
            for (const auto& id : batch) {
                vertexIds.push_back(std::string(VERTEX_COLLECTION) + "/" + std::any_cast<std::string>(id));
            }

            // Determine traversal direction
            std::string traversalDir;
            if (direction == "OUT" || direction == "OUTGOING") {
                traversalDir = "OUTBOUND";
            } else if (direction == "IN" || direction == "INCOMING") {
                traversalDir = "INBOUND";
            } else {
                traversalDir = "ANY";
            }

            // Single batch traversal query
            std::string query =
                "FOR vid IN @vids "
                "  FOR v IN 1..1 " + traversalDir + " vid " + std::string(EDGE_COLLECTION) + " "
                "    RETURN v";
            json bindVars = {{"vids", vertexIds}};
            json result = arangoUtils_->executeAQL(query, bindVars);

            // Prevent dead code elimination of query result
            asm volatile("" : : "r,m"(result) : "memory");
        });
    }

    std::string getDatabaseNameImpl() const { return "arangodb"; }
    std::string getDatabasePathImpl() const { return dbPath_; }
    std::string getSnapshotPathImpl() const { return snapshotPath_; }

    int getErrorCountImpl() const { return errorCount_; }
    void resetErrorCountImpl() { errorCount_ = 0; }

    /**
     * Get system ID (ArangoDB document key) from origin ID.
     */
    std::any getSystemIdImpl(int64_t originId) const {
        auto it = nodeIdsMap_.find(originId);
        if (it != nodeIdsMap_.end()) {
            return it->second;
        }
        return std::any();
    }

    /**
     * Get progress callback for sending log messages.
     */
    ProgressCallback* getProgressCallback() {
        return progressCallback_.get();
    }

protected:
    static constexpr const char* DB_PATH = "/tmp/arangodb-benchmark-db";
    static constexpr const char* SNAPSHOT_PATH = "/tmp/arangodb-benchmark-db-snapshot";
    static constexpr const char* DB_NAME = "benchmark";
    static constexpr const char* VERTEX_COLLECTION = "vertices";
    static constexpr const char* EDGE_COLLECTION = "edges";

    std::string dbPath_;
    std::string snapshotPath_;
    std::shared_ptr<ArangoDBClient> arangoUtils_;
    std::shared_ptr<ProgressCallback> progressCallback_;
    std::map<int64_t, std::string> nodeIdsMap_;
    int errorCount_;

    /**
     * Helper: execute operation in batches (count-based).
     * Measures latency per operation.
     */
    template<typename Func>
    std::vector<double> batchExecute(int count, int batchSize, Func operation) {
        std::vector<double> latencies;
        for (int i = 0; i < count; i += batchSize) {
            int batchCount = std::min(batchSize, count - i);
            auto start = std::chrono::high_resolution_clock::now();
            try {
                operation(batchCount);
            } catch (const std::exception& e) {
                errorCount_ += batchCount;
            }
            auto end = std::chrono::high_resolution_clock::now();
            double latency = std::chrono::duration<double, std::micro>(end - start).count() / batchCount;
            latencies.push_back(latency);
        }
        return latencies;
    }

    /**
     * Helper: execute operation in batches (item-based).
     * Measures latency per item.
     */
    template<typename T, typename Func>
    std::vector<double> batchExecute(const std::vector<T>& items, int batchSize, Func operation) {
        std::vector<double> latencies;
        for (size_t i = 0; i < items.size(); i += batchSize) {
            size_t end = std::min(i + batchSize, items.size());
            std::vector<T> batch(items.begin() + i, items.begin() + end);

            auto start = std::chrono::high_resolution_clock::now();
            try {
                operation(batch);
            } catch (const std::exception& e) {
                errorCount_ += batch.size();
            }
            auto endTime = std::chrono::high_resolution_clock::now();
            double latency = std::chrono::duration<double, std::micro>(endTime - start).count() / batch.size();
            latencies.push_back(latency);
        }
        return latencies;
    }
};

} // namespace graphbench
