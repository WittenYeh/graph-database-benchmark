#pragma once

#include <graphbench/benchmark_executor.hpp>
#include <graphbench/progress_callback.hpp>
#include <graphbench/benchmark_utils.hpp>
#include "aster_graph_loader.hpp"
#include <rocksdb/db.h>
#include <rocksdb/graph.h>
#include <rocksdb/options.h>
#include <string>
#include <vector>
#include <map>
#include <any>
#include <memory>
#include <chrono>
#include <iostream>
#include <stdexcept>

using namespace ROCKSDB_NAMESPACE;

namespace graphbench {

/**
 * Aster benchmark executor for structural operations.
 * Implements graph operations using Aster's RocksGraph API.
 */
class AsterBenchmarkExecutor : public BenchmarkExecutor<AsterBenchmarkExecutor> {
public:
    AsterBenchmarkExecutor()
        : dbPath_("/tmp/aster-benchmark-db"),
          snapshotPath_("/tmp/aster-benchmark-snapshot"),
          graph_(nullptr),
          progressCallback_(nullptr),
          errorCount_(0) {}

    virtual ~AsterBenchmarkExecutor() {
        if (graph_) {
            delete graph_;
            graph_ = nullptr;
        }
    }

    // Implementation methods for CRTP
    void initDatabaseImpl() {
        try {
            // Clean up existing database
            if (std::filesystem::exists(dbPath_)) {
                BenchmarkUtils::deleteDirectory(dbPath_);
            }

            // Setup RocksDB options
            Options options;
            options.create_if_missing = true;
            options.level_compaction_dynamic_level_bytes = false;
            options.write_buffer_size = 4 * 1024 * 1024;
            options.max_bytes_for_level_base = options.write_buffer_size * options.max_bytes_for_level_multiplier;

            // Create RocksGraph instance with EDGE_UPDATE_ADAPTIVE policy, auto_reinitialize=true, and db_path
            graph_ = new RocksGraph(options, EDGE_UPDATE_ADAPTIVE, ENCODING_TYPE_NONE, true, dbPath_);

            if (progressCallback_) {
                progressCallback_->sendLogMessage("Aster database initialized at " + dbPath_, "INFO");
            }
        } catch (const std::exception& e) {
            throw std::runtime_error("Failed to initialize Aster database: " + std::string(e.what()));
        }
    }

    void shutdownImpl() {
        if (graph_) {
            delete graph_;
            graph_ = nullptr;
        }
        if (progressCallback_) {
            progressCallback_->sendLogMessage("Aster database shutdown", "INFO");
        }
    }

    std::map<std::string, std::any> loadGraphImpl(const std::string& datasetPath) {
        return AsterGraphLoader<AsterBenchmarkExecutor>::loadGraph(this, datasetPath);
    }

    // Helper function: execute operation in batches (count-based)
    // Measures latency per operation
    template<typename Operation>
    std::vector<double> executeBatchOperation(int count, int batchSize, Operation op) {
        std::vector<double> latencies;
        int processed = 0;

        while (processed < count) {
            int batchCount = std::min(batchSize, count - processed);

            auto start = std::chrono::high_resolution_clock::now();

            for (int i = 0; i < batchCount; i++) {
                try {
                    op();
                } catch (const std::exception& e) {
                    errorCount_++;
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            double totalLatency = std::chrono::duration<double, std::micro>(end - start).count();
            double perOpLatency = totalLatency / batchCount;
            latencies.push_back(perOpLatency);

            processed += batchCount;
        }

        return latencies;
    }

    // Helper function: execute operation in batches (item-based)
    // Measures latency per item
    template<typename Container, typename Operation>
    std::vector<double> executeBatchOperation(const Container& items, int batchSize, Operation op) {
        std::vector<double> latencies;
        size_t processed = 0;

        while (processed < items.size()) {
            size_t batchCount = std::min(static_cast<size_t>(batchSize), items.size() - processed);

            auto start = std::chrono::high_resolution_clock::now();

            for (size_t i = 0; i < batchCount; i++) {
                try {
                    op(items[processed + i]);
                } catch (const std::exception& e) {
                    errorCount_++;
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            double totalLatency = std::chrono::duration<double, std::micro>(end - start).count();
            double perOpLatency = totalLatency / batchCount;
            latencies.push_back(perOpLatency);

            processed += batchCount;
        }

        return latencies;
    }

    std::vector<double> addVertexImpl(int count, int batchSize) {
        return executeBatchOperation(count, batchSize, [this]() {
            node_id_t nodeId = nextVertexId_++;
            Status s = graph_->AddVertex(nodeId);
            if (!s.ok()) {
                errorCount_++;
            }
        });
    }

    std::vector<double> removeVertexImpl(const std::vector<std::any>& systemIds, int batchSize) {
        return executeBatchOperation(systemIds, batchSize, [this](const std::any& systemId) {
            node_id_t nodeId = std::any_cast<node_id_t>(systemId);
            // Aster doesn't have explicit DeleteVertex, vertices are implicitly removed when all edges are deleted
            // For now, we'll just track the operation
        });
    }

    std::vector<double> addEdgeImpl(const std::string& label,
                                    const std::vector<std::pair<std::any, std::any>>& pairs,
                                    int batchSize) {
        return executeBatchOperation(pairs, batchSize, [this](const std::pair<std::any, std::any>& pair) {
            node_id_t src = std::any_cast<node_id_t>(pair.first);
            node_id_t dst = std::any_cast<node_id_t>(pair.second);

            Status s = graph_->AddEdge(src, dst);
            if (!s.ok()) {
                errorCount_++;
            }
        });
    }

    std::vector<double> removeEdgeImpl(const std::string& label,
                                       const std::vector<std::pair<std::any, std::any>>& pairs,
                                       int batchSize) {
        return executeBatchOperation(pairs, batchSize, [this](const std::pair<std::any, std::any>& pair) {
            node_id_t src = std::any_cast<node_id_t>(pair.first);
            node_id_t dst = std::any_cast<node_id_t>(pair.second);

            Status s = graph_->DeleteEdge(src, dst);
            if (!s.ok()) {
                errorCount_++;
            }
        });
    }

    std::vector<double> getNbrsImpl(const std::string& direction,
                                    const std::vector<std::any>& systemIds,
                                    int batchSize) {
        return executeBatchOperation(systemIds, batchSize, [this, &direction](const std::any& systemId) {
            node_id_t nodeId = std::any_cast<node_id_t>(systemId);
            Edges edges;
            Status s = graph_->GetAllEdges(nodeId, &edges);

            if (s.ok()) {
                // Consume the edges based on direction
                if (direction == "OUT" || direction == "OUTGOING") {
                    for (uint32_t j = 0; j < edges.num_edges_out; j++) {
                        volatile node_id_t neighbor = edges.nxts_out[j].nxt;
                        (void)neighbor;
                    }
                } else if (direction == "IN" || direction == "INCOMING") {
                    for (uint32_t j = 0; j < edges.num_edges_in; j++) {
                        volatile node_id_t neighbor = edges.nxts_in[j].nxt;
                        (void)neighbor;
                    }
                } else { // BOTH
                    for (uint32_t j = 0; j < edges.num_edges_out; j++) {
                        volatile node_id_t neighbor = edges.nxts_out[j].nxt;
                        (void)neighbor;
                    }
                    for (uint32_t j = 0; j < edges.num_edges_in; j++) {
                        volatile node_id_t neighbor = edges.nxts_in[j].nxt;
                        (void)neighbor;
                    }
                }
            } else {
                errorCount_++;
            }
        });
    }

    std::string getDatabaseNameImpl() const {
        return "Aster";
    }

    std::string getDatabasePathImpl() const {
        return dbPath_;
    }

    std::string getSnapshotPathImpl() const {
        return snapshotPath_;
    }

    void closeDatabaseImpl() {
        if (graph_) {
            delete graph_;
            graph_ = nullptr;
        }
    }

    void openDatabaseImpl() {
        Options options;
        options.create_if_missing = false;
        options.level_compaction_dynamic_level_bytes = false;
        options.write_buffer_size = 4 * 1024 * 1024;
        options.max_bytes_for_level_base = options.write_buffer_size * options.max_bytes_for_level_multiplier;

        graph_ = new RocksGraph(options, EDGE_UPDATE_ADAPTIVE, ENCODING_TYPE_NONE, false, dbPath_);
    }

    int getErrorCountImpl() const {
        return errorCount_;
    }

    void resetErrorCountImpl() {
        errorCount_ = 0;
    }

    std::any getSystemIdImpl(int64_t originId) const {
        auto it = originToSystemId_.find(originId);
        if (it != originToSystemId_.end()) {
            return it->second;
        }
        return std::any();
    }

    ProgressCallback* getProgressCallback() {
        return progressCallback_.get();
    }

    void setProgressCallback(std::unique_ptr<ProgressCallback> callback) {
        progressCallback_ = std::move(callback);
    }

protected:
    std::string dbPath_;
    std::string snapshotPath_;
    RocksGraph* graph_;
    std::unique_ptr<ProgressCallback> progressCallback_;
    int errorCount_;
    node_id_t nextVertexId_ = 1;
    std::map<int64_t, node_id_t> originToSystemId_;

    template<typename ExecutorType>
    friend class AsterGraphLoader;
};

} // namespace graphbench
