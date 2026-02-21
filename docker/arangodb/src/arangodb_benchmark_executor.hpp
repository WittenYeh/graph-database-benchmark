#pragma once

#include <graphbench/benchmark_executor.hpp>
#include <graphbench/property_benchmark_executor.hpp>
#include <graphbench/progress_callback.hpp>
#include <graphbench/benchmark_utils.hpp>
#include <fuerte/fuerte.h>
#include <map>
#include <string>
#include <vector>
#include <memory>
#include <chrono>

namespace graphbench {

/**
 * ArangoDB benchmark executor using Fuerte C++ driver.
 * Implements structural graph operations.
 */
class ArangoDBBenchmarkExecutor : public BenchmarkExecutor<ArangoDBBenchmarkExecutor> {
public:
    ArangoDBBenchmarkExecutor();
    ~ArangoDBBenchmarkExecutor();

    // Required implementations for CRTP
    void initDatabaseImpl();
    void shutdownImpl();
    std::map<std::string, std::any> loadGraphImpl(const std::string& datasetPath);

    std::vector<double> addVertexImpl(int count, int batchSize);
    std::vector<double> removeVertexImpl(const std::vector<std::any>& systemIds, int batchSize);
    std::vector<double> addEdgeImpl(const std::string& label,
                                    const std::vector<std::pair<std::any, std::any>>& pairs,
                                    int batchSize);
    std::vector<double> removeEdgeImpl(const std::string& label,
                                       const std::vector<std::pair<std::any, std::any>>& pairs,
                                       int batchSize);
    std::vector<double> getNbrsImpl(const std::string& direction,
                                    const std::vector<std::any>& systemIds,
                                    int batchSize);

    std::string getDatabaseNameImpl() const { return "arangodb"; }
    std::string getDatabasePathImpl() const { return dbPath_; }
    std::string getSnapshotPathImpl() const { return snapshotPath_; }

    void closeDatabaseImpl();
    void openDatabaseImpl();

    int getErrorCountImpl() const { return errorCount_; }
    void resetErrorCountImpl() { errorCount_ = 0; }

    std::any getSystemIdImpl(int64_t originId) const;

private:
    static constexpr const char* DB_PATH = "/tmp/arangodb-benchmark-db";
    static constexpr const char* SNAPSHOT_PATH = "/tmp/arangodb-benchmark-db-snapshot";
    static constexpr const char* DB_NAME = "benchmark";
    static constexpr const char* VERTEX_COLLECTION = "vertices";
    static constexpr const char* EDGE_COLLECTION = "edges";
    static constexpr int LOAD_BATCH_SIZE = 10000;

    std::string dbPath_;
    std::string snapshotPath_;
    std::shared_ptr<arangodb::fuerte::Connection> connection_;
    std::unique_ptr<ProgressCallback> progressCallback_;
    std::map<int64_t, std::string> nodeIdsMap_;
    int errorCount_;

    // Helper methods
    void createDatabase();
    void dropDatabase();
    void createCollections();
    std::string executeAQL(const std::string& query,
                          const std::map<std::string, std::any>& bindVars = {});

    template<typename Func>
    std::vector<double> batchExecute(int count, int batchSize, Func operation);

    template<typename T, typename Func>
    std::vector<double> batchExecute(const std::vector<T>& items, int batchSize, Func operation);
};

/**
 * ArangoDB property benchmark executor.
 */
class ArangoDBPropertyBenchmarkExecutor : public PropertyBenchmarkExecutor<ArangoDBPropertyBenchmarkExecutor>,
                                          public ArangoDBBenchmarkExecutor {
public:
    std::string getDatabaseNameImpl() const { return "arangodb-property"; }

    std::vector<double> updateVertexPropertyImpl(const std::vector<VertexUpdate>& updates, int batchSize);
    std::vector<double> updateEdgePropertyImpl(const std::string& label,
                                               const std::vector<EdgeUpdate>& updates,
                                               int batchSize);
    std::vector<double> getVertexByPropertyImpl(const std::vector<PropertyQuery>& queries, int batchSize);
    std::vector<double> getEdgeByPropertyImpl(const std::vector<PropertyQuery>& queries, int batchSize);
};

} // namespace graphbench
