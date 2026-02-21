#include "arangodb_benchmark_executor.hpp"
#include "arangodb_property_benchmark_executor.hpp"
#include <graphbench/benchmark_main.hpp>

using namespace graphbench;

/**
 * Register ArangoDB executors and start server.
 * This file only contains executor registration - main() is in common-cpp.
 */

// Register structural executor
static bool registered_structural = []() {
    ExecutorRegistry<ArangoDBBenchmarkExecutor>::registerExecutor(
        "arangodb",
        "ArangoDB",
        []() { return std::make_unique<ArangoDBBenchmarkExecutor>(); }
    );
    return true;
}();

// Register property executor
static bool registered_property = []() {
    ExecutorRegistry<ArangoDBBenchmarkExecutor>::registerExecutor(
        "arangodb-property",
        "ArangoDB (Property)",
        []() { return std::make_unique<ArangoDBPropertyBenchmarkExecutor>(); }
    );
    return true;
}();

int main(int argc, char* argv[]) {
    return benchmarkMain<ArangoDBBenchmarkExecutor>(argc, argv);
}
