#include "arangodb_benchmark_executor.hpp"
#include <graphbench/benchmark_server.hpp>
#include <graphbench/benchmark_utils.hpp>
#include <iostream>
#include <memory>

using namespace graphbench;

/**
 * Main entry point for ArangoDB structural benchmark server.
 */
int main(int argc, char* argv[]) {
    try {
        // Structural benchmark executor
        startBenchmarkServer<ArangoDBBenchmarkExecutor>(
            "arangodb",
            "ArangoDB",
            []() { return std::make_unique<ArangoDBBenchmarkExecutor>(); }
        );
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
