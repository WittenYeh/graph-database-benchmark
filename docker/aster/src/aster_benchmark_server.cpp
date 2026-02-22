#include "aster_benchmark_executor.hpp"
#include "aster_property_benchmark_executor.hpp"
#include <graphbench/benchmark_main.hpp>

using namespace graphbench;

/**
 * Register Aster executors and start server.
 * This file only contains executor registration - main() is in common-cpp.
 */

// Register structural executor
static bool registered_structural = []() {
    ExecutorRegistry<AsterBenchmarkExecutor>::registerExecutor(
        "aster",
        "Aster",
        []() { return std::make_unique<AsterBenchmarkExecutor>(); }
    );
    return true;
}();

// Register property executor
static bool registered_property = []() {
    ExecutorRegistry<AsterBenchmarkExecutor>::registerExecutor(
        "aster-property",
        "Aster (Property)",
        []() { return std::make_unique<AsterPropertyBenchmarkExecutor>(); }
    );
    return true;
}();

int main(int argc, char* argv[]) {
    return benchmarkMain<AsterBenchmarkExecutor>(argc, argv);
}
