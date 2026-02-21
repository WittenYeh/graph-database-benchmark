#pragma once

#include <graphbench/benchmark_server.hpp>
#include <graphbench/benchmark_utils.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <map>
#include <functional>

namespace graphbench {

/**
 * Registry for database executors.
 * Maps database type string to executor factory function.
 */
template<typename BaseExecutor>
class ExecutorRegistry {
public:
    using ExecutorFactory = std::function<std::unique_ptr<BaseExecutor>()>;
    using RegistryMap = std::map<std::string, std::pair<std::string, ExecutorFactory>>;

    /**
     * Register an executor type.
     * @param dbType Database type identifier (e.g., "arangodb")
     * @param displayName Display name (e.g., "ArangoDB")
     * @param factory Factory function to create executor instances
     */
    static void registerExecutor(const std::string& dbType,
                                 const std::string& displayName,
                                 ExecutorFactory factory) {
        getRegistry()[dbType] = {displayName, factory};
    }

    /**
     * Get registered executor info.
     */
    static bool getExecutor(const std::string& dbType,
                           std::string& displayName,
                           ExecutorFactory& factory) {
        auto& registry = getRegistry();
        auto it = registry.find(dbType);
        if (it == registry.end()) {
            return false;
        }
        displayName = it->second.first;
        factory = it->second.second;
        return true;
    }

    /**
     * Get all registered database types.
     */
    static std::vector<std::string> getRegisteredTypes() {
        std::vector<std::string> types;
        for (const auto& entry : getRegistry()) {
            types.push_back(entry.first);
        }
        return types;
    }

private:
    static RegistryMap& getRegistry() {
        static RegistryMap registry;
        return registry;
    }
};

/**
 * Generic main function for benchmark servers.
 * Reads DB_TYPE from environment and starts the appropriate server.
 */
template<typename BaseExecutor>
int benchmarkMain(int argc, char* argv[]) {
    try {
        // Get database type from environment
        std::string dbType = BenchmarkUtils::getEnv("DB_TYPE", "");

        if (dbType.empty()) {
            std::cerr << "Error: DB_TYPE environment variable not set" << std::endl;
            auto types = ExecutorRegistry<BaseExecutor>::getRegisteredTypes();
            std::cerr << "Available types: ";
            for (size_t i = 0; i < types.size(); i++) {
                if (i > 0) std::cerr << ", ";
                std::cerr << types[i];
            }
            std::cerr << std::endl;
            return 1;
        }

        // Get executor factory
        std::string displayName;
        typename ExecutorRegistry<BaseExecutor>::ExecutorFactory factory;

        if (!ExecutorRegistry<BaseExecutor>::getExecutor(dbType, displayName, factory)) {
            std::cerr << "Error: Unknown database type: " << dbType << std::endl;
            auto types = ExecutorRegistry<BaseExecutor>::getRegisteredTypes();
            std::cerr << "Available types: ";
            for (size_t i = 0; i < types.size(); i++) {
                if (i > 0) std::cerr << ", ";
                std::cerr << types[i];
            }
            std::cerr << std::endl;
            return 1;
        }

        // Start benchmark server
        startBenchmarkServer<BaseExecutor>(dbType, displayName, factory);

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

} // namespace graphbench
