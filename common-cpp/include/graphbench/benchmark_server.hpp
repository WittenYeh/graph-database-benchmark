#pragma once

#include <httplib.h>
#include <nlohmann/json.hpp>
#include <graphbench/workload_dispatcher.hpp>
#include <graphbench/benchmark_utils.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <functional>

namespace graphbench {

using json = nlohmann::json;

/**
 * Generic HTTP server for benchmark execution.
 * Template parameter Executor should be a BenchmarkExecutor or PropertyBenchmarkExecutor.
 *
 * Usage:
 *   BenchmarkServer<MyExecutor> server(port, "mydb", []() {
 *       return std::make_unique<MyExecutor>();
 *   });
 *   server.start();
 */
template<typename Executor>
class BenchmarkServer {
public:
    using ExecutorFactory = std::function<std::unique_ptr<Executor>()>;

    /**
     * Constructor.
     * @param port HTTP server port
     * @param databaseName Database display name
     * @param executorFactory Factory function to create executor instances
     */
    BenchmarkServer(int port, const std::string& databaseName, ExecutorFactory executorFactory)
        : port_(port), databaseName_(databaseName), executorFactory_(executorFactory) {}

    /**
     * Start the HTTP server.
     * Blocks until server is stopped.
     */
    void start() {
        httplib::Server server;

        // Health check endpoint
        server.Get("/health", [](const httplib::Request&, httplib::Response& res) {
            json response = {{"status", "ok"}};
            res.set_content(response.dump(), "application/json");
        });

        // Execute benchmark endpoint
        server.Post("/execute", [this](const httplib::Request& req, httplib::Response& res) {
            try {
                json request = json::parse(req.body);
                std::string datasetName = request.at("dataset_name").get<std::string>();
                std::string datasetPath = request.at("dataset_path").get<std::string>();

                std::cout << "Executing benchmark for dataset: " << datasetName << std::endl;

                // Execute full benchmark workflow
                json results = executeBenchmark(datasetPath);

                res.set_content(results.dump(), "application/json");
            } catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << std::endl;
                json error = {{"error", e.what()}};
                res.status = 500;
                res.set_content(error.dump(), "application/json");
            }
        });

        std::cout << "Starting " << databaseName_ << " Benchmark Server on port " << port_ << std::endl;
        server.listen("0.0.0.0", port_);
    }

private:
    int port_;
    std::string databaseName_;
    ExecutorFactory executorFactory_;

    /**
     * Execute full benchmark workflow.
     * Creates executor, uses WorkloadDispatcher to execute all workload files.
     */
    json executeBenchmark(const std::string& datasetPath) {
        // Create executor instance
        auto executor = executorFactory_();

        // Use WorkloadDispatcher to execute benchmark
        std::string workloadDir = "/data/workloads";
        WorkloadDispatcher<Executor> dispatcher(executor.get(), datasetPath);
        return dispatcher.executeBenchmark(workloadDir);
    }
};

/**
 * Helper function to create and start a benchmark server from environment variables.
 * Expects DB_TYPE and API_PORT environment variables.
 *
 * @param databaseType Database type identifier (e.g., "arangodb", "neo4j")
 * @param displayName Database display name
 * @param executorFactory Factory function to create executor instances
 */
template<typename Executor>
void startBenchmarkServer(const std::string& databaseType,
                         const std::string& displayName,
                         typename BenchmarkServer<Executor>::ExecutorFactory executorFactory) {
    try {
        // Get configuration from environment variables
        std::string dbType = BenchmarkUtils::getEnv("DB_TYPE", databaseType);
        int port = std::stoi(BenchmarkUtils::getEnv("API_PORT", "50082"));

        // Verify database type matches
        if (dbType != databaseType) {
            std::cerr << "Error: DB_TYPE=" << dbType << " does not match expected type: "
                     << databaseType << std::endl;
            std::exit(1);
        }

        // Create and start server
        BenchmarkServer<Executor> server(port, displayName, executorFactory);
        server.start();

    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        std::exit(1);
    }
}

} // namespace graphbench
