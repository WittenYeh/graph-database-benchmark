#pragma once

#include <nlohmann/json.hpp>
#include <graphbench/progress_callback.hpp>
#include <graphbench/benchmark_utils.hpp>
#include <graphbench/workload_parameters.hpp>
#include <graphbench/parameter_parser.hpp>
#include <filesystem>
#include <fstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <memory>

namespace graphbench {

using json = nlohmann::json;
namespace fs = std::filesystem;

/**
 * WorkloadDispatcher - reads and executes workload files.
 * Template parameter Executor should be a BenchmarkExecutor or PropertyBenchmarkExecutor.
 */
template<typename Executor>
class WorkloadDispatcher {
public:
    WorkloadDispatcher(Executor* executor, const std::string& datasetPath)
        : executor_(executor), datasetPath_(datasetPath), parameterParser_(executor) {
        // Get progress callback URL from environment
        std::string callbackUrl = BenchmarkUtils::getEnv("PROGRESS_CALLBACK_URL", "");
        progressCallback_ = std::make_shared<ProgressCallback>(callbackUrl);
    }

    /**
     * Execute all workload files in the specified directory.
     * Returns results in the same format as Java implementation.
     */
    json executeBenchmark(const std::string& workloadDir) {
        // Initialize database
        executor_->initDatabase();

        // Load workload files
        std::vector<fs::path> workloadFiles;
        for (const auto& entry : fs::directory_iterator(workloadDir)) {
            if (entry.path().extension() == ".json") {
                workloadFiles.push_back(entry.path());
            }
        }
        std::sort(workloadFiles.begin(), workloadFiles.end());

        if (workloadFiles.empty()) {
            throw std::runtime_error("No workload files found in: " + workloadDir);
        }

        // Build metadata
        json metadata;
        metadata["database"] = executor_->getDatabaseName();

        fs::path datasetPathObj(datasetPath_);
        std::string datasetName = datasetPathObj.filename().string();
        metadata["dataset"] = datasetName;
        metadata["datasetPath"] = datasetPath_;
        metadata["timestamp"] = getCurrentTimestamp();

        // Get workload name from directory or first file
        std::string workloadName = extractWorkloadName(workloadFiles[0].parent_path().filename().string());
        metadata["workload"] = workloadName;

        // Execute tasks
        json results = json::array();
        int totalTasks = workloadFiles.size();

        for (size_t i = 0; i < workloadFiles.size(); i++) {
            const auto& workloadFile = workloadFiles[i];
            std::cout << "Executing: " << workloadFile.filename() << std::endl;

            json result = executeWorkloadFile(workloadFile, i, totalTasks);
            results.push_back(result);

            // If LOAD_GRAPH failed, stop execution
            if (result["task_type"] == "LOAD_GRAPH" && result["status"] == "failed") {
                std::cerr << "❌ LOAD_GRAPH task failed, stopping benchmark execution" << std::endl;
                throw std::runtime_error("LOAD_GRAPH failed");
            }
        }

        // Shutdown database
        executor_->shutdown();

        // Build final response
        json response;
        response["metadata"] = metadata;
        response["results"] = results;

        return response;
    }

private:
    Executor* executor_;
    std::string datasetPath_;
    std::shared_ptr<ProgressCallback> progressCallback_;
    ParameterParser<Executor> parameterParser_;

    std::string getCurrentTimestamp() {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::gmtime(&time_t), "%Y-%m-%dT%H:%M:%SZ");
        return ss.str();
    }

    std::string extractWorkloadName(const std::string& dirname) {
        // Extract workload name from directory like "arangodb_delaunay_n13"
        size_t lastUnderscore = dirname.rfind('_');
        if (lastUnderscore != std::string::npos) {
            size_t secondLastUnderscore = dirname.rfind('_', lastUnderscore - 1);
            if (secondLastUnderscore != std::string::npos) {
                return dirname.substr(secondLastUnderscore + 1);
            }
        }
        return "unknown";
    }

    json executeWorkloadFile(const fs::path& workloadFile, int taskIndex, int totalTasks) {
        std::ifstream file(workloadFile);
        if (!file.is_open()) {
            throw std::runtime_error("Cannot open workload file: " + workloadFile.string());
        }

        json workload;
        file >> workload;

        std::string taskType = workload.at("task_type").get<std::string>();
        int opsCount = workload.value("ops_count", 0);

        json result;
        result["task_type"] = taskType;
        result["ops_count"] = opsCount;

        // Send task start callback
        progressCallback_->sendProgressCallback("task_start", taskType, workloadFile.filename().string(),
                                               "", -1.0, taskIndex, totalTasks);

        try {
            auto startTime = std::chrono::high_resolution_clock::now();

            if (taskType == "LOAD_GRAPH") {
                executeLoadGraph(result);
            } else {
                // Get parameters for all non-LOAD_GRAPH tasks
                const auto& parameters = workload.at("parameters");

                if (taskType == "ADD_VERTEX") {
                    auto params = parameterParser_.parseAddVertexParameters(parameters);
                    executeVaryBatchSizeBench(workload, result, taskIndex, totalTasks, params.count,
                        [this, &params](int batchSize) {
                            return executor_->addVertex(params.count, batchSize);
                        });
                } else if (taskType == "ADD_EDGE") {
                    auto params = parameterParser_.parseAddEdgeParameters(parameters);
                    executeVaryBatchSizeBench(workload, result, taskIndex, totalTasks, params.originalCount,
                        [this, &params](int batchSize) {
                            return executor_->addEdge(params.label, params.pairs, batchSize);
                        });
                } else if (taskType == "REMOVE_VERTEX") {
                    auto params = parameterParser_.parseRemoveVertexParameters(parameters);
                    executeVaryBatchSizeBench(workload, result, taskIndex, totalTasks, params.originalCount,
                        [this, &params](int batchSize) {
                            return executor_->removeVertex(params.systemIds, batchSize);
                        });
                } else if (taskType == "REMOVE_EDGE") {
                    auto params = parameterParser_.parseRemoveEdgeParameters(parameters);
                    executeVaryBatchSizeBench(workload, result, taskIndex, totalTasks, params.originalCount,
                        [this, &params](int batchSize) {
                            return executor_->removeEdge(params.label, params.pairs, batchSize);
                        });
                } else if (taskType == "GET_NBRS") {
                    auto params = parameterParser_.parseGetNbrsParameters(parameters);
                    executeVaryBatchSizeBench(workload, result, taskIndex, totalTasks, params.originalCount,
                        [this, &params](int batchSize) {
                            return executor_->getNbrs(params.direction, params.systemIds, batchSize);
                        });
                } else {
                    result["status"] = "skipped";
                    result["message"] = "Task type not recognized: " + taskType;
                }
            }

            auto endTime = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration<double>(endTime - startTime).count();
            result["durationSeconds"] = duration;

            // Send task complete callback
            progressCallback_->sendProgressCallback("task_complete", taskType, workloadFile.filename().string(),
                                                   "success", duration, taskIndex, totalTasks);

        } catch (const std::exception& e) {
            result["status"] = "failed";
            result["error"] = e.what();

            // Send task complete callback with error
            progressCallback_->sendProgressCallback("task_complete", taskType, workloadFile.filename().string(),
                                                   "failed", -1.0, taskIndex, totalTasks);
        }

        return result;
    }

    void executeLoadGraph(json& result) {
        auto loadResult = executor_->loadGraph(datasetPath_);
        result["nodes"] = std::any_cast<int>(loadResult["nodes"]);
        result["edges"] = std::any_cast<int>(loadResult["edges"]);
        result["status"] = "success";

        // Create snapshot after loading graph
        try {
            progressCallback_->sendProgressCallback("snapshot_start", "SNAPSHOT", "", "", -1.0, 0, 0);
            executor_->snapGraph();
            progressCallback_->sendProgressCallback("snapshot_complete", "SNAPSHOT", "", "success", -1.0, 0, 0);
        } catch (const std::exception& e) {
            progressCallback_->sendProgressCallback("snapshot_complete", "SNAPSHOT", "", "failed", -1.0, 0, 0);
            std::cerr << "Warning: Failed to create snapshot: " << e.what() << std::endl;
        }
    }

    /**
     * Execute a task with automatic restore before each batch size.
     * Similar to Java's transactionalExecute method.
     *
     * @param workload Workload JSON containing task_type and batch_sizes
     * @param result Result JSON to populate
     * @param taskIndex Current task index
     * @param totalTasks Total number of tasks
     * @param numOps Number of operations for this task
     * @param taskFunc Function that executes the actual task for a given batch size
     */
    template<typename Func>
    void executeVaryBatchSizeBench(const json& workload, json& result, int taskIndex, int totalTasks,
                         int numOps, Func taskFunc) {
        std::vector<int> batchSizes = workload.at("batch_sizes").get<std::vector<int>>();
        json batchResults = json::array();
        std::string taskType = workload.at("task_type").get<std::string>();

        for (int batchSize : batchSizes) {
            // Restore graph to clean state before executing workload
            try {
                progressCallback_->sendProgressCallback("restore_start", "RESTORE", "", "", -1.0, taskIndex, totalTasks);
                executor_->restoreGraph();
                progressCallback_->sendProgressCallback("restore_complete", "RESTORE", "", "success", -1.0, taskIndex, totalTasks);
            } catch (const std::exception& e) {
                progressCallback_->sendProgressCallback("restore_complete", "RESTORE", "", "failed", -1.0, taskIndex, totalTasks);
                std::cerr << "Warning: Failed to restore graph: " << e.what() << std::endl;
            }

            // Send subtask start callback
            std::string subtaskName = taskType + " (batch_size=" + std::to_string(batchSize) + ")";
            progressCallback_->sendProgressCallback("subtask_start", subtaskName, "",
                                                   "", -1.0, taskIndex, totalTasks,
                                                   numOps, -1, -1, numOps);

            auto startTime = std::chrono::high_resolution_clock::now();
            auto latencies = taskFunc(batchSize);
            auto endTime = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration<double>(endTime - startTime).count();

            double avgLatency = 0.0;
            for (double lat : latencies) {
                avgLatency += lat;
            }
            avgLatency /= latencies.size();

            json batchResult;
            batchResult["batch_size"] = batchSize;
            batchResult["latency_us"] = avgLatency;
            batchResult["validOpsCount"] = numOps;
            batchResult["filteredOpsCount"] = 0;
            batchResult["errorCount"] = 0;
            batchResult["originalOpsCount"] = numOps;
            batchResult["status"] = "success";

            batchResults.push_back(batchResult);

            // Send subtask complete callback
            progressCallback_->sendProgressCallback("subtask_complete", subtaskName, "",
                                                   "success", duration, taskIndex, totalTasks,
                                                   numOps, numOps, 0);
        }

        result["batch_results"] = batchResults;
        result["status"] = "success";
    }
};

} // namespace graphbench
