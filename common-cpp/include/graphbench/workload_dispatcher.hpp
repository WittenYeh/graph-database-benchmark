#pragma once

#include <nlohmann/json.hpp>
#include <graphbench/progress_callback.hpp>
#include <graphbench/benchmark_utils.hpp>
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
        : executor_(executor), datasetPath_(datasetPath) {
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
            } else if (taskType == "ADD_VERTEX") {
                executeBatchTask(workload, result, taskIndex, totalTasks, [this](int count, int batchSize) {
                    return executor_->addVertex(count, batchSize);
                });
            } else if (taskType == "ADD_EDGE") {
                executeAddEdge(workload, result, taskIndex, totalTasks);
            } else if (taskType == "REMOVE_VERTEX") {
                executeRemoveVertex(workload, result, taskIndex, totalTasks);
            } else if (taskType == "REMOVE_EDGE") {
                executeRemoveEdge(workload, result, taskIndex, totalTasks);
            } else if (taskType == "GET_NBRS") {
                executeGetNbrs(workload, result, taskIndex, totalTasks);
            } else {
                result["status"] = "skipped";
                result["message"] = "Task type not recognized: " + taskType;
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
    }

    void executeAddEdge(const json& workload, json& result, int taskIndex, int totalTasks) {
        const auto& parameters = workload.at("parameters");
        std::string label = parameters.at("label").get<std::string>();
        const auto& pairs = parameters.at("pairs");
        std::vector<int> batchSizes = workload.at("batch_sizes").get<std::vector<int>>();

        json batchResults = json::array();

        for (int batchSize : batchSizes) {
            // Send subtask start callback
            std::string subtaskName = "ADD_EDGE (batch_size=" + std::to_string(batchSize) + ")";
            int numOps = pairs.size();
            progressCallback_->sendProgressCallback("subtask_start", subtaskName, "",
                                                   "", -1.0, taskIndex, totalTasks,
                                                   numOps, -1, -1, numOps);

            std::vector<std::pair<std::any, std::any>> edgePairs;
            for (const auto& pair : pairs) {
                int64_t src = pair.at("src").get<int64_t>();
                int64_t dst = pair.at("dst").get<int64_t>();
                // Convert origin IDs to system IDs
                std::any srcSystemId = executor_->getSystemId(src);
                std::any dstSystemId = executor_->getSystemId(dst);
                edgePairs.push_back({srcSystemId, dstSystemId});
            }

            auto startTime = std::chrono::high_resolution_clock::now();
            auto latencies = executor_->addEdge(label, edgePairs, batchSize);
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
            batchResult["validOpsCount"] = pairs.size();
            batchResult["filteredOpsCount"] = 0;
            batchResult["errorCount"] = 0;
            batchResult["originalOpsCount"] = pairs.size();
            batchResult["status"] = "success";

            batchResults.push_back(batchResult);

            // Send subtask complete callback
            progressCallback_->sendProgressCallback("subtask_complete", subtaskName, "",
                                                   "success", duration, taskIndex, totalTasks,
                                                   (int)pairs.size(), (int)pairs.size(), 0);
        }

        result["batch_results"] = batchResults;
        result["status"] = "success";
    }

    void executeRemoveVertex(const json& workload, json& result, int taskIndex, int totalTasks) {
        const auto& parameters = workload.at("parameters");
        auto vertexIds = parameters.at("ids").get<std::vector<int64_t>>();
        std::vector<int> batchSizes = workload.at("batch_sizes").get<std::vector<int>>();

        json batchResults = json::array();

        for (int batchSize : batchSizes) {
            // Send subtask start callback
            std::string subtaskName = "REMOVE_VERTEX (batch_size=" + std::to_string(batchSize) + ")";
            int numOps = vertexIds.size();
            progressCallback_->sendProgressCallback("subtask_start", subtaskName, "",
                                                   "", -1.0, taskIndex, totalTasks,
                                                   numOps, -1, -1, numOps);

            // Convert origin IDs to system IDs
            std::vector<std::any> systemIds;
            for (int64_t originId : vertexIds) {
                systemIds.push_back(executor_->getSystemId(originId));
            }

            auto startTime = std::chrono::high_resolution_clock::now();
            auto latencies = executor_->removeVertex(systemIds, batchSize);
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
            batchResult["validOpsCount"] = vertexIds.size();
            batchResult["filteredOpsCount"] = 0;
            batchResult["errorCount"] = 0;
            batchResult["originalOpsCount"] = vertexIds.size();
            batchResult["status"] = "success";

            batchResults.push_back(batchResult);

            // Send subtask complete callback
            progressCallback_->sendProgressCallback("subtask_complete", subtaskName, "",
                                                   "success", duration, taskIndex, totalTasks,
                                                   (int)vertexIds.size(), (int)vertexIds.size(), 0);
        }

        result["batch_results"] = batchResults;
        result["status"] = "success";
    }

    void executeRemoveEdge(const json& workload, json& result, int taskIndex, int totalTasks) {
        const auto& parameters = workload.at("parameters");
        std::string label = parameters.at("label").get<std::string>();
        const auto& pairs = parameters.at("pairs");
        std::vector<int> batchSizes = workload.at("batch_sizes").get<std::vector<int>>();

        json batchResults = json::array();

        for (int batchSize : batchSizes) {
            // Send subtask start callback
            std::string subtaskName = "REMOVE_EDGE (batch_size=" + std::to_string(batchSize) + ")";
            int numOps = pairs.size();
            progressCallback_->sendProgressCallback("subtask_start", subtaskName, "",
                                                   "", -1.0, taskIndex, totalTasks,
                                                   numOps, -1, -1, numOps);

            std::vector<std::pair<std::any, std::any>> edgePairs;
            for (const auto& pair : pairs) {
                int64_t src = pair.at("src").get<int64_t>();
                int64_t dst = pair.at("dst").get<int64_t>();
                // Convert origin IDs to system IDs
                std::any srcSystemId = executor_->getSystemId(src);
                std::any dstSystemId = executor_->getSystemId(dst);
                edgePairs.push_back({srcSystemId, dstSystemId});
            }

            auto startTime = std::chrono::high_resolution_clock::now();
            auto latencies = executor_->removeEdge(label, edgePairs, batchSize);
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
            batchResult["validOpsCount"] = pairs.size();
            batchResult["filteredOpsCount"] = 0;
            batchResult["errorCount"] = 0;
            batchResult["originalOpsCount"] = pairs.size();
            batchResult["status"] = "success";

            batchResults.push_back(batchResult);

            // Send subtask complete callback
            progressCallback_->sendProgressCallback("subtask_complete", subtaskName, "",
                                                   "success", duration, taskIndex, totalTasks,
                                                   (int)pairs.size(), (int)pairs.size(), 0);
        }

        result["batch_results"] = batchResults;
        result["status"] = "success";
    }

    void executeGetNbrs(const json& workload, json& result, int taskIndex, int totalTasks) {
        const auto& parameters = workload.at("parameters");
        auto vertexIds = parameters.at("ids").get<std::vector<int64_t>>();
        std::string direction = parameters.at("direction").get<std::string>();
        std::vector<int> batchSizes = workload.at("batch_sizes").get<std::vector<int>>();

        json batchResults = json::array();

        for (int batchSize : batchSizes) {
            // Send subtask start callback
            std::string subtaskName = "GET_NBRS (batch_size=" + std::to_string(batchSize) + ")";
            int numOps = vertexIds.size();
            progressCallback_->sendProgressCallback("subtask_start", subtaskName, "",
                                                   "", -1.0, taskIndex, totalTasks,
                                                   numOps, -1, -1, numOps);

            // Convert origin IDs to system IDs
            std::vector<std::any> systemIds;
            for (int64_t originId : vertexIds) {
                systemIds.push_back(executor_->getSystemId(originId));
            }

            auto startTime = std::chrono::high_resolution_clock::now();
            auto latencies = executor_->getNbrs(direction, systemIds, batchSize);
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
            batchResult["validOpsCount"] = vertexIds.size();
            batchResult["filteredOpsCount"] = 0;
            batchResult["errorCount"] = 0;
            batchResult["originalOpsCount"] = vertexIds.size();
            batchResult["status"] = "success";

            batchResults.push_back(batchResult);

            // Send subtask complete callback
            progressCallback_->sendProgressCallback("subtask_complete", subtaskName, "",
                                                   "success", duration, taskIndex, totalTasks,
                                                   (int)vertexIds.size(), (int)vertexIds.size(), 0);
        }

        result["batch_results"] = batchResults;
        result["status"] = "success";
    }

    template<typename Func>
    void executeBatchTask(const json& workload, json& result, int taskIndex, int totalTasks, Func taskFunc) {
        int opsCount = workload["ops_count"];
        std::vector<int> batchSizes = workload.at("batch_sizes").get<std::vector<int>>();

        json batchResults = json::array();

        std::string taskType = workload.at("task_type").get<std::string>();

        for (int batchSize : batchSizes) {
            // Send subtask start callback
            std::string subtaskName = taskType + " (batch_size=" + std::to_string(batchSize) + ")";
            progressCallback_->sendProgressCallback("subtask_start", subtaskName, "",
                                                   "", -1.0, taskIndex, totalTasks,
                                                   opsCount, -1, -1, opsCount);

            auto startTime = std::chrono::high_resolution_clock::now();
            auto latencies = taskFunc(opsCount, batchSize);
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
            batchResult["validOpsCount"] = opsCount;
            batchResult["filteredOpsCount"] = 0;
            batchResult["errorCount"] = 0;
            batchResult["originalOpsCount"] = opsCount;
            batchResult["status"] = "success";

            batchResults.push_back(batchResult);

            // Send subtask complete callback
            progressCallback_->sendProgressCallback("subtask_complete", subtaskName, "",
                                                   "success", duration, taskIndex, totalTasks,
                                                   opsCount, opsCount, 0);
        }

        result["batch_results"] = batchResults;
        result["status"] = "success";
    }
};

} // namespace graphbench
