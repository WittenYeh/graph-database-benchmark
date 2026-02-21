#pragma once

#include <nlohmann/json.hpp>
#include <graphbench/workload_parameters.hpp>

namespace graphbench {

using json = nlohmann::json;

/**
 * ParameterParser - parses JSON workload parameters into C++ parameter objects.
 * Similar to Java's WorkloadDispatcher parameter preprocessing.
 */
template<typename Executor>
class ParameterParser {
public:
    explicit ParameterParser(Executor* executor) : executor_(executor) {}

    /**
     * Parse parameters for ADD_VERTEX task.
     */
    AddVertexParameters parseAddVertexParameters(const json& parameters) {
        AddVertexParameters params;
        params.count = parameters.at("count").get<int>();
        return params;
    }

    /**
     * Parse parameters for ADD_EDGE task.
     * Pre-converts origin IDs to system IDs.
     */
    AddEdgeParameters parseAddEdgeParameters(const json& parameters) {
        AddEdgeParameters params;
        params.label = parameters.at("label").get<std::string>();
        const auto& pairs = parameters.at("pairs");

        for (const auto& pair : pairs) {
            int64_t src = pair.at("src").get<int64_t>();
            int64_t dst = pair.at("dst").get<int64_t>();
            std::any srcSystemId = executor_->getSystemId(src);
            std::any dstSystemId = executor_->getSystemId(dst);
            params.pairs.push_back({srcSystemId, dstSystemId});
        }
        params.originalCount = pairs.size();
        return params;
    }

    /**
     * Parse parameters for REMOVE_VERTEX task.
     * Pre-converts origin IDs to system IDs.
     */
    RemoveVertexParameters parseRemoveVertexParameters(const json& parameters) {
        RemoveVertexParameters params;
        auto vertexIds = parameters.at("ids").get<std::vector<int64_t>>();

        for (int64_t originId : vertexIds) {
            params.systemIds.push_back(executor_->getSystemId(originId));
        }
        params.originalCount = vertexIds.size();
        return params;
    }

    /**
     * Parse parameters for REMOVE_EDGE task.
     * Pre-converts origin IDs to system IDs.
     */
    RemoveEdgeParameters parseRemoveEdgeParameters(const json& parameters) {
        RemoveEdgeParameters params;
        params.label = parameters.at("label").get<std::string>();
        const auto& pairs = parameters.at("pairs");

        for (const auto& pair : pairs) {
            int64_t src = pair.at("src").get<int64_t>();
            int64_t dst = pair.at("dst").get<int64_t>();
            std::any srcSystemId = executor_->getSystemId(src);
            std::any dstSystemId = executor_->getSystemId(dst);
            params.pairs.push_back({srcSystemId, dstSystemId});
        }
        params.originalCount = pairs.size();
        return params;
    }

    /**
     * Parse parameters for GET_NBRS task.
     * Pre-converts origin IDs to system IDs.
     */
    GetNbrsParameters parseGetNbrsParameters(const json& parameters) {
        GetNbrsParameters params;
        params.direction = parameters.at("direction").get<std::string>();
        auto vertexIds = parameters.at("ids").get<std::vector<int64_t>>();

        for (int64_t originId : vertexIds) {
            params.systemIds.push_back(executor_->getSystemId(originId));
        }
        params.originalCount = vertexIds.size();
        return params;
    }

private:
    Executor* executor_;
};

} // namespace graphbench