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
     * Pre-converts origin IDs to system IDs and filters out non-existent vertices.
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

            // Only add if both vertices exist
            if (srcSystemId.has_value() && dstSystemId.has_value()) {
                params.pairs.push_back({srcSystemId, dstSystemId});
            }
        }
        params.originalCount = pairs.size();
        return params;
    }

    /**
     * Parse parameters for REMOVE_VERTEX task.
     * Pre-converts origin IDs to system IDs and filters out non-existent vertices.
     */
    RemoveVertexParameters parseRemoveVertexParameters(const json& parameters) {
        RemoveVertexParameters params;
        auto vertexIds = parameters.at("ids").get<std::vector<int64_t>>();

        for (int64_t originId : vertexIds) {
            std::any systemId = executor_->getSystemId(originId);
            if (systemId.has_value()) {
                params.systemIds.push_back(systemId);
            }
        }
        params.originalCount = vertexIds.size();
        return params;
    }

    /**
     * Parse parameters for REMOVE_EDGE task.
     * Pre-converts origin IDs to system IDs and filters out non-existent edges.
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

            // Only add if both vertices exist
            if (srcSystemId.has_value() && dstSystemId.has_value()) {
                params.pairs.push_back({srcSystemId, dstSystemId});
            }
        }
        params.originalCount = pairs.size();
        return params;
    }

    /**
     * Parse parameters for GET_NBRS task.
     * Pre-converts origin IDs to system IDs and filters out non-existent vertices.
     */
    GetNbrsParameters parseGetNbrsParameters(const json& parameters) {
        GetNbrsParameters params;
        params.direction = parameters.at("direction").get<std::string>();
        auto vertexIds = parameters.at("ids").get<std::vector<int64_t>>();

        for (int64_t originId : vertexIds) {
            std::any systemId = executor_->getSystemId(originId);
            if (systemId.has_value()) {
                params.systemIds.push_back(systemId);
            }
        }
        params.originalCount = vertexIds.size();
        return params;
    }

private:
    Executor* executor_;
};

} // namespace graphbench