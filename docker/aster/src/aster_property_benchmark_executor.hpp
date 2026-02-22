#pragma once

#include "aster_benchmark_executor.hpp"
#include <graphbench/property_benchmark_executor.hpp>
#include <graphbench/type_converter.hpp>
#include <rocksdb/graph.h>
#include <string>
#include <vector>
#include <map>
#include <any>

using namespace ROCKSDB_NAMESPACE;

namespace graphbench {

/**
 * Aster property benchmark executor.
 * Extends AsterBenchmarkExecutor with property operations.
 */
class AsterPropertyBenchmarkExecutor : public PropertyBenchmarkExecutor<AsterPropertyBenchmarkExecutor>,
                                       public AsterBenchmarkExecutor {
public:
    AsterPropertyBenchmarkExecutor() : AsterBenchmarkExecutor() {}

    std::string getDatabaseNameImpl() const {
        return "Aster (Property)";
    }

    // Property operation implementations
    std::vector<double> updateVertexPropertyImpl(
            const std::vector<std::tuple<std::any, std::string, std::string>>& updates,
            int batchSize) {
        return executeBatchOperation(updates, batchSize,
            [this](const std::tuple<std::any, std::string, std::string>& update) {
                const auto& [systemId, key, value] = update;
                node_id_t nodeId = std::any_cast<node_id_t>(systemId);

                Property prop;
                prop.name = key;
                prop.value = value;

                Status s = graph_->AddVertexProperty(nodeId, prop);
                if (!s.ok()) {
                    errorCount_++;
                }
            });
    }

    std::vector<double> updateEdgePropertyImpl(
            const std::vector<std::tuple<std::any, std::any, std::string, std::string>>& updates,
            int batchSize) {
        return executeBatchOperation(updates, batchSize,
            [this](const std::tuple<std::any, std::any, std::string, std::string>& update) {
                const auto& [srcSystemId, dstSystemId, key, value] = update;
                node_id_t src = std::any_cast<node_id_t>(srcSystemId);
                node_id_t dst = std::any_cast<node_id_t>(dstSystemId);

                Property prop;
                prop.name = key;
                prop.value = value;

                Status s = graph_->AddEdgeProperty(src, dst, prop);
                if (!s.ok()) {
                    errorCount_++;
                }
            });
    }

    std::vector<double> getVertexByPropertyImpl(
            const std::vector<std::pair<std::string, std::string>>& queries,
            int batchSize) {
        return executeBatchOperation(queries, batchSize,
            [this](const std::pair<std::string, std::string>& query) {
                const auto& [key, value] = query;

                Property prop;
                prop.name = key;
                prop.value = value;

                std::vector<node_id_t> results = graph_->GetVerticesWithProperty(prop);

                // Consume results
                for (const auto& nodeId : results) {
                    volatile node_id_t consumed = nodeId;
                    (void)consumed;
                }
            });
    }

    std::vector<double> getEdgeByPropertyImpl(
            const std::vector<std::pair<std::string, std::string>>& queries,
            int batchSize) {
        return executeBatchOperation(queries, batchSize,
            [this](const std::pair<std::string, std::string>& query) {
                const auto& [key, value] = query;

                Property prop;
                prop.name = key;
                prop.value = value;

                std::vector<std::pair<node_id_t, node_id_t>> results = graph_->GetEdgesWithProperty(prop);

                // Consume results
                for (const auto& edge : results) {
                    volatile node_id_t src = edge.first;
                    volatile node_id_t dst = edge.second;
                    (void)src;
                    (void)dst;
                }
            });
    }
};

} // namespace graphbench