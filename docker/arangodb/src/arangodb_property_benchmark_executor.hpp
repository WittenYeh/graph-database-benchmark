#pragma once

#include "arangodb_benchmark_executor.hpp"
#include "arangodb_graph_loader.hpp"
#include <graphbench/property_benchmark_executor.hpp>

namespace graphbench {

/**
 * ArangoDB property benchmark executor.
 * Extends structural executor with property-related operations.
 * Loads graph with properties and creates indexes for efficient property queries.
 */
class ArangoDBPropertyBenchmarkExecutor : public PropertyBenchmarkExecutor<ArangoDBPropertyBenchmarkExecutor>,
                                          public ArangoDBBenchmarkExecutor {
public:
    std::string getDatabaseNameImpl() const { return "arangodb-property"; }

    /**
     * Load graph with properties from CSV files.
     * Creates property indexes after loading for efficient queries.
     */
    std::map<std::string, std::any> loadGraphImpl(const std::string& datasetPath) {
        // Load graph with properties enabled
        ArangoDBGraphLoader loader(arangoUtils_, DB_NAME, progressCallback_, true);
        auto result = loader.load(datasetPath);
        nodeIdsMap_ = loader.getNodeIdsMap();
        metadata_ = loader.getMetadata();

        // Create property indexes for efficient queries
        loader.createPropertyIndexes(metadata_);

        return result;
    }

    /**
     * Update vertex properties in batches.
     * Uses batch AQL UPDATE to modify multiple vertices in one query.
     */
    std::vector<double> updateVertexPropertyImpl(const std::vector<VertexUpdate>& updates, int batchSize) {
        return batchExecute(updates, batchSize, [this](const std::vector<VertexUpdate>& batch) {
            // Build array of update specifications
            json updateSpecs = json::array();
            for (const auto& update : batch) {
                json spec = {
                    {"_key", std::any_cast<std::string>(update.systemId)}
                };
                // Add properties to update
                for (const auto& [key, value] : update.properties) {
                    spec[key] = convertAnyToJson(value);
                }
                updateSpecs.push_back(spec);
            }

            // Single batch UPDATE query
            std::string query =
                "FOR spec IN @specs "
                "  UPDATE spec._key WITH spec IN " + std::string(VERTEX_COLLECTION);
            json bindVars = {{"specs", updateSpecs}};
            arangoUtils_->executeAQL(query, bindVars);
        });
    }

    /**
     * Update edge properties in batches.
     * Uses batch AQL to find edges and update their properties in one query.
     */
    std::vector<double> updateEdgePropertyImpl(const std::string& label,
                                               const std::vector<EdgeUpdate>& updates,
                                               int batchSize) {
        return batchExecute(updates, batchSize, [this, &label](const std::vector<EdgeUpdate>& batch) {
            // Build array of update specifications
            json updateSpecs = json::array();
            for (const auto& update : batch) {
                json spec = {
                    {"from", std::string(VERTEX_COLLECTION) + "/" + std::any_cast<std::string>(update.srcSystemId)},
                    {"to", std::string(VERTEX_COLLECTION) + "/" + std::any_cast<std::string>(update.dstSystemId)},
                    {"props", json::object()}
                };
                // Add properties to update
                for (const auto& [key, value] : update.properties) {
                    spec["props"][key] = convertAnyToJson(value);
                }
                updateSpecs.push_back(spec);
            }

            // Single batch UPDATE query
            std::string query =
                "FOR spec IN @specs "
                "  FOR e IN " + std::string(EDGE_COLLECTION) + " "
                "    FILTER e._from == spec.from AND e._to == spec.to AND e.label == @label "
                "    UPDATE e WITH spec.props IN " + std::string(EDGE_COLLECTION);
            json bindVars = {{"specs", updateSpecs}, {"label", label}};
            arangoUtils_->executeAQL(query, bindVars);
        });
    }

    /**
     * Get vertices by property in batches.
     * Uses batch AQL to query multiple property values in one query.
     */
    std::vector<double> getVertexByPropertyImpl(const std::vector<PropertyQuery>& queries, int batchSize) {
        return batchExecute(queries, batchSize, [this](const std::vector<PropertyQuery>& batch) {
            // Build array of query specifications
            json querySpecs = json::array();
            for (const auto& query : batch) {
                querySpecs.push_back({
                    {"key", query.key},
                    {"value", convertAnyToJson(query.value)}
                });
            }

            // Single batch query using FOR loop
            std::string aql =
                "FOR spec IN @specs "
                "  FOR v IN " + std::string(VERTEX_COLLECTION) + " "
                "    FILTER v[spec.key] == spec.value "
                "    RETURN v";
            json bindVars = {{"specs", querySpecs}};
            arangoUtils_->executeAQLWithResults(aql, bindVars);
        });
    }

    /**
     * Get edges by property in batches.
     * Uses batch AQL to query multiple property values in one query.
     */
    std::vector<double> getEdgeByPropertyImpl(const std::vector<PropertyQuery>& queries, int batchSize) {
        return batchExecute(queries, batchSize, [this](const std::vector<PropertyQuery>& batch) {
            // Build array of query specifications
            json querySpecs = json::array();
            for (const auto& query : batch) {
                querySpecs.push_back({
                    {"key", query.key},
                    {"value", convertAnyToJson(query.value)}
                });
            }

            // Single batch query using FOR loop
            std::string aql =
                "FOR spec IN @specs "
                "  FOR e IN " + std::string(EDGE_COLLECTION) + " "
                "    FILTER e[spec.key] == spec.value "
                "    RETURN e";
            json bindVars = {{"specs", querySpecs}};
            arangoUtils_->executeAQLWithResults(aql, bindVars);
        });
    }

private:
    CsvMetadata metadata_;

    /**
     * Convert std::any to JSON value.
     * Handles common types: string, int, double, bool.
     */
    json convertAnyToJson(const std::any& value) const {
        if (!value.has_value()) {
            return nullptr;
        }

        // Try common types
        try {
            if (value.type() == typeid(std::string)) {
                return std::any_cast<std::string>(value);
            } else if (value.type() == typeid(int)) {
                return std::any_cast<int>(value);
            } else if (value.type() == typeid(int64_t)) {
                return std::any_cast<int64_t>(value);
            } else if (value.type() == typeid(double)) {
                return std::any_cast<double>(value);
            } else if (value.type() == typeid(bool)) {
                return std::any_cast<bool>(value);
            } else if (value.type() == typeid(const char*)) {
                return std::string(std::any_cast<const char*>(value));
            }
        } catch (const std::bad_any_cast&) {
            // Fall through to default
        }

        // Default: return null for unknown types
        return nullptr;
    }
};

} // namespace graphbench
