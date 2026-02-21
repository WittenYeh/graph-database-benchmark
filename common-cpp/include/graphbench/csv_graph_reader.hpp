#pragma once

#include <csv.hpp>
#include <nlohmann/json.hpp>
#include <string>
#include <map>
#include <vector>
#include <functional>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace graphbench {

using json = nlohmann::json;
namespace fs = std::filesystem;

/**
 * Type enumeration for property types.
 */
enum class PropertyType {
    STRING,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN
};

/**
 * Metadata about CSV columns discovered from headers.
 */
class CsvMetadata {
public:
    CsvMetadata(const std::vector<std::string>& nodeHeaders,
                const std::vector<std::string>& edgeHeaders,
                const std::map<std::string, PropertyType>& nodePropertyTypes,
                const std::map<std::string, PropertyType>& edgePropertyTypes)
        : nodeHeaders_(nodeHeaders),
          edgeHeaders_(edgeHeaders),
          nodePropertyTypes_(nodePropertyTypes),
          edgePropertyTypes_(edgePropertyTypes) {}

    /** All column names from nodes.csv */
    const std::vector<std::string>& getNodeHeaders() const { return nodeHeaders_; }

    /** All column names from edges.csv */
    const std::vector<std::string>& getEdgeHeaders() const { return edgeHeaders_; }

    /** Property column names from nodes.csv (everything after "node_id") */
    std::vector<std::string> getNodePropertyHeaders() const {
        if (nodeHeaders_.size() <= 1) return {};
        return std::vector<std::string>(nodeHeaders_.begin() + 1, nodeHeaders_.end());
    }

    /** Property column names from edges.csv (everything after "src","dst") */
    std::vector<std::string> getEdgePropertyHeaders() const {
        if (edgeHeaders_.size() <= 2) return {};
        return std::vector<std::string>(edgeHeaders_.begin() + 2, edgeHeaders_.end());
    }

    /** Inferred type for a node property column */
    PropertyType getNodePropertyType(const std::string& column) const {
        auto it = nodePropertyTypes_.find(column);
        return it != nodePropertyTypes_.end() ? it->second : PropertyType::STRING;
    }

    /** Inferred type for an edge property column */
    PropertyType getEdgePropertyType(const std::string& column) const {
        auto it = edgePropertyTypes_.find(column);
        return it != edgePropertyTypes_.end() ? it->second : PropertyType::STRING;
    }

private:
    std::vector<std::string> nodeHeaders_;
    std::vector<std::string> edgeHeaders_;
    std::map<std::string, PropertyType> nodePropertyTypes_;
    std::map<std::string, PropertyType> edgePropertyTypes_;
};

/**
 * Utility for reading graph CSV files (nodes.csv, edges.csv).
 * Provides streaming callbacks with property maps built from CSV headers.
 */
class CsvGraphReader {
public:
    using NodeCallback = std::function<void(int64_t nodeId, const std::map<std::string, std::string>& properties)>;
    using EdgeCallback = std::function<void(int64_t srcId, int64_t dstId, const std::map<std::string, std::string>& properties)>;

    /**
     * Read only the CSV headers from nodes.csv and edges.csv without loading any data rows.
     * Much faster than read() when only header information is needed.
     */
    static CsvMetadata readHeaders(const std::string& datasetDir) {
        fs::path nodesPath = fs::path(datasetDir) / "nodes.csv";
        fs::path edgesPath = fs::path(datasetDir) / "edges.csv";

        // Read node headers
        csv::CSVReader nodeReader(nodesPath.string());
        std::vector<std::string> nodeHeaders = nodeReader.get_col_names();

        // Read edge headers
        csv::CSVReader edgeReader(edgesPath.string());
        std::vector<std::string> edgeHeaders = edgeReader.get_col_names();

        // Load type metadata
        auto typeMaps = readPropertyTypes(datasetDir);

        return CsvMetadata(nodeHeaders, edgeHeaders, typeMaps.first, typeMaps.second);
    }

    /**
     * Read CSV files and invoke callbacks for each node and edge.
     * Callbacks receive property maps built from CSV headers automatically.
     */
    static CsvMetadata read(const std::string& datasetDir,
                           NodeCallback nodeCallback,
                           EdgeCallback edgeCallback) {
        fs::path nodesPath = fs::path(datasetDir) / "nodes.csv";
        fs::path edgesPath = fs::path(datasetDir) / "edges.csv";

        // Read nodes.csv
        csv::CSVReader nodeReader(nodesPath.string());
        std::vector<std::string> nodeHeaders = nodeReader.get_col_names();

        for (csv::CSVRow& row : nodeReader) {
            int64_t nodeId = row[0].get<int64_t>();
            auto props = buildPropertyMap(nodeHeaders, row, 1);
            nodeCallback(nodeId, props);
        }

        // Read edges.csv
        csv::CSVReader edgeReader(edgesPath.string());
        std::vector<std::string> edgeHeaders = edgeReader.get_col_names();

        for (csv::CSVRow& row : edgeReader) {
            int64_t srcId = row[0].get<int64_t>();
            int64_t dstId = row[1].get<int64_t>();
            auto props = buildPropertyMap(edgeHeaders, row, 2);
            edgeCallback(srcId, dstId, props);
        }

        // Load type metadata
        auto typeMaps = readPropertyTypes(datasetDir);

        return CsvMetadata(nodeHeaders, edgeHeaders, typeMaps.first, typeMaps.second);
    }

private:
    /**
     * Read property type metadata from type_meta.json.
     * Returns pair of [nodePropertyTypes, edgePropertyTypes]
     */
    static std::pair<std::map<std::string, PropertyType>, std::map<std::string, PropertyType>>
    readPropertyTypes(const std::string& datasetDir) {
        std::map<std::string, PropertyType> nodePropertyTypes;
        std::map<std::string, PropertyType> edgePropertyTypes;

        fs::path typeMetaPath = fs::path(datasetDir) / "type_meta.json";
        if (!fs::exists(typeMetaPath)) {
            return {nodePropertyTypes, edgePropertyTypes};
        }

        try {
            std::ifstream file(typeMetaPath);
            json meta;
            file >> meta;

            if (meta.contains("node_properties")) {
                for (auto& [key, value] : meta["node_properties"].items()) {
                    nodePropertyTypes[key] = typeStringToEnum(value.get<std::string>());
                }
            }

            if (meta.contains("edge_properties")) {
                for (auto& [key, value] : meta["edge_properties"].items()) {
                    edgePropertyTypes[key] = typeStringToEnum(value.get<std::string>());
                }
            }
        } catch (const std::exception& e) {
            // Silently fall back to String for all properties if type_meta.json is invalid
            std::cerr << "Warning: Failed to read type_meta.json: " << e.what() << std::endl;
        }

        return {nodePropertyTypes, edgePropertyTypes};
    }

    /**
     * Convert type string from type_meta.json to PropertyType enum.
     */
    static PropertyType typeStringToEnum(const std::string& typeStr) {
        std::string lower = typeStr;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

        if (lower == "integer") return PropertyType::INTEGER;
        if (lower == "long") return PropertyType::LONG;
        if (lower == "float") return PropertyType::FLOAT;
        if (lower == "double") return PropertyType::DOUBLE;
        if (lower == "boolean") return PropertyType::BOOLEAN;
        return PropertyType::STRING;
    }

    /**
     * Build a property map from CSV headers and record values, starting at the given offset.
     */
    static std::map<std::string, std::string> buildPropertyMap(
            const std::vector<std::string>& headers,
            const csv::CSVRow& row,
            size_t offset) {
        std::map<std::string, std::string> props;

        for (size_t i = offset; i < headers.size() && i < row.size(); i++) {
            std::string value = row[i].get<>();
            if (!value.empty()) {
                props[headers[i]] = value;
            }
        }

        return props;
    }
};

} // namespace graphbench
