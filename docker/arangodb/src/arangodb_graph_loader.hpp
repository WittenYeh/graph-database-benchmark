#pragma once

#include "arango_utils.hpp"
#include <graphbench/progress_callback.hpp>
#include <graphbench/benchmark_utils.hpp>
#include <csv.hpp>
#include <map>
#include <string>
#include <memory>
#include <chrono>
#include <iostream>

namespace graphbench {

/**
 * CSV metadata for tracking property types.
 * Stores information about property columns found in CSV files.
 */
struct CsvMetadata {
    std::map<std::string, std::string> vertexPropertyTypes;
    std::map<std::string, std::string> edgePropertyTypes;

    bool hasVertexProperties() const { return !vertexPropertyTypes.empty(); }
    bool hasEdgeProperties() const { return !edgePropertyTypes.empty(); }
};

/**
 * ArangoDB graph loader.
 * Handles loading graph data from CSV files and creating indexes for property queries.
 * Uses batch insertion for optimal performance.
 */
class ArangoDBGraphLoader {
public:
    /**
     * Constructor.
     * @param arangoUtils ArangoDB utility instance for executing queries
     * @param dbName Database name to use
     * @param progressCallback Progress callback for reporting load progress
     * @param loadProperties Whether to load property columns from CSV (true for property benchmarks)
     */
    ArangoDBGraphLoader(std::shared_ptr<ArangoUtils> arangoUtils,
                        const std::string& dbName,
                        std::shared_ptr<ProgressCallback> progressCallback,
                        bool loadProperties)
        : arangoUtils_(arangoUtils),
          dbName_(dbName),
          progressCallback_(progressCallback),
          loadProperties_(loadProperties) {}

    /**
     * Load graph from CSV files (nodes.csv and edges.csv).
     * Reads CSV files in batches and inserts into ArangoDB collections using batch AQL queries.
     * This is much faster than individual inserts.
     * @param datasetPath Path to dataset directory containing nodes.csv and edges.csv
     * @return Map containing load statistics (nodes count, edges count, duration in seconds)
     */
    std::map<std::string, std::any> load(const std::string& datasetPath) {
        auto startTime = std::chrono::high_resolution_clock::now();

        // Load nodes from nodes.csv
        std::string nodesFile = datasetPath + "/nodes.csv";
        int nodeCount = loadNodes(nodesFile);

        // Load edges from edges.csv
        std::string edgesFile = datasetPath + "/edges.csv";
        int edgeCount = loadEdges(edgesFile);

        auto endTime = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double>(endTime - startTime).count();

        std::map<std::string, std::any> result;
        result["nodes"] = nodeCount;
        result["edges"] = edgeCount;
        result["duration"] = duration;

        return result;
    }

    /**
     * Create property indexes for efficient property-based queries.
     * Should be called after load() for property benchmark executors.
     * Creates persistent indexes on all property columns.
     * @param metadata CSV metadata containing property column information
     */
    void createPropertyIndexes(const CsvMetadata& metadata) {
        // Create indexes for vertex properties
        for (const auto& [propName, propType] : metadata.vertexPropertyTypes) {
            arangoUtils_->createIndex(dbName_, VERTEX_COLLECTION, {propName});
            progressCallback_->sendLogMessage("Created vertex property index: " + propName, "INFO");
        }

        // Create indexes for edge properties
        for (const auto& [propName, propType] : metadata.edgePropertyTypes) {
            arangoUtils_->createIndex(dbName_, EDGE_COLLECTION, {propName});
            progressCallback_->sendLogMessage("Created edge property index: " + propName, "INFO");
        }
    }

    /**
     * Get the node IDs mapping (originId -> systemId).
     * Used for converting dataset IDs to ArangoDB document keys.
     */
    const std::map<int64_t, std::string>& getNodeIdsMap() const { return nodeIdsMap_; }

    /**
     * Get CSV metadata containing property type information.
     */
    const CsvMetadata& getMetadata() const { return metadata_; }

private:
    static constexpr const char* VERTEX_COLLECTION = "vertices";
    static constexpr const char* EDGE_COLLECTION = "edges";
    static constexpr int LOAD_BATCH_SIZE = 10000;

    std::shared_ptr<ArangoUtils> arangoUtils_;
    std::string dbName_;
    std::shared_ptr<ProgressCallback> progressCallback_;
    bool loadProperties_;
    std::map<int64_t, std::string> nodeIdsMap_;
    CsvMetadata metadata_;

    /**
     * Load nodes from CSV file in batches.
     * Parses nodes.csv and inserts documents into vertex collection.
     * First column is always the node ID (originId).
     * Additional columns are treated as properties if loadProperties_ is true.
     * @param nodesFile Path to nodes.csv
     * @return Number of nodes loaded
     */
    int loadNodes(const std::string& nodesFile) {
        // Create CSV reader
        csv::CSVReader reader(nodesFile);

        // Get column names from header
        std::vector<std::string> headers = reader.get_col_names();
        std::vector<std::string> propertyColumns;

        // First column is always node ID, rest are properties
        if (loadProperties_ && headers.size() > 1) {
            for (size_t i = 1; i < headers.size(); i++) {
                propertyColumns.push_back(headers[i]);
                // Infer type as string by default (can be enhanced with type detection)
                metadata_.vertexPropertyTypes[headers[i]] = "string";
            }
        }

        int nodeCount = 0;
        json nodeBatch = json::array();

        // Read CSV rows
        for (csv::CSVRow& row : reader) {
            // Parse node ID from first column
            int64_t originId = row[0].get<int64_t>();
            std::string vertexKey = "v" + std::to_string(originId);

            // Build node document
            json nodeDoc = {
                {"_key", vertexKey},
                {"originId", originId}
            };

            // Add properties if enabled
            if (loadProperties_) {
                for (size_t i = 0; i < propertyColumns.size() && i + 1 < row.size(); i++) {
                    nodeDoc[propertyColumns[i]] = row[i + 1].get<>();
                }
            }

            nodeBatch.push_back(nodeDoc);
            nodeIdsMap_[originId] = vertexKey;
            nodeCount++;

            // Batch insert when batch size reached
            if (nodeBatch.size() >= LOAD_BATCH_SIZE) {
                insertBatch(VERTEX_COLLECTION, nodeBatch);
                nodeBatch.clear();
            }
        }

        // Insert remaining nodes
        if (!nodeBatch.empty()) {
            insertBatch(VERTEX_COLLECTION, nodeBatch);
        }

        progressCallback_->sendLogMessage("Loaded " + std::to_string(nodeCount) + " nodes", "INFO");
        return nodeCount;
    }

    /**
     * Load edges from CSV file in batches.
     * Parses edges.csv and inserts documents into edge collection.
     * First two columns are source and destination node IDs.
     * Additional columns are treated as properties if loadProperties_ is true.
     * @param edgesFile Path to edges.csv
     * @return Number of edges loaded
     */
    int loadEdges(const std::string& edgesFile) {
        // Create CSV reader
        csv::CSVReader reader(edgesFile);

        // Get column names from header
        std::vector<std::string> headers = reader.get_col_names();
        std::vector<std::string> propertyColumns;

        // First two columns are src/dst, rest are properties
        if (loadProperties_ && headers.size() > 2) {
            for (size_t i = 2; i < headers.size(); i++) {
                propertyColumns.push_back(headers[i]);
                metadata_.edgePropertyTypes[headers[i]] = "string";
            }
        }

        int edgeCount = 0;
        json edgeBatch = json::array();

        // Read CSV rows
        for (csv::CSVRow& row : reader) {
            if (row.size() < 2) continue;

            // Parse source and destination IDs
            int64_t srcId = row[0].get<int64_t>();
            int64_t dstId = row[1].get<int64_t>();

            // Skip if nodes don't exist
            if (nodeIdsMap_.find(srcId) == nodeIdsMap_.end() ||
                nodeIdsMap_.find(dstId) == nodeIdsMap_.end()) {
                continue;
            }

            // Build edge document with _from and _to
            json edgeDoc = {
                {"_from", std::string(VERTEX_COLLECTION) + "/" + nodeIdsMap_[srcId]},
                {"_to", std::string(VERTEX_COLLECTION) + "/" + nodeIdsMap_[dstId]}
            };

            // Add properties if enabled
            if (loadProperties_) {
                for (size_t i = 0; i < propertyColumns.size() && i + 2 < row.size(); i++) {
                    edgeDoc[propertyColumns[i]] = row[i + 2].get<>();
                }
            }

            edgeBatch.push_back(edgeDoc);
            edgeCount++;

            // Batch insert when batch size reached
            if (edgeBatch.size() >= LOAD_BATCH_SIZE) {
                insertBatch(EDGE_COLLECTION, edgeBatch);
                edgeBatch.clear();
            }
        }

        // Insert remaining edges
        if (!edgeBatch.empty()) {
            insertBatch(EDGE_COLLECTION, edgeBatch);
        }

        progressCallback_->sendLogMessage("Loaded " + std::to_string(edgeCount) + " edges", "INFO");
        return edgeCount;
    }

    /**
     * Insert a batch of documents into a collection using a single AQL query.
     * This is the key optimization: instead of N individual inserts, we do 1 batch insert.
     * Uses AQL FOR loop to iterate over documents array and insert each one.
     * @param collection Collection name
     * @param documents JSON array of documents to insert
     */
    void insertBatch(const std::string& collection, const json& documents) {
        // Use AQL FOR loop to insert all documents in one query
        // This is much faster than individual INSERT statements
        std::string query = "FOR doc IN @docs INSERT doc INTO " + collection;
        json bindVars = {{"docs", documents}};
        arangoUtils_->executeAQL(query, bindVars);
    }
};

} // namespace graphbench
