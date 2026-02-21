#pragma once

#include <string>
#include <vector>
#include <map>
#include <any>
#include <cstdint>

namespace graphbench {

/**
 * Base class for workload parameters.
 */
struct WorkloadParameters {
    virtual ~WorkloadParameters() = default;
};

/**
 * Parameters for ADD_VERTEX task.
 */
struct AddVertexParameters : public WorkloadParameters {
    int count;
};

/**
 * Parameters for ADD_EDGE task.
 */
struct AddEdgeParameters : public WorkloadParameters {
    std::string label;
    std::vector<std::pair<std::any, std::any>> pairs;  // Pre-converted system IDs
    int originalCount;  // Original number of pairs before conversion
};

/**
 * Parameters for REMOVE_VERTEX task.
 */
struct RemoveVertexParameters : public WorkloadParameters {
    std::vector<std::any> systemIds;  // Pre-converted system IDs
    int originalCount;
};

/**
 * Parameters for REMOVE_EDGE task.
 */
struct RemoveEdgeParameters : public WorkloadParameters {
    std::string label;
    std::vector<std::pair<std::any, std::any>> pairs;  // Pre-converted system IDs
    int originalCount;
};

/**
 * Parameters for GET_NBRS task.
 */
struct GetNbrsParameters : public WorkloadParameters {
    std::string direction;
    std::vector<std::any> systemIds;  // Pre-converted system IDs
    int originalCount;
};

/**
 * Parameters for UPDATE_VERTEX_PROPERTY task.
 */
struct UpdateVertexPropertyParameters : public WorkloadParameters {
    struct Update {
        std::any vertexId;  // System ID
        std::map<std::string, std::any> properties;
    };
    std::vector<Update> updates;
};

/**
 * Parameters for UPDATE_EDGE_PROPERTY task.
 */
struct UpdateEdgePropertyParameters : public WorkloadParameters {
    struct Update {
        std::any srcId;  // System ID
        std::any dstId;  // System ID
        std::string label;
        std::map<std::string, std::any> properties;
    };
    std::vector<Update> updates;
};

/**
 * Parameters for GET_VERTEX_BY_PROPERTY task.
 */
struct GetVertexByPropertyParameters : public WorkloadParameters {
    struct Query {
        std::string propertyName;
        std::any propertyValue;
    };
    std::vector<Query> queries;
};

/**
 * Parameters for GET_EDGE_BY_PROPERTY task.
 */
struct GetEdgeByPropertyParameters : public WorkloadParameters {
    struct Query {
        std::string propertyName;
        std::any propertyValue;
    };
    std::vector<Query> queries;
};

} // namespace graphbench
