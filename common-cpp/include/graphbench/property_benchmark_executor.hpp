#pragma once

#include <graphbench/benchmark_executor.hpp>
#include <map>

namespace graphbench {

/**
 * Vertex update structure for property operations.
 */
struct VertexUpdate {
    std::any systemId;
    std::map<std::string, std::any> properties;
};

/**
 * Edge update structure for property operations.
 */
struct EdgeUpdate {
    std::any srcSystemId;
    std::any dstSystemId;
    std::map<std::string, std::any> properties;
};

/**
 * Property query structure.
 */
struct PropertyQuery {
    std::string key;
    std::any value;
};

/**
 * CRTP base class for property benchmark executors.
 * Extends BenchmarkExecutor with property-related operations.
 * Uses static polymorphism for zero-overhead abstraction.
 */
template<typename Derived>
class PropertyBenchmarkExecutor : public BenchmarkExecutor<Derived> {
public:
    std::vector<double> updateVertexProperty(const std::vector<VertexUpdate>& updates,
                                             int batchSize) {
        return static_cast<Derived*>(this)->updateVertexPropertyImpl(updates, batchSize);
    }

    std::vector<double> updateEdgeProperty(const std::string& label,
                                           const std::vector<EdgeUpdate>& updates,
                                           int batchSize) {
        return static_cast<Derived*>(this)->updateEdgePropertyImpl(label, updates, batchSize);
    }

    std::vector<double> getVertexByProperty(const std::vector<PropertyQuery>& queries,
                                            int batchSize) {
        return static_cast<Derived*>(this)->getVertexByPropertyImpl(queries, batchSize);
    }

    std::vector<double> getEdgeByProperty(const std::vector<PropertyQuery>& queries,
                                          int batchSize) {
        return static_cast<Derived*>(this)->getEdgeByPropertyImpl(queries, batchSize);
    }

    // Default methods with batch size = 1
    std::vector<double> updateVertexProperty(const std::vector<VertexUpdate>& updates) {
        return updateVertexProperty(updates, 1);
    }

    std::vector<double> updateEdgeProperty(const std::string& label,
                                           const std::vector<EdgeUpdate>& updates) {
        return updateEdgeProperty(label, updates, 1);
    }

    std::vector<double> getVertexByProperty(const std::vector<PropertyQuery>& queries) {
        return getVertexByProperty(queries, 1);
    }

    std::vector<double> getEdgeByProperty(const std::vector<PropertyQuery>& queries) {
        return getEdgeByProperty(queries, 1);
    }

protected:
    ~PropertyBenchmarkExecutor() = default;
};

} // namespace graphbench
