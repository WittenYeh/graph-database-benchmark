package com.graphbench.db;

import java.util.List;

/**
 * Abstraction for an embedded graph database.
 * Implementations must be thread-safe for throughput benchmarks.
 */
public interface DatabaseAdapter extends AutoCloseable {

    /**
     * Initializes the embedded database at the given storage path.
     */
    void open(String storagePath, com.graphbench.config.DatabaseInstanceConfig config);

    /**
     * Shuts down the database cleanly.
     */
    void close();

    /**
     * Returns the database name (e.g., "neo4j", "janusgraph").
     */
    String getName();

    /**
     * Adds a node with the given ID.
     */
    void addNode(long nodeId);

    /**
     * Adds an edge from srcId to dstId.
     */
    void addEdge(long srcId, long dstId);

    /**
     * Deletes the node with the given ID and all its edges.
     */
    void deleteNode(long nodeId);

    /**
     * Deletes the edge between srcId and dstId.
     */
    void deleteEdge(long srcId, long dstId);

    /**
     * Returns the neighbor node IDs of the given node.
     */
    List<Long> readNeighbors(long nodeId);

    /**
     * Bulk-loads a graph from the given edge list.
     *
     * @param edges    list of [src, dst] pairs
     * @param numNodes total number of nodes declared in the dataset
     */
    void loadGraph(List<long[]> edges, int numNodes);

    /**
     * Executes a pre-compiled query string in the database's native language.
     */
    void executeQuery(String query);
}
