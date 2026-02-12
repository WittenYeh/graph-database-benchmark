package com.graphbench.dataset;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parses Matrix Market (.mtx) files into node and edge lists.
 *
 * Expected format:
 *   - Lines starting with '%' are comments.
 *   - First non-comment line: "rows cols nnz" (header).
 *   - Subsequent lines: "src dst" edge pairs (1-indexed).
 */
public class MatrixMarketReader {

    private int numNodes;
    private int numEdges;
    private final List<long[]> edges = new ArrayList<>();
    private final Set<Long> nodeIds = new HashSet<>();

    /**
     * Reads and parses the given .mtx file.
     */
    public void read(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean headerRead = false;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("%")) {
                    continue;
                }

                String[] parts = line.split("\\s+");

                if (!headerRead) {
                    // Header line: rows cols nnz
                    numNodes = Integer.parseInt(parts[0]);
                    numEdges = Integer.parseInt(parts[2]);
                    headerRead = true;
                    continue;
                }

                // Edge line: src dst (1-indexed in .mtx, we keep as-is)
                long src = Long.parseLong(parts[0]);
                long dst = Long.parseLong(parts[1]);
                edges.add(new long[]{src, dst});
                nodeIds.add(src);
                nodeIds.add(dst);
            }
        }
    }

    public int getNumNodes() {
        return numNodes;
    }

    public int getNumEdges() {
        return numEdges;
    }

    public List<long[]> getEdges() {
        return edges;
    }

    /**
     * Returns all unique node IDs found in the edge list.
     */
    public Set<Long> getNodeIds() {
        return nodeIds;
    }

    /**
     * Returns the node IDs as a list for random access.
     */
    public List<Long> getNodeIdList() {
        return new ArrayList<>(nodeIds);
    }
}
