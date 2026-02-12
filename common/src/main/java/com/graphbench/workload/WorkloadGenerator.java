package com.graphbench.workload;

import com.graphbench.config.TaskConfig;
import com.graphbench.dataset.MatrixMarketReader;

import java.util.*;

/**
 * Generates concrete workload operations from a task configuration and dataset.
 *
 * For read/delete operations, node IDs and edges are sampled from the loaded dataset.
 * For add operations, new IDs are generated beyond the existing range.
 */
public class WorkloadGenerator {

    private final MatrixMarketReader dataset;
    private final Random random;
    private final List<Long> nodeIdList;
    private final List<long[]> edgeList;
    private long nextNodeId;

    // Track deleted nodes and edges to avoid deleting them again
    private final Set<Long> deletedNodes;
    private final Set<String> deletedEdges; // Store as "srcId-dstId" strings

    public WorkloadGenerator(MatrixMarketReader dataset, long seed) {
        this.dataset = dataset;
        this.random = new Random(seed);
        this.nodeIdList = dataset.getNodeIdList();
        this.edgeList = dataset.getEdges();
        // New node IDs start beyond the declared node count
        this.nextNodeId = dataset.getNumNodes() + 1;
        this.deletedNodes = new HashSet<>();
        this.deletedEdges = new HashSet<>();
    }

    /**
     * Generates a Workload for the given task configuration.
     */
    public Workload generate(TaskConfig task, String datasetName, String datasetPath) {
        Workload workload = new Workload();
        workload.setTaskName(task.getName());
        workload.setDataset(datasetName);
        workload.setDatasetPath(datasetPath);
        workload.setTotalOps(task.getOps());
        workload.setClientThreads(task.isThroughput() ? task.getClientThreads() : 1);
        workload.setCopyMode(task.isCopyMode());

        List<WorkloadOperation> ops;
        String name = task.getName();

        if (name.equals("load_graph")) {
            ops = new ArrayList<>();
            ops.add(WorkloadOperation.loadGraphOp());
        } else if (name.startsWith("mixed_workload")) {
            ops = generateMixedOps(task.getOps(), task.getRatios());
        } else {
            Operation opType = resolveOperationType(name);
            ops = generateUniformOps(opType, task.getOps());
        }

        workload.setOperations(ops);
        return workload;
    }

    /**
     * Resolves the Operation enum from a task name like "add_nodes_latency".
     */
    private Operation resolveOperationType(String taskName) {
        // Strip the _latency or _throughput suffix
        String base = taskName.replaceAll("_(latency|throughput)$", "");
        switch (base) {
            case "add_nodes":    return Operation.ADD_NODE;
            case "add_edges":    return Operation.ADD_EDGE;
            case "delete_nodes": return Operation.DELETE_NODE;
            case "delete_edges": return Operation.DELETE_EDGE;
            case "read_nbrs":    return Operation.READ_NBRS;
            default:
                throw new IllegalArgumentException("Unknown task type: " + taskName);
        }
    }

    /**
     * Generates a list of uniform operations of the given type.
     */
    private List<WorkloadOperation> generateUniformOps(Operation type, int count) {
        List<WorkloadOperation> ops = new ArrayList<>(count);

        // For READ_NBRS, ensure we use unique nodes (no duplicates)
        if (type == Operation.READ_NBRS) {
            // Create a shuffled copy of node IDs to ensure uniqueness
            List<Long> shuffledNodes = new ArrayList<>(nodeIdList);
            // Shuffle using the seeded random to ensure reproducibility across databases
            java.util.Collections.shuffle(shuffledNodes, random);

            // Take up to 'count' unique nodes
            int nodesToRead = Math.min(count, shuffledNodes.size());
            for (int i = 0; i < nodesToRead; i++) {
                ops.add(WorkloadOperation.nodeOp(type, shuffledNodes.get(i)));
            }

            // If count > available nodes, log a warning but continue
            if (count > shuffledNodes.size()) {
                System.err.println("Warning: Requested " + count + " read operations but only "
                    + shuffledNodes.size() + " unique nodes available. Using all available nodes.");
            }
        } else {
            // For other operations, generate as before (may have duplicates)
            for (int i = 0; i < count; i++) {
                ops.add(createOperation(type));
            }
        }

        return ops;
    }

    /**
     * Generates a mixed list of operations according to the given ratio distribution.
     * Ratios must sum to 1.0 (with small tolerance).
     */
    private List<WorkloadOperation> generateMixedOps(int count, Map<String, Double> ratios) {
        if (ratios == null || ratios.isEmpty()) {
            throw new IllegalArgumentException("Mixed workload requires non-empty ratios");
        }

        double sum = ratios.values().stream().mapToDouble(Double::doubleValue).sum();
        if (Math.abs(sum - 1.0) > 0.01) {
            throw new IllegalArgumentException("Ratios must sum to 1.0, got: " + sum);
        }

        // Build cumulative distribution
        TreeMap<Double, Operation> cdf = new TreeMap<>();
        double cumulative = 0.0;
        for (Map.Entry<String, Double> entry : ratios.entrySet()) {
            cumulative += entry.getValue();
            cdf.put(cumulative, mapRatioKeyToOperation(entry.getKey()));
        }

        List<WorkloadOperation> ops = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            double roll = random.nextDouble();
            Map.Entry<Double, Operation> selected = cdf.ceilingEntry(roll);
            if (selected == null) {
                selected = cdf.lastEntry();
            }
            ops.add(createOperation(selected.getValue()));
        }
        return ops;
    }

    /**
     * Maps a ratio key (e.g., "add_node") to an Operation enum value.
     */
    private Operation mapRatioKeyToOperation(String key) {
        switch (key) {
            case "add_node":    return Operation.ADD_NODE;
            case "add_edge":    return Operation.ADD_EDGE;
            case "delete_node": return Operation.DELETE_NODE;
            case "delete_edge": return Operation.DELETE_EDGE;
            case "read_nbrs":   return Operation.READ_NBRS;
            default:
                throw new IllegalArgumentException("Unknown ratio key: " + key);
        }
    }

    /**
     * Creates a single WorkloadOperation with appropriate random arguments.
     */
    private WorkloadOperation createOperation(Operation type) {
        switch (type) {
            case ADD_NODE:
                return WorkloadOperation.nodeOp(Operation.ADD_NODE, nextNodeId++);
            case ADD_EDGE: {
                long src = randomExistingNode();
                long dst = randomExistingNode();
                return WorkloadOperation.edgeOp(Operation.ADD_EDGE, src, dst);
            }
            case DELETE_NODE: {
                // Find a node that hasn't been deleted yet
                long nodeId = randomNonDeletedNode();
                deletedNodes.add(nodeId);
                return WorkloadOperation.nodeOp(Operation.DELETE_NODE, nodeId);
            }
            case DELETE_EDGE: {
                // Find an edge that hasn't been deleted yet
                long[] edge = randomNonDeletedEdge();
                String edgeKey = edge[0] + "-" + edge[1];
                deletedEdges.add(edgeKey);
                // Also add reverse direction for undirected graphs
                deletedEdges.add(edge[1] + "-" + edge[0]);
                return WorkloadOperation.edgeOp(Operation.DELETE_EDGE, edge[0], edge[1]);
            }
            case READ_NBRS:
                return WorkloadOperation.nodeOp(Operation.READ_NBRS, randomExistingNode());
            default:
                throw new IllegalArgumentException("Cannot create operation for type: " + type);
        }
    }

    private long randomExistingNode() {
        return nodeIdList.get(random.nextInt(nodeIdList.size()));
    }

    /**
     * Returns a random node that hasn't been deleted yet.
     * If all nodes have been deleted, returns a random existing node anyway.
     */
    private long randomNonDeletedNode() {
        // Try up to 100 times to find a non-deleted node
        for (int attempt = 0; attempt < 100; attempt++) {
            long nodeId = randomExistingNode();
            if (!deletedNodes.contains(nodeId)) {
                return nodeId;
            }
        }
        // If we can't find a non-deleted node after 100 attempts,
        // just return a random node (this shouldn't happen in practice)
        return randomExistingNode();
    }

    private long[] randomExistingEdge() {
        return edgeList.get(random.nextInt(edgeList.size()));
    }

    /**
     * Returns a random edge that hasn't been deleted yet.
     * If all edges have been deleted, returns a random existing edge anyway.
     */
    private long[] randomNonDeletedEdge() {
        // Try up to 100 times to find a non-deleted edge
        for (int attempt = 0; attempt < 100; attempt++) {
            long[] edge = randomExistingEdge();
            String edgeKey = edge[0] + "-" + edge[1];
            String reverseKey = edge[1] + "-" + edge[0];
            if (!deletedEdges.contains(edgeKey) && !deletedEdges.contains(reverseKey)) {
                return edge;
            }
        }
        // If we can't find a non-deleted edge after 100 attempts,
        // just return a random edge (this shouldn't happen in practice)
        return randomExistingEdge();
    }
}
