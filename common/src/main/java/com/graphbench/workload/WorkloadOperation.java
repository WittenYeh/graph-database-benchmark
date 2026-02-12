package com.graphbench.workload;

/**
 * A single abstract workload operation with its arguments.
 */
public class WorkloadOperation {

    private Operation type;
    private long nodeId;   // Used by ADD_NODE, DELETE_NODE, READ_NBRS
    private long srcId;    // Used by ADD_EDGE, DELETE_EDGE
    private long dstId;    // Used by ADD_EDGE, DELETE_EDGE

    public WorkloadOperation() {
    }

    /**
     * Creates a node-level operation (ADD_NODE, DELETE_NODE, READ_NBRS).
     */
    public static WorkloadOperation nodeOp(Operation type, long nodeId) {
        WorkloadOperation op = new WorkloadOperation();
        op.type = type;
        op.nodeId = nodeId;
        return op;
    }

    /**
     * Creates an edge-level operation (ADD_EDGE, DELETE_EDGE).
     */
    public static WorkloadOperation edgeOp(Operation type, long srcId, long dstId) {
        WorkloadOperation op = new WorkloadOperation();
        op.type = type;
        op.srcId = srcId;
        op.dstId = dstId;
        return op;
    }

    /**
     * Creates a LOAD_GRAPH operation (no arguments needed).
     */
    public static WorkloadOperation loadGraphOp() {
        WorkloadOperation op = new WorkloadOperation();
        op.type = Operation.LOAD_GRAPH;
        return op;
    }

    public Operation getType() {
        return type;
    }

    public long getNodeId() {
        return nodeId;
    }

    public long getSrcId() {
        return srcId;
    }

    public long getDstId() {
        return dstId;
    }
}
