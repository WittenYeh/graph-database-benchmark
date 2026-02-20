package com.graphbench.api;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility for reading graph CSV files (nodes.csv, edges.csv) using univocity-parsers.
 * Provides streaming callbacks with property maps built from CSV headers.
 */
public class CsvGraphReader {

    /**
     * Metadata about the CSV columns discovered from headers.
     */
    public static class CsvMetadata {
        private final String[] nodeHeaders;
        private final String[] edgeHeaders;

        public CsvMetadata(String[] nodeHeaders, String[] edgeHeaders) {
            this.nodeHeaders = nodeHeaders;
            this.edgeHeaders = edgeHeaders;
        }

        /** All column names from nodes.csv */
        public String[] getNodeHeaders() { return nodeHeaders; }

        /** All column names from edges.csv */
        public String[] getEdgeHeaders() { return edgeHeaders; }

        /** Property column names from nodes.csv (everything after "node_id") */
        public String[] getNodePropertyHeaders() {
            if (nodeHeaders.length <= 1) return new String[0];
            String[] props = new String[nodeHeaders.length - 1];
            System.arraycopy(nodeHeaders, 1, props, 0, props.length);
            return props;
        }

        /** Property column names from edges.csv (everything after "src","dst") */
        public String[] getEdgePropertyHeaders() {
            if (edgeHeaders.length <= 2) return new String[0];
            String[] props = new String[edgeHeaders.length - 2];
            System.arraycopy(edgeHeaders, 2, props, 0, props.length);
            return props;
        }
    }

    /**
     * Read a graph dataset directory containing nodes.csv and edges.csv.
     * Callbacks receive property maps built from CSV headers automatically.
     *
     * @param datasetDir   Path to the dataset directory
     * @param nodeCallback Called for each node: (nodeId, properties map)
     * @param edgeCallback Called for each edge: (srcId, dstId, properties map)
     * @return CsvMetadata with header information
     */
    public static CsvMetadata read(
            String datasetDir,
            NodeCallback nodeCallback,
            EdgeCallback edgeCallback
    ) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setLineSeparatorDetectionEnabled(true);

        // 1. Read nodes.csv
        File nodesFile = new File(datasetDir, "nodes.csv");
        CsvParser nodeParser = new CsvParser(settings);
        nodeParser.beginParsing(nodesFile);
        String[] nodeHeaders = nodeParser.getRecordMetadata().headers();

        String[] record;
        while ((record = nodeParser.parseNext()) != null) {
            long nodeId = Long.parseLong(record[0]);
            Map<String, String> props = buildPropertyMap(nodeHeaders, record, 1);
            nodeCallback.accept(nodeId, props);
        }
        nodeParser.stopParsing();

        // 2. Read edges.csv
        File edgesFile = new File(datasetDir, "edges.csv");
        CsvParser edgeParser = new CsvParser(settings);
        edgeParser.beginParsing(edgesFile);
        String[] edgeHeaders = edgeParser.getRecordMetadata().headers();

        while ((record = edgeParser.parseNext()) != null) {
            long srcId = Long.parseLong(record[0]);
            long dstId = Long.parseLong(record[1]);
            Map<String, String> props = buildPropertyMap(edgeHeaders, record, 2);
            edgeCallback.accept(srcId, dstId, props);
        }
        edgeParser.stopParsing();

        return new CsvMetadata(nodeHeaders, edgeHeaders);
    }

    /**
     * Build a property map from CSV headers and record values, starting at the given offset.
     */
    private static Map<String, String> buildPropertyMap(String[] headers, String[] record, int offset) {
        if (headers.length <= offset) return Collections.emptyMap();
        Map<String, String> props = new LinkedHashMap<>();
        for (int i = offset; i < headers.length && i < record.length; i++) {
            if (record[i] != null && !record[i].isEmpty()) {
                props.put(headers[i], record[i]);
            }
        }
        return props;
    }

    @FunctionalInterface
    public interface NodeCallback {
        /** @param nodeId the node ID; @param properties map of property name to value (may be empty) */
        void accept(long nodeId, Map<String, String> properties);
    }

    @FunctionalInterface
    public interface EdgeCallback {
        /** @param srcId source node ID; @param dstId dest node ID; @param properties map (may be empty) */
        void accept(long srcId, long dstId, Map<String, String> properties);
    }
}
