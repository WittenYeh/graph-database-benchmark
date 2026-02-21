package com.graphbench.api;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.HashMap;

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
        private final Map<String, Class<?>> nodePropertyTypes;
        private final Map<String, Class<?>> edgePropertyTypes;

        public CsvMetadata(String[] nodeHeaders, String[] edgeHeaders,
                           Map<String, Class<?>> nodePropertyTypes,
                           Map<String, Class<?>> edgePropertyTypes) {
            this.nodeHeaders = nodeHeaders;
            this.edgeHeaders = edgeHeaders;
            this.nodePropertyTypes = nodePropertyTypes != null ? nodePropertyTypes : new HashMap<>();
            this.edgePropertyTypes = edgePropertyTypes != null ? edgePropertyTypes : new HashMap<>();
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

        /** Inferred Java type for a node property column. Defaults to String.class if unknown. */
        public Class<?> getNodePropertyType(String column) {
            return nodePropertyTypes.getOrDefault(column, String.class);
        }

        /** Inferred Java type for an edge property column. Defaults to String.class if unknown. */
        public Class<?> getEdgePropertyType(String column) {
            return edgePropertyTypes.getOrDefault(column, String.class);
        }
    }

    /**
     * Read only the CSV headers from nodes.csv and edges.csv without loading any data rows.
     * Much faster than read() when only header information is needed.
     *
     * @param datasetDir Path to the dataset directory
     * @return CsvMetadata with header information and type metadata
     */
    public static CsvMetadata readHeaders(String datasetDir) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(true);
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setNumberOfRowsToSkip(0);

        File nodesFile = new File(datasetDir, "nodes.csv");
        CsvParser nodeParser = new CsvParser(settings);
        nodeParser.beginParsing(nodesFile);
        String[] nodeHeaders = nodeParser.getRecordMetadata().headers();
        nodeParser.stopParsing();

        File edgesFile = new File(datasetDir, "edges.csv");
        CsvParser edgeParser = new CsvParser(settings);
        edgeParser.beginParsing(edgesFile);
        String[] edgeHeaders = edgeParser.getRecordMetadata().headers();
        edgeParser.stopParsing();

        // Load type metadata
        Map<String, Class<?>>[] typeMaps = readPropertyTypes(datasetDir);

        return new CsvMetadata(nodeHeaders, edgeHeaders, typeMaps[0], typeMaps[1]);
    }

    /**
     * Read property type metadata from type_meta.json.
     *
     * @param datasetDir Path to the dataset directory
     * @return Array of two maps: [nodePropertyTypes, edgePropertyTypes]
     */
    private static Map<String, Class<?>>[] readPropertyTypes(String datasetDir) {
        Map<String, Class<?>> nodePropertyTypes = new HashMap<>();
        Map<String, Class<?>> edgePropertyTypes = new HashMap<>();

        File typeMetaFile = new File(datasetDir, "type_meta.json");
        if (typeMetaFile.exists()) {
            try (FileReader reader = new FileReader(typeMetaFile)) {
                Gson gson = new Gson();
                JsonObject meta = gson.fromJson(reader, JsonObject.class);

                if (meta.has("node_properties")) {
                    JsonObject nodeProps = meta.getAsJsonObject("node_properties");
                    for (String key : nodeProps.keySet()) {
                        String typeStr = nodeProps.get(key).getAsString();
                        nodePropertyTypes.put(key, typeStringToClass(typeStr));
                    }
                }

                if (meta.has("edge_properties")) {
                    JsonObject edgeProps = meta.getAsJsonObject("edge_properties");
                    for (String key : edgeProps.keySet()) {
                        String typeStr = edgeProps.get(key).getAsString();
                        edgePropertyTypes.put(key, typeStringToClass(typeStr));
                    }
                }
            } catch (Exception e) {
                // Silently fall back to String for all properties if type_meta.json is invalid
            }
        }

        return new Map[]{nodePropertyTypes, edgePropertyTypes};
    }

    /**
     * Convert type string from type_meta.json to Java Class.
     */
    private static Class<?> typeStringToClass(String typeStr) {
        switch (typeStr.toLowerCase()) {
            case "integer": return Integer.class;
            case "long": return Long.class;
            case "float": return Float.class;
            case "double": return Double.class;
            case "boolean": return Boolean.class;
            default: return String.class;
        }
    }

    /**
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

        // Load type metadata
        Map<String, Class<?>>[] typeMaps = readPropertyTypes(datasetDir);

        return new CsvMetadata(nodeHeaders, edgeHeaders, typeMaps[0], typeMaps[1]);
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
