package com.graphbench.api;

import com.google.gson.Gson;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles progress callbacks and logging to the host server.
 */
public class ProgressCallback {
    private static final Gson gson = new Gson();
    private final String callbackUrl;

    public ProgressCallback(String callbackUrl) {
        this.callbackUrl = callbackUrl;
    }

    /**
     * Send progress callback to host.
     */
    public void sendProgressCallback(String event, String taskName, String workloadFile,
                                     String status, Double durationSeconds, int taskIndex, int totalTasks) {
        sendProgressCallback(event, taskName, workloadFile, status, durationSeconds, taskIndex, totalTasks, null, null, null);
    }

    /**
     * Send progress callback to host with operation counts.
     */
    public void sendProgressCallback(String event, String taskName, String workloadFile,
                                     String status, Double durationSeconds, int taskIndex, int totalTasks,
                                     Integer originalOpsCount, Integer validOpsCount, Integer filteredOpsCount) {
        if (callbackUrl == null || callbackUrl.isEmpty()) {
            return;
        }

        try {
            URL url = new URL(callbackUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            Map<String, Object> payload = new HashMap<>();
            payload.put("event", event);
            payload.put("task_name", taskName);
            payload.put("task_index", taskIndex);
            payload.put("total_tasks", totalTasks);
            if (workloadFile != null) {
                payload.put("workload_file", workloadFile);
            }
            if (status != null) {
                payload.put("status", status);
            }
            if (durationSeconds != null) {
                payload.put("duration_seconds", durationSeconds);
            }
            if (originalOpsCount != null) {
                payload.put("original_ops_count", originalOpsCount);
            }
            if (validOpsCount != null) {
                payload.put("valid_ops_count", validOpsCount);
            }
            if (filteredOpsCount != null) {
                payload.put("filtered_ops_count", filteredOpsCount);
            }

            String jsonPayload = gson.toJson(payload);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes("UTF-8"));
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.println("Progress callback failed with code: " + responseCode);
            }
            conn.disconnect();
        } catch (Exception e) {
            // Silently ignore callback failures to not disrupt benchmark execution
            System.err.println("Failed to send progress callback: " + e.getMessage());
        }
    }

    /**
     * Send log message to host.
     */
    public void sendLogMessage(String message, String level) {
        if (callbackUrl == null || callbackUrl.isEmpty()) {
            return;
        }

        try {
            URL url = new URL(callbackUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            Map<String, Object> payload = new HashMap<>();
            payload.put("event", "log_message");
            payload.put("message", message);
            payload.put("level", level);

            String jsonPayload = gson.toJson(payload);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes("UTF-8"));
            }

            conn.getResponseCode();
            conn.disconnect();
        } catch (Exception e) {
            // Silently ignore
        }
    }

    /**
     * Send error message to host.
     */
    public void sendErrorMessage(String message, String errorType) {
        if (callbackUrl == null || callbackUrl.isEmpty()) {
            return;
        }

        try {
            URL url = new URL(callbackUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            Map<String, Object> payload = new HashMap<>();
            payload.put("event", "error_message");
            payload.put("message", message);
            payload.put("error_type", errorType);

            String jsonPayload = gson.toJson(payload);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes("UTF-8"));
            }

            conn.getResponseCode();
            conn.disconnect();
        } catch (Exception e) {
            // Silently ignore
        }
    }
}
