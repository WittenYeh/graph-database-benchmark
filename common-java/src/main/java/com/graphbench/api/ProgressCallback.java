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
     * Structured parameter object for progress events.
     */
    public static class ProgressEvent {
        public String event;
        public String taskName;
        public String workloadFile;
        public String status;
        public Double durationSeconds;
        public int taskIndex;
        public int totalTasks;
        public Integer originalOpsCount;
        public Integer validOpsCount;
        public Integer filteredOpsCount;
        public Integer numOps;

        public ProgressEvent(String event, String taskName) {
            this.event = event;
            this.taskName = taskName;
        }

        public ProgressEvent workloadFile(String workloadFile) {
            this.workloadFile = workloadFile;
            return this;
        }

        public ProgressEvent status(String status) {
            this.status = status;
            return this;
        }

        public ProgressEvent duration(Double durationSeconds) {
            this.durationSeconds = durationSeconds;
            return this;
        }

        public ProgressEvent taskProgress(int taskIndex, int totalTasks) {
            this.taskIndex = taskIndex;
            this.totalTasks = totalTasks;
            return this;
        }

        public ProgressEvent opsCounts(Integer originalOpsCount, Integer validOpsCount, Integer filteredOpsCount) {
            this.originalOpsCount = originalOpsCount;
            this.validOpsCount = validOpsCount;
            this.filteredOpsCount = filteredOpsCount;
            return this;
        }

        public ProgressEvent numOps(Integer numOps) {
            this.numOps = numOps;
            return this;
        }
    }

    /**
     * Send progress callback to host using structured event object.
     */
    public void sendProgressCallback(ProgressEvent event) {
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
            payload.put("event", event.event);
            payload.put("task_name", event.taskName);
            payload.put("task_index", event.taskIndex);
            payload.put("total_tasks", event.totalTasks);
            if (event.workloadFile != null) {
                payload.put("workload_file", event.workloadFile);
            }
            if (event.status != null) {
                payload.put("status", event.status);
            }
            if (event.durationSeconds != null) {
                payload.put("duration_seconds", event.durationSeconds);
            }
            if (event.originalOpsCount != null) {
                payload.put("original_ops_count", event.originalOpsCount);
            }
            if (event.validOpsCount != null) {
                payload.put("valid_ops_count", event.validOpsCount);
            }
            if (event.filteredOpsCount != null) {
                payload.put("filtered_ops_count", event.filteredOpsCount);
            }
            if (event.numOps != null) {
                payload.put("num_ops", event.numOps);
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
