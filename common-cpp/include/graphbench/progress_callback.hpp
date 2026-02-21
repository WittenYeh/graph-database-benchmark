#pragma once

#include <string>
#include <nlohmann/json.hpp>
#include <curl/curl.h>

namespace graphbench {

using json = nlohmann::json;

/**
 * Handles progress callbacks and logging to the host server.
 * Similar to Java's ProgressCallback class.
 */
class ProgressCallback {
public:
    explicit ProgressCallback(const std::string& callbackUrl)
        : callbackUrl_(callbackUrl) {
        curl_global_init(CURL_GLOBAL_DEFAULT);
    }

    ~ProgressCallback() {
        curl_global_cleanup();
    }

    /**
     * Send progress callback to host.
     */
    void sendProgressCallback(const std::string& event,
                             const std::string& taskName,
                             const std::string& workloadFile,
                             const std::string& status,
                             double durationSeconds,
                             int taskIndex,
                             int totalTasks) {
        sendProgressCallback(event, taskName, workloadFile, status, durationSeconds,
                           taskIndex, totalTasks, -1, -1, -1);
    }

    /**
     * Send progress callback to host with operation counts.
     */
    void sendProgressCallback(const std::string& event,
                             const std::string& taskName,
                             const std::string& workloadFile,
                             const std::string& status,
                             double durationSeconds,
                             int taskIndex,
                             int totalTasks,
                             int originalOpsCount,
                             int validOpsCount,
                             int filteredOpsCount) {
        if (callbackUrl_.empty()) {
            return;
        }

        json payload = {
            {"event", event},
            {"task_name", taskName},
            {"task_index", taskIndex},
            {"total_tasks", totalTasks}
        };

        if (!workloadFile.empty()) {
            payload["workload_file"] = workloadFile;
        }
        if (!status.empty()) {
            payload["status"] = status;
        }
        if (durationSeconds >= 0) {
            payload["duration_seconds"] = durationSeconds;
        }
        if (originalOpsCount >= 0) {
            payload["original_ops_count"] = originalOpsCount;
        }
        if (validOpsCount >= 0) {
            payload["valid_ops_count"] = validOpsCount;
        }
        if (filteredOpsCount >= 0) {
            payload["filtered_ops_count"] = filteredOpsCount;
        }

        sendHttpPost(payload.dump());
    }

    /**
     * Send log message to host.
     */
    void sendLogMessage(const std::string& message, const std::string& level) {
        if (callbackUrl_.empty()) {
            return;
        }

        json payload = {
            {"event", "log_message"},
            {"message", message},
            {"level", level}
        };

        sendHttpPost(payload.dump());
    }

    /**
     * Send error message to host.
     */
    void sendErrorMessage(const std::string& message, const std::string& errorType) {
        if (callbackUrl_.empty()) {
            return;
        }

        json payload = {
            {"event", "error_message"},
            {"message", message},
            {"error_type", errorType}
        };

        sendHttpPost(payload.dump());
    }

private:
    std::string callbackUrl_;

    static size_t writeCallback(void* contents, size_t size, size_t nmemb, void* userp) {
        return size * nmemb;
    }

    void sendHttpPost(const std::string& jsonPayload) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            return;
        }

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");

        curl_easy_setopt(curl, CURLOPT_URL, callbackUrl_.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, jsonPayload.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);

        CURLcode res = curl_easy_perform(curl);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }
};

} // namespace graphbench
