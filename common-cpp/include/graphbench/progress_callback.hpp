#pragma once

#include <string>
#include <optional>
#include <nlohmann/json.hpp>
#include <curl/curl.h>

namespace graphbench {

using json = nlohmann::json;

/**
 * Structured parameter object for progress events.
 */
struct ProgressEvent {
    std::string event;
    std::string taskName;
    std::optional<std::string> workloadFile;
    std::optional<std::string> status;
    std::optional<double> durationSeconds;
    int taskIndex = 0;
    int totalTasks = 0;
    std::optional<int> originalOpsCount;
    std::optional<int> validOpsCount;
    std::optional<int> filteredOpsCount;
    std::optional<int> numOps;

    ProgressEvent(const std::string& event, const std::string& taskName)
        : event(event), taskName(taskName) {}

    ProgressEvent& setWorkloadFile(const std::string& file) {
        workloadFile = file;
        return *this;
    }

    ProgressEvent& setStatus(const std::string& s) {
        status = s;
        return *this;
    }

    ProgressEvent& setDuration(double duration) {
        durationSeconds = duration;
        return *this;
    }

    ProgressEvent& setTaskProgress(int index, int total) {
        taskIndex = index;
        totalTasks = total;
        return *this;
    }

    ProgressEvent& setOpsCounts(int original, int valid, int filtered) {
        originalOpsCount = original;
        validOpsCount = valid;
        filteredOpsCount = filtered;
        return *this;
    }

    ProgressEvent& setNumOps(int ops) {
        numOps = ops;
        return *this;
    }
};

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
     * Send progress callback to host using structured event object.
     */
    void sendProgressCallback(const ProgressEvent& event) {
        if (callbackUrl_.empty()) {
            return;
        }

        json payload = {
            {"event", event.event},
            {"task_name", event.taskName},
            {"task_index", event.taskIndex},
            {"total_tasks", event.totalTasks}
        };

        if (event.workloadFile.has_value()) {
            payload["workload_file"] = event.workloadFile.value();
        }
        if (event.status.has_value()) {
            payload["status"] = event.status.value();
        }
        if (event.durationSeconds.has_value()) {
            payload["duration_seconds"] = event.durationSeconds.value();
        }
        if (event.originalOpsCount.has_value()) {
            payload["original_ops_count"] = event.originalOpsCount.value();
        }
        if (event.validOpsCount.has_value()) {
            payload["valid_ops_count"] = event.validOpsCount.value();
        }
        if (event.filteredOpsCount.has_value()) {
            payload["filtered_ops_count"] = event.filteredOpsCount.value();
        }
        if (event.numOps.has_value()) {
            payload["num_ops"] = event.numOps.value();
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
