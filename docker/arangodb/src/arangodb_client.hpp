#pragma once

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <string>
#include <memory>
#include <stdexcept>

namespace graphbench {

using json = nlohmann::json;

/**
 * Client class for ArangoDB REST API operations.
 * Provides common functionality for executing HTTP requests to ArangoDB.
 */
class ArangoDBClient {
private:
    std::string baseUrl;
    std::string username;
    std::string password;
    std::string currentDatabase;

    // Callback for libcurl to write response data
    static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* userp) {
        userp->append((char*)contents, size * nmemb);
        return size * nmemb;
    }

public:
    ArangoDBClient(const std::string& host, int port,
                const std::string& user = "root", const std::string& pass = "")
        : baseUrl("http://" + host + ":" + std::to_string(port)),
          username(user), password(pass), currentDatabase("_system") {}

    /**
     * Set the current database context for subsequent operations.
     */
    void useDatabase(const std::string& dbName) {
        currentDatabase = dbName;
    }

    /**
     * Execute HTTP request to ArangoDB.
     * @param method HTTP method (GET, POST, DELETE, etc.)
     * @param endpoint API endpoint (e.g., "/_api/cursor")
     * @param payload Request body as JSON (optional)
     * @return Response body as JSON object
     */
    json executeRequest(const std::string& method, const std::string& endpoint,
                       const json& payload = json::object()) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            throw std::runtime_error("Failed to initialize CURL");
        }

        std::string responseStr;
        std::string url = baseUrl + endpoint;

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseStr);

        // Set authentication
        if (!username.empty()) {
            std::string auth = username + ":" + password;
            curl_easy_setopt(curl, CURLOPT_USERPWD, auth.c_str());
        }

        // Set HTTP method and payload
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        std::string payloadStr;
        if (method == "POST" || method == "PUT" || method == "PATCH") {
            payloadStr = payload.dump();
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payloadStr.c_str());
            if (method == "PUT") {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
            } else if (method == "PATCH") {
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
            }
        } else if (method == "DELETE") {
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        }

        CURLcode res = curl_easy_perform(curl);

        long httpCode = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK) {
            throw std::runtime_error("CURL request failed: " + std::string(curl_easy_strerror(res)));
        }

        if (httpCode >= 400) {
            throw std::runtime_error("HTTP request failed with code " + std::to_string(httpCode) +
                                   ": " + responseStr);
        }

        if (responseStr.empty()) {
            return json::object();
        }

        return json::parse(responseStr);
    }

    /**
     * Execute AQL query with optional bind variables.
     * @param query AQL query string
     * @param bindVars JSON object containing bind variables
     * @return Response body as JSON object
     */
    json executeAQL(const std::string& query, const json& bindVars = json::object()) {
        json payload = {{"query", query}};
        if (!bindVars.empty()) {
            payload["bindVars"] = bindVars;
        }
        // Use database-specific endpoint
        std::string endpoint = "/_db/" + currentDatabase + "/_api/cursor";
        return executeRequest("POST", endpoint, payload);
    }

    /**
     * Execute AQL query and return result array.
     * @param query AQL query string
     * @param bindVars JSON object containing bind variables
     * @return JSON array of results
     */
    json executeAQLWithResults(const std::string& query, const json& bindVars = json::object()) {
        json response = executeAQL(query, bindVars);
        if (response.contains("result") && response["result"].is_array()) {
            return response["result"];
        }
        return json::array();
    }

    /**
     * Create a database.
     */
    void createDatabase(const std::string& dbName) {
        json payload = {{"name", dbName}};
        executeRequest("POST", "/_api/database", payload);
    }

    /**
     * Drop a database.
     */
    void dropDatabase(const std::string& dbName) {
        executeRequest("DELETE", "/_api/database/" + dbName);
    }

    /**
     * Create a collection.
     */
    void createCollection(const std::string& dbName, const std::string& collectionName,
                         bool isEdgeCollection = false) {
        json payload = {
            {"name", collectionName},
            {"type", isEdgeCollection ? 3 : 2}  // 2 = document, 3 = edge
        };
        executeRequest("POST", "/_db/" + dbName + "/_api/collection", payload);
    }

    /**
     * Create a persistent index on a collection.
     */
    void createIndex(const std::string& dbName, const std::string& collectionName,
                    const std::vector<std::string>& fields) {
        json payload = {
            {"type", "persistent"},
            {"fields", fields}
        };
        executeRequest("POST", "/_db/" + dbName + "/_api/index?collection=" + collectionName, payload);
    }
};

} // namespace graphbench
