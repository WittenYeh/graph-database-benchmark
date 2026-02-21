cmake_minimum_required(VERSION 3.14)
project(arangodb-benchmark VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find common library
find_package(graphbench REQUIRED PATHS ${CMAKE_SOURCE_DIR}/../common-cpp)

# Dependencies
include(FetchContent)

# nlohmann/json
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(json)

# libcurl
find_package(CURL REQUIRED)

# No additional dependencies needed - using REST API with libcurl

# HTTP server library (cpp-httplib)
FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.14.3
)
FetchContent_MakeAvailable(httplib)

# csv-parser for CSV reading
FetchContent_Declare(
    csv_parser
    GIT_REPOSITORY https://github.com/vincentlaucsb/csv-parser.git
    GIT_TAG 2.5.0
)
FetchContent_MakeAvailable(csv_parser)

# Build benchmark executor
add_executable(arangodb_benchmark_server
    src/arangodb_benchmark_server.cpp
)

target_link_libraries(arangodb_benchmark_server PRIVATE
    graphbench::graphbench
    nlohmann_json::nlohmann_json
    CURL::libcurl
    httplib::httplib
    csv
)

target_include_directories(arangodb_benchmark_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${csv_parser_SOURCE_DIR}/single_include
)
