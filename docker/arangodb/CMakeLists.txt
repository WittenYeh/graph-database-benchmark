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

# Fuerte (ArangoDB C++ driver)
FetchContent_Declare(
    fuerte
    GIT_REPOSITORY https://github.com/arangodb/arangodb.git
    GIT_TAG v3.12.7.2
    SOURCE_SUBDIR 3rdParty/fuerte
)
FetchContent_MakeAvailable(fuerte)

# libcurl
find_package(CURL REQUIRED)

# Boost
find_package(Boost REQUIRED COMPONENTS system)

# HTTP server library (cpp-httplib)
FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.14.3
)
FetchContent_MakeAvailable(httplib)

# Build benchmark executor
add_executable(arangodb_benchmark_server
    src/arangodb_benchmark_executor.cpp
    src/arangodb_benchmark_server.cpp
)

target_link_libraries(arangodb_benchmark_server PRIVATE
    graphbench::graphbench
    nlohmann_json::nlohmann_json
    fuerte
    CURL::libcurl
    Boost::system
    httplib::httplib
)

target_include_directories(arangodb_benchmark_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)
