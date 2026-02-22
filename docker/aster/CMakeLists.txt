cmake_minimum_required(VERSION 3.15)
project(aster-benchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find common-cpp
# In Docker build: /build/common-cpp, in local build: ../../common-cpp
if(EXISTS "/build/common-cpp")
    set(COMMON_CPP_DIR "/build/common-cpp")
else()
    set(COMMON_CPP_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../common-cpp)
endif()
include_directories(${COMMON_CPP_DIR}/include)

# Find Aster (RocksDB with graph extensions)
# In Docker build: /build/Aster, in local build: ../../third-party/Aster
if(EXISTS "/build/Aster")
    set(ASTER_DIR "/build/Aster")
else()
    set(ASTER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../third-party/Aster)
endif()
include_directories(${ASTER_DIR}/include)

# Find Aster static library
find_library(ROCKSDB_LIB NAMES rocksdb PATHS ${ASTER_DIR} NO_DEFAULT_PATH)
if(NOT ROCKSDB_LIB)
    message(FATAL_ERROR "librocksdb.a not found in ${ASTER_DIR}. Please build Aster first with 'make static_lib'")
endif()

# Find required packages
find_package(CURL REQUIRED)
find_package(Threads REQUIRED)

# Dependencies
include(FetchContent)

# nlohmann/json
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(json)

# cpp-httplib
FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.14.3
)
FetchContent_MakeAvailable(httplib)

# csv-parser
FetchContent_Declare(
    csv_parser
    GIT_REPOSITORY https://github.com/vincentlaucsb/csv-parser.git
    GIT_TAG 2.5.0
)
FetchContent_MakeAvailable(csv_parser)

# Source files
set(SOURCES
    src/aster_benchmark_server.cpp
)

# Create executable
add_executable(aster_benchmark_server ${SOURCES})

# Link libraries
target_link_libraries(aster_benchmark_server
    ${ROCKSDB_LIB}
    nlohmann_json::nlohmann_json
    httplib::httplib
    csv
    ${CURL_LIBRARIES}
    Threads::Threads
    dl
    z
    bz2
    lz4
    zstd
    snappy
)

# Include directories
target_include_directories(aster_benchmark_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${csv_parser_SOURCE_DIR}/single_include
)

# Installation
install(TARGETS aster_benchmark_server DESTINATION /app)
