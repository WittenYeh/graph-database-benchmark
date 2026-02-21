cmake_minimum_required(VERSION 3.14)
project(graphbench-common-cpp VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Header-only library
add_library(graphbench INTERFACE)
target_include_directories(graphbench INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Dependencies
include(FetchContent)

# nlohmann/json for JSON handling
FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(json)

# libcurl for HTTP callbacks
find_package(CURL REQUIRED)

# Link dependencies - use generator expressions to avoid export issues
target_link_libraries(graphbench INTERFACE
    $<BUILD_INTERFACE:nlohmann_json::nlohmann_json>
    CURL::libcurl
)

# For installed version, users need to find nlohmann_json themselves
target_include_directories(graphbench INTERFACE
    $<INSTALL_INTERFACE:${nlohmann_json_INCLUDE_DIRS}>
)

# Installation
install(TARGETS graphbench
    EXPORT graphbench-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

install(DIRECTORY include/
    DESTINATION include
)

install(EXPORT graphbench-targets
    FILE graphbench-targets.cmake
    NAMESPACE graphbench::
    DESTINATION lib/cmake/graphbench
)

# Package config
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/graphbench-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/graphbench-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/graphbench-config.cmake"
    INSTALL_DESTINATION lib/cmake/graphbench
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/graphbench-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/graphbench-config-version.cmake"
    DESTINATION lib/cmake/graphbench
)