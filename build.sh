#!/bin/bash

# Build script for Docker images
#
# Usage:
#   ./build.sh                    # Build all databases
#   ./build.sh neo4j              # Build only Neo4j
#   ./build.sh neo4j janusgraph   # Build Neo4j and JanusGraph
#   ./build.sh sqlg               # Build only SQLG

set -e

# All available databases
ALL_DATABASES=("neo4j" "janusgraph" "arangodb" "orientdb" "aster" "sqlg")

# If no arguments provided, build all databases
if [ $# -eq 0 ]; then
    DATABASES_TO_BUILD=("${ALL_DATABASES[@]}")
else
    DATABASES_TO_BUILD=("$@")
fi

# Function to build a specific database
build_database() {
    local db=$1
    case $db in
        neo4j)
            echo
            echo "=========================================="
            echo "Building Neo4j benchmark image..."
            echo "=========================================="
            docker build -t bench-neo4j -f ./docker/neo4j/Dockerfile .
            ;;
        janusgraph)
            echo
            echo "=========================================="
            echo "Building JanusGraph benchmark image..."
            echo "=========================================="
            docker build -t bench-janusgraph -f ./docker/janusgraph/Dockerfile .
            ;;
        arangodb)
            echo
            echo "=========================================="
            echo "Building ArangoDB benchmark image..."
            echo "=========================================="
            docker build -t bench-arangodb -f ./docker/arangodb/Dockerfile .
            ;;
        orientdb)
            echo
            echo "=========================================="
            echo "Building OrientDB benchmark image..."
            echo "=========================================="
            docker build -t bench-orientdb -f ./docker/orientdb/Dockerfile .
            ;;
        aster)
            echo
            echo "=========================================="
            echo "Building Aster benchmark image..."
            echo "=========================================="
            docker build -t bench-aster -f ./docker/aster/Dockerfile .
            ;;
        sqlg)
            echo
            echo "=========================================="
            echo "Building SQLG benchmark image..."
            echo "=========================================="
            docker build -t bench-sqlg -f ./docker/sqlg/Dockerfile .
            ;;
        *)
            echo "Unknown database: $db"
            echo "Available databases: ${ALL_DATABASES[*]}"
            exit 1
            ;;
    esac
}

# Cleanup function for specified databases
cleanup_databases() {
    local containers=()
    local images=()

    for db in "${DATABASES_TO_BUILD[@]}"; do
        containers+=("${db}-benchmark")
        images+=("bench-${db}")
    done

    if [ ${#containers[@]} -gt 0 ]; then
        echo "=========================================="
        echo "Cleaning up old containers and images..."
        echo "=========================================="

        echo "Stopping containers..."
        docker stop "${containers[@]}" 2>/dev/null || true
        docker rm "${containers[@]}" 2>/dev/null || true

        echo "Removing old images..."
        docker rmi "${images[@]}" 2>/dev/null || true
    fi
}

# Cleanup specified databases
cleanup_databases

# Build each specified database
for db in "${DATABASES_TO_BUILD[@]}"; do
    build_database "$db"
done

echo
echo "=========================================="
echo "Build complete!"
echo "=========================================="
docker images | grep bench-

