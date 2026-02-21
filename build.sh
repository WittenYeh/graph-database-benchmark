#!/bin/bash

# Build script for Docker images

set -e

echo "=========================================="
echo "Cleaning up old containers and images..."
echo "=========================================="

# Stop and remove containers if they exist
echo "Stopping containers..."
docker stop neo4j-benchmark janusgraph-benchmark arangodb-benchmark 2>/dev/null || true
docker rm neo4j-benchmark janusgraph-benchmark arangodb-benchmark 2>/dev/null || true

# Remove old images if they exist
echo "Removing old images..."
docker rmi bench-neo4j bench-janusgraph bench-arangodb 2>/dev/null || true

echo
echo "=========================================="
echo "Building Neo4j benchmark image..."
echo "=========================================="
docker build -t bench-neo4j -f ./docker/neo4j/Dockerfile .

echo
echo "=========================================="
echo "Building JanusGraph benchmark image..."
echo "=========================================="
docker build -t bench-janusgraph -f ./docker/janusgraph/Dockerfile .

echo
echo "=========================================="
echo "Building ArangoDB benchmark image..."
echo "=========================================="
docker build -t bench-arangodb -f ./docker/arangodb/Dockerfile .

echo
echo "=========================================="
echo "All images built successfully!"
echo "=========================================="
docker images | grep bench-

