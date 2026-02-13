#!/bin/bash

# Complete setup script for new users

set -e

echo "=========================================="
echo "Graph Database Benchmark - Setup Script"
echo "=========================================="
echo

# Check prerequisites
echo "Checking prerequisites..."

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi
echo "✓ Python 3 found: $(python3 --version)"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker."
    exit 1
fi
echo "✓ Docker found: $(docker --version)"

# Check Maven (optional, only needed for local builds)
if command -v mvn &> /dev/null; then
    echo "✓ Maven found: $(mvn --version | head -1)"
else
    echo "⚠ Maven not found (optional, Docker will use Maven inside containers)"
fi

echo

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt
echo "✓ Python dependencies installed"
echo

# Initialize git submodules for datasets
echo "Initializing dataset submodule..."
if [ -d "graph-datasets/.git" ]; then
    echo "✓ Dataset submodule already initialized"
else
    git submodule update --init --recursive
    echo "✓ Dataset submodule initialized"
fi
echo

# Build Docker images
echo "Building Docker images (this may take several minutes)..."
./build.sh
echo

# Validate project structure
echo "Validating project structure..."
python validate.py
echo

echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo
echo "Next steps:"
echo "  1. Run a quick test:"
echo "     make test"
echo
echo "  2. Run a full benchmark:"
echo "     ./run_benchmark.sh neo4j coAuthorsDBLP"
echo
echo "  3. Generate visualizations:"
echo "     make visualize"
echo
echo "For more information, see:"
echo "  - README.md for full documentation"
echo "  - QUICKSTART.md for quick start guide"
echo "  - Makefile for available commands"
echo
