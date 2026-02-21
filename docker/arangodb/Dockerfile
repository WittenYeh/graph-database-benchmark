FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libcurl4-openssl-dev \
    libssl-dev \
    libboost-all-dev \
    wget \
    gnupg2 \
    && rm -rf /var/lib/apt/lists/*

# Install ArangoDB 3.12.7.2
RUN wget -q https://download.arangodb.com/arangodb312/DEBIAN/Release.key -O- | apt-key add - && \
    echo 'deb https://download.arangodb.com/arangodb312/DEBIAN/ /' | tee /etc/apt/sources.list.d/arangodb.list && \
    apt-get update && \
    apt-get install -y arangodb3=3.12.7-2 arangodb3-client=3.12.7-2 && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy common C++ library
COPY common-cpp /app/common-cpp

# Copy ArangoDB benchmark executor source
COPY docker/arangodb /app/arangodb

# Build the benchmark executor
WORKDIR /app/arangodb
RUN mkdir -p build && cd build && \
    cmake .. && \
    make -j$(nproc)

# Expose API port
EXPOSE 50082

# Run the benchmark server
CMD ["./build/arangodb_benchmark_server"]
