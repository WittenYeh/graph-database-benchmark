FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libcurl4-openssl-dev \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    libboost-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy Aster source code
COPY third-party/Aster /build/Aster

# Build Aster static library
WORKDIR /build/Aster
# Disable all -Werror flags to allow compilation
RUN sed -i 's/-Werror[^ ]*//g' Makefile && \
    make static_lib -j$(nproc)

# Copy common-cpp
COPY common-cpp /build/common-cpp

# Copy Aster benchmark source
COPY docker/aster /build/docker/aster

# Build Aster benchmark
WORKDIR /build/docker/aster/build
RUN cmake .. && make -j$(nproc)

# Runtime stage
FROM ubuntu:22.04

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libcurl4 \
    libsnappy1v5 \
    zlib1g \
    libbz2-1.0 \
    liblz4-1 \
    libzstd1 \
    && rm -rf /var/lib/apt/lists/*

# Copy built executable
COPY --from=builder /build/docker/aster/build/aster_benchmark_server /app/

# Create data directories
RUN mkdir -p /data/datasets /data/workloads /tmp/aster-benchmark-db

# Set environment variables
ENV API_PORT=8080
ENV DB_TYPE=aster

# Expose API port
EXPOSE 8080

# Run the benchmark server
CMD ["/app/aster_benchmark_server"]
