FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libcurl4-openssl-dev \
    libssl-dev \
    wget \
    gnupg2 \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Install ArangoDB 3.12.7
RUN wget https://download.arangodb.com/arangodb312/DEBIAN/Release.key -O- | gpg --dearmor -o /usr/share/keyrings/arangodb-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/arangodb-archive-keyring.gpg] https://download.arangodb.com/arangodb312/DEBIAN/ /" | tee /etc/apt/sources.list.d/arangodb.list && \
    apt-get update && \
    apt-get install -y arangodb3 && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy common C++ library
COPY common-cpp /app/common-cpp

# Build and install common-cpp library
WORKDIR /app/common-cpp
RUN mkdir -p build && cd build && \
    cmake .. && \
    make install

# Copy ArangoDB benchmark executor source
COPY docker/arangodb /app/arangodb

# Build the benchmark executor
WORKDIR /app/arangodb
RUN mkdir -p build && cd build && \
    cmake .. && \
    make -j$(nproc)

# Create supervisor config
RUN mkdir -p /var/log/supervisor
COPY docker/arangodb/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose ports
EXPOSE 8529 50082

# Run supervisor to manage both ArangoDB and benchmark server
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

