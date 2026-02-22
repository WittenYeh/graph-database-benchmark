FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app

# Copy and build common module first
COPY common-java/pom.xml ./common-java/pom.xml
COPY common-java/src ./common-java/src
RUN cd common-java && mvn clean install -DskipTests

# Copy pom.xml and download dependencies
COPY docker/janusgraph/pom.xml .
RUN mvn dependency:go-offline

# Copy source code and build
COPY docker/janusgraph/src ./src
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy built jar
COPY --from=builder /app/target/janusgraph-benchmark-1.0-SNAPSHOT.jar /app/benchmark.jar

# Create data directories
RUN mkdir -p /data/datasets /data/workloads /tmp/janusgraph-benchmark-db

# Expose API port
EXPOSE 50081

# Set environment variables
ENV JAVA_OPTS="-Xmx4G -Xms4G"
ENV API_PORT=50081
ENV DB_TYPE=janusgraph

# Run the benchmark server
CMD ["sh", "-c", "java $JAVA_OPTS -cp /app/benchmark.jar com.graphbench.janusgraph.JanusGraphBenchmarkServer"]
