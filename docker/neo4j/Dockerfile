FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

# Copy and build common module first
COPY common/pom.xml ./common/pom.xml
COPY common/src ./common/src
RUN cd common && mvn clean install -DskipTests

# Copy pom.xml and download dependencies
COPY docker/neo4j/pom.xml .
RUN mvn dependency:go-offline

# Copy source code and build
COPY docker/neo4j/src ./src
RUN mvn clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy built jar
COPY --from=builder /app/target/neo4j-benchmark-1.0-SNAPSHOT.jar /app/benchmark.jar

# Create data directories
RUN mkdir -p /data/datasets /data/workloads /tmp/neo4j-benchmark-db

# Expose API port
EXPOSE 50080

# Set environment variables
ENV JAVA_OPTS="-Xmx4G -Xms4G"
ENV API_PORT=50080

# Run the benchmark server
CMD ["sh", "-c", "java $JAVA_OPTS -jar /app/benchmark.jar"]
