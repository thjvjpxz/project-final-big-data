#!/bin/bash

# Start databases
docker-compose -f docker-compose.yml up -d

# Wait for databases to be ready
echo "Waiting for databases to start..."
sleep 5

# Build project
echo "Building project..."
mvn clean package -DskipTests

# Check if JAR file exists
JAR_FILE="target/project-final-big-data-1.0-SNAPSHOT-jar-with-dependencies.jar"
if [ -f "$JAR_FILE" ]; then
    echo "Starting application..."
    java -Dfile.encoding=UTF-8 -jar "$JAR_FILE"
else
    echo "Error: JAR file not found!"
    echo "Build failed or JAR file was not created"
    exit 1
fi