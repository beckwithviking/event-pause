#!/bin/bash
set -e

# Define container name
CONTAINER_NAME="kafka-poc"

# Check if container exists and remove it
if podman ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "Stopping and removing existing container: ${CONTAINER_NAME}"
    podman stop ${CONTAINER_NAME}
    podman rm ${CONTAINER_NAME}
fi

echo "Starting Kafka (Kraft Mode)..."

# Run Kafka using Apache official image (Kraft mode)
# We expose port 9092 for the host.
podman run -d \
    --name ${CONTAINER_NAME} \
    -p 9092:9092 \
    -e KAFKA_NODE_ID=0 \
    -e KAFKA_PROCESS_ROLES=controller,broker \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
    apache/kafka:3.7.0

echo "Waiting for Kafka to be ready..."
# Simple wait loop (could be more robust)
sleep 10
podman logs ${CONTAINER_NAME} | grep "Kafka Server started" || echo "Kafka might still be starting, check logs with 'podman logs ${CONTAINER_NAME}'"

echo "Kafka started on localhost:9092"
