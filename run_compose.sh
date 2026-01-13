#!/bin/bash
set -e

# Configuration
NETWORK_NAME="anionet"
KAFKA_CONTAINER="kafka-poc"
BACKEND_CONTAINER="backend"
FRONTEND_CONTAINER="frontend"
BACKEND_IMAGE="event-pause-backend"
FRONTEND_IMAGE="event-pause-frontend"

echo "=========================================="
echo "Starting deployment with Native Podman"
echo "=========================================="

# 1. Cleanup
echo "Cleaning up previous containers..."
podman rm -f $KAFKA_CONTAINER $BACKEND_CONTAINER $FRONTEND_CONTAINER 2>/dev/null || true

# 2. Network Setup
echo "Setting up network: $NETWORK_NAME"
if ! podman network exists $NETWORK_NAME; then
    podman network create $NETWORK_NAME
fi

# 3. Start Kafka
echo "Starting Kafka..."
podman run -d \
    --name $KAFKA_CONTAINER \
    --network $NETWORK_NAME \
    --network-alias $KAFKA_CONTAINER \
    -p 9092:9092 \
    -e KAFKA_NODE_ID=0 \
    -e KAFKA_PROCESS_ROLES=controller,broker \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=0@$KAFKA_CONTAINER:9093 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$KAFKA_CONTAINER:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    apache/kafka:3.7.0

# 4. Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 20

echo "Initializing Topics..."
# Use shared init script
./scripts/init_topics.sh

echo "Topics initialized."

# 5. Build and Start Backend
echo "Building Backend..."
podman build -t $BACKEND_IMAGE ./backend

echo "Starting Backend..."
podman run -d \
    --name $BACKEND_CONTAINER \
    --network $NETWORK_NAME \
    -p 8080:8080 \
    -e SPRING_KAFKA_BOOTSTRAP_SERVERS=$KAFKA_CONTAINER:9092 \
    -e SPRING_KAFKA_STREAMS_BOOTSTRAP_SERVERS=$KAFKA_CONTAINER:9092 \
    $BACKEND_IMAGE

# 6. Build and Start Frontend
echo "Building Frontend..."
podman build -t $FRONTEND_IMAGE ./frontend

echo "Starting Frontend..."
podman run -d \
    --name $FRONTEND_CONTAINER \
    --network $NETWORK_NAME \
    -p 4200:80 \
    $FRONTEND_IMAGE
echo "Deployment complete."
echo "Frontend: http://localhost:4200"
echo "Backend:  http://localhost:8080"
