#!/bin/bash
set -e

CONTAINER_NAME="kafka-poc"
BOOTSTRAP_SERVER="localhost:9092"

echo "Creating Topics..."

# Function to create topic if it doesn't exist
create_topic() {
    TOPIC_NAME=$1
    CONFIG=$2
    
    echo "Creating topic: $TOPIC_NAME"
    podman exec ${CONTAINER_NAME} /opt/kafka/bin/kafka-topics.sh --create \
        --bootstrap-server ${BOOTSTRAP_SERVER} \
        --topic ${TOPIC_NAME} \
        --partitions 6 \
        --replication-factor 1 \
        $CONFIG || echo "Topic $TOPIC_NAME might already exist"
}

# 1. Status Topics (Compacted) - CRITICAL
# Source of truth for Key Status (Active/Paused)
create_topic "demo-status" "--config cleanup.policy=compact"
create_topic "orders-status" "--config cleanup.policy=compact"
create_topic "payments-status" "--config cleanup.policy=compact"

# 2. Topics for 'Demo' flow
create_topic "demo-in" ""      # Main Events (Input)
create_topic "demo-out" ""     # Processed Events (Output)
create_topic "demo-resume" ""  # Resume Trigger (Command Stream)

# 3. Topics for 'Orders' flow
create_topic "orders-in" ""
create_topic "orders-out" ""
create_topic "orders-resume" ""

# 4. Topics for 'Payments' flow
create_topic "payments-in" ""
create_topic "payments-out" ""
create_topic "payments-resume" ""

echo "Topics created."
podman exec ${CONTAINER_NAME} /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server ${BOOTSTRAP_SERVER}
