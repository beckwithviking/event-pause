#!/bin/bash
# Script to stop containers started by run_compose.sh

# Configuration matches run_compose.sh
KAFKA_CONTAINER="kafka-poc"
BACKEND_CONTAINER="backend"
FRONTEND_CONTAINER="frontend"

echo "Stopping and removing containers..."
podman rm -f $KAFKA_CONTAINER $BACKEND_CONTAINER $FRONTEND_CONTAINER 2>/dev/null || true

echo "Containers stopped and removed."
