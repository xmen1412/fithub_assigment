#!/bin/bash

# log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# handle errors
handle_error() {
    log "Error: $1"
    exit 1
}

# Ensure the Docker container is running
log "Starting Docker container..."
docker-compose up -d || handle_error "Failed to start Docker container"

# Run the PySpark job
log "Running PySpark job..."
if [ "$1" ]; then
    # If a script name is provided as an argument, use that
    docker-compose exec -T pyspark spark-submit "/home/jovyan/scripts/$1" || handle_error "Failed to run PySpark job"
else
    docker-compose exec -T pyspark spark-submit /home/jovyan/scripts/main.py || handle_error "Failed to run PySpark job"
fi

log "PySpark job completed successfully"

# Uncomment the following line if you want to stop the container after the job is done
# docker-compose down

exit 0