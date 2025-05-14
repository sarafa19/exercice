#!/bin/bash

# Check if a LocalStack container is already running
if [ "$(docker ps -q -f name=localstack)" ]; then
    echo "LocalStack is already running."
else
    if [ "$(docker ps -aq -f name=localstack)" ]; then
        echo "LocalStack container exists but is stopped. Restarting..."
        docker start localstack
    else
        echo "LocalStack container does not exist. Starting a new container..."
        docker run -d -p 4566:4566 -p 4572:4572 --name localstack localstack/localstack
    fi
fi

# Then DVC
echo "LocalStack started or already running at $(date)" > localstack_running.log