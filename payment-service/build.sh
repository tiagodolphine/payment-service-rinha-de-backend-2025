#!/bin/sh
IMAGE_NAME="tiagodolphine/payment-service:latest"

# Build the Docker image
docker build -t $IMAGE_NAME .

# Log in to Docker Hub
docker login

# Push the image to Docker Hub
docker push $IMAGE_NAME