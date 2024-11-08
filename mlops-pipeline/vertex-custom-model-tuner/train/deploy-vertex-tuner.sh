#!/bin/bash

# Variables
PROJECT_ID=ba882-group-10              # Your GCP project ID
REGION=us-central1                      # GCP region for Vertex AI
TRAIN_IMAGE_NAME=sarima-tuning          # Name for the SARIMA training image
TRAIN_IMAGE_URI=gcr.io/$PROJECT_ID/$TRAIN_IMAGE_NAME:latest  # Full image URI

echo "======================================================"
echo "Setting the GCP project"
echo "======================================================"

gcloud config set project $PROJECT_ID

echo "======================================================"
echo "Enabling necessary APIs"
echo "======================================================"

gcloud services enable aiplatform.googleapis.com
gcloud services enable storage.googleapis.com

echo "======================================================"
echo "Building the training Docker image locally and pushing to GCR"
echo "======================================================"

docker build -t $TRAIN_IMAGE_URI .
docker push $TRAIN_IMAGE_URI

echo "======================================================"
echo "Launching a custom training job on Vertex AI"
echo "======================================================"

gcloud ai custom-jobs create \
  --region=$REGION \
  --display-name=sarima-tuning-training \
  --config=worker-pool-spec.yml
