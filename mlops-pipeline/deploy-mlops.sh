# Set the project
gcloud config set project ba882-group-10

echo "======================================================"
echo "Deploying the CDC disease occurrences view creation function"
echo "======================================================"

gcloud functions deploy create_cdc_views \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/retrieve-train-data \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 600s
