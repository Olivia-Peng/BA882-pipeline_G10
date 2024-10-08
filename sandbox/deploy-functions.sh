######
## Simple script to deploy Google Cloud Functions
######

# Setup the project
gcloud config set project ba882-group-10

# CDC data extraction function deployment
echo "======================================================"
echo "Deploying the CDC data extraction function"
echo "======================================================"

gcloud functions deploy download-cdc-data \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./extract-txt-and-transform\extract-txt \
    --stage-bucket ba882-victorgf-stage-bucket \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB
