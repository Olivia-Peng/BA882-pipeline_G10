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
    --source ./extract-txt-and-transform/extract-txt \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB

# BigQuery schema creation function deployment
echo "======================================================"
echo "Deploying the BigQuery schema creation function"
echo "======================================================"

gcloud functions deploy create-schema \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point create_schema \
    --source ./extract-txt-and-transform/schema-setup \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 256MB

# BigQuery upload function deployment
echo "======================================================"
echo "Deploying the BigQuery upload function"
echo "======================================================"

gcloud functions deploy upload-txt-to-bigquery \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point upload_txt_to_bigquery \
    --source ./extract-txt-and-transform/load \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB