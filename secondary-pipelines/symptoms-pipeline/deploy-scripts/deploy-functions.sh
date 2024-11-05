######
## Simple script to deploy Google Cloud Functions
######

# Setup the project
gcloud config set project ba882-group-10

# Disease symptoms table schema set-up
echo "======================================================"
echo "Deploying the disease symptoms table schema"
echo "======================================================"

gcloud functions deploy create-symptom-schema \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point create_symptom_schema \
    --source ./functions/schema-setup \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
    --timeout 300s



# Populate disease symptoms table
echo "======================================================"
echo "Deploying populating disease symptoms table"
echo "======================================================"

gcloud functions deploy populate-symptoms-table \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point run_scraping_to_bigquery \
    --source ./functions/symptoms-info \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 300s

    