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

# Transforming .txt files into tabular (dataframe data) and save as a parquet file
echo "======================================================"
echo "Deploying the Transform Function"
echo "======================================================"

gcloud functions deploy transform_txt_to_dataframe \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point transform_txt_to_dataframe \
    --source ./extract-txt-and-transform/transform \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB