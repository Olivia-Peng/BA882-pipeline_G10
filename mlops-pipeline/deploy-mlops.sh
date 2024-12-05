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


echo "======================================================"
echo "Deploying the MLOps Model Schema Creation Function"
echo "======================================================"

gcloud functions deploy mlops_create_schema \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point create_schema \
    --source ./functions/schema-setup \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1024MB \
    --timeout 600s


echo "======================================================"
echo "Deploying the SARIMA Model Training Function"
echo "======================================================"

gcloud functions deploy train_sarima_models \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point train_sarima_models \
    --source ./functions/trainer \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 2048MB \
    --timeout 600s



echo "======================================================"
echo "Deploying the SARIMA Model Predictions Function"
echo "======================================================"

gcloud functions deploy predict_sarima_models \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point predict_with_latest_models \
    --source ./functions/predictions \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 2048MB \
    --timeout 600s


echo "======================================================"
echo "Deploying the SARIMA Model Hyperparameter Tuning Function"
echo "======================================================"

gcloud functions deploy sarima_hyperparameter_tuning \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point sarima_hyperparameter_tuning \
    --source ./functions/hyperparameter-tuning \
    --stage-bucket ba882-cloud-functions-stage \
    --service-account etl-pipeline@ba882-group-10.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 5120MB \
    --timeout 750s

echo "======================================================"
echo "Deployment Complete"
echo "======================================================"
