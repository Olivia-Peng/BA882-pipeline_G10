"""
Cloud Function to generate predictions for each trained SARIMA model in GCS.
1. Lists all models in the GCS path `ba882-group-10-mlops/pipeline/model_for_{disease_code}/`.
2. Loads each model and retrieves its last training date from the metadata file.
3. Generates predictions for the next 8 weeks.
4. Stores predictions in the BigQuery `predictions` table.

BigQuery Dataset: cdc_data
GCS Bucket: ba882-group-10-mlops
"""

import functions_framework
import joblib
import datetime
import json
import re
import tempfile
from google.cloud import storage, bigquery

# Settings
project_id = 'ba882-group-10'
bucket_name = 'ba882-group-10-mlops'
dataset_id = 'cdc_data'
model_storage_path = 'pipeline'

# Initialize BigQuery client
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client()

@functions_framework.http
def predict_with_latest_models(request):
    """Predicts for the next 8 weeks using the latest SARIMA model for each disease code."""

    # Get list of all models in GCS
    blobs = storage_client.list_blobs(bucket_name, prefix=model_storage_path)
    disease_models = {}

    # Identify each disease model path in the format: pipeline/model_for_{disease_code}/model_id.joblib
    for blob in blobs:
        match = re.match(rf'{model_storage_path}/model_for_(\d+)/(.+)\.joblib', blob.name)
        if match:
            disease_code = match.group(1)
            model_id = match.group(2)
            # Store the latest model for each disease_code (assuming latest model has the highest datetime in its model_id)
            if disease_code not in disease_models or model_id > disease_models[disease_code][1]:
                disease_models[disease_code] = (blob.name, model_id)

    results = []

    # Process each disease model
    for disease_code, (model_path, model_id) in disease_models.items():
        # Load the model
        model = load_model_from_gcs(bucket_name, model_path)

        # Load metadata (last training date)
        metadata_path = model_path.replace(".joblib", "_metadata.json")
        metadata = load_metadata_from_gcs(bucket_name, metadata_path)
        last_training_date = metadata.get("last_training_date")
        
        if not last_training_date:
            print(f"No last_training_date found for disease code {disease_code}. Skipping...")
            continue

        # Parse the last training date to start predictions
        last_date = datetime.datetime.strptime(last_training_date, '%Y-%m-%d')
        
        # Generate next 8 weeks of dates
        future_dates = [last_date + datetime.timedelta(weeks=i) for i in range(1, 9)]
        
        # Generate predictions
        predictions = model.predict(start=len(model.data.endog), end=len(model.data.endog) + 7)

        # Log predictions to BigQuery
        log_predictions_to_bq(bigquery_client, disease_code, model_id, future_dates, predictions)

        # Append results for logging
        results.append({
            "disease_code": disease_code,
            "model_id": model_id,
            "predictions": [
                {"date": date.strftime('%Y-%m-%d'), "predicted_occurrence": pred}
                for date, pred in zip(future_dates, predictions)
            ]
        })

    return {"results": results}, 200

def load_model_from_gcs(bucket_name, model_path):
    """Loads a SARIMA model from GCS using google-cloud-storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(model_path)
    
    # Use a temporary file to download and load the model
    with tempfile.NamedTemporaryFile() as temp_file:
        blob.download_to_filename(temp_file.name)
        model = joblib.load(temp_file.name)
    
    return model

def load_metadata_from_gcs(bucket_name, metadata_path):
    """Loads model metadata from a JSON file in GCS using google-cloud-storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(metadata_path)
    
    # Download the JSON metadata and parse it
    metadata_json = blob.download_as_text()
    metadata = json.loads(metadata_json)
    
    return metadata

def log_predictions_to_bq(client, disease_code, model_id, future_dates, predictions):
    """Logs predictions to BigQuery predictions table with an additional Disease field."""
    table_id = f"{project_id}.{dataset_id}.predictions"
    # Truncate inference_date to nearest hour
    inference_date = datetime.datetime.now().replace(minute=0, second=0, microsecond=0).isoformat()
    
    rows_to_insert = [{
        "model_id": model_id,
        "inference_date": inference_date,  # Use truncated timestamp
        "date": date.strftime('%Y-%m-%d'),
        "predicted_occurrence": prediction,
        "Disease": disease_code  # Add disease code as 'Disease' field
    } for date, prediction in zip(future_dates, predictions)]
    
    client.insert_rows_json(table_id, rows_to_insert)
    print(f"Predictions logged to BigQuery for disease code {disease_code}")

