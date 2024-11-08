"""
Cloud Function to train SARIMA models on CDC disease occurrences data.
1. Loops through each disease code in the `training-data` folder on GCS.
2. Retrieves data for each disease from GCS.
3. Splits data into training and testing sets based on date.
4. Fits a SARIMA model and evaluates it.
5. Stores each model in GCS and logs metadata, metrics, and parameters to BigQuery.

BigQuery Dataset: cdc_data
GCS Bucket: ba882-group-10-mlops
"""

# Imports
import functions_framework
import pandas as pd
import joblib
import uuid
import datetime
import json
import re
import os

from google.cloud import storage, bigquery
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from gcsfs import GCSFileSystem

# Settings
project_id = 'ba882-group-10'
bucket_name = 'ba882-group-10-mlops'
dataset_id = 'cdc_data'
model_storage_path = 'pipeline'

# Default SARIMA hyperparameters
DEFAULT_ORDER = (2, 1, 2)
DEFAULT_SEASONAL_ORDER = (1, 1, 1, 12)  # Monthly seasonality

@functions_framework.http
def train_sarima_models(request):
    """Train SARIMA models for each disease code in the training data."""

    # Initialize clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client(project=project_id)

    # List all folders under 'training-data' in the GCS bucket
    blobs = storage_client.list_blobs(bucket_name, prefix="training-data/")
    disease_codes = set(re.match(r'training-data/code-(\d+)', blob.name).group(1)
                        for blob in blobs if re.match(r'training-data/code-(\d+)', blob.name))

    results = []
    
    # Process each disease code
    for disease_code in disease_codes:
        csv_path = f"gs://{bucket_name}/training-data/code-{disease_code}/cdc_occurrences_{disease_code}.csv"
        try:
            # Load data
            df = pd.read_csv(csv_path, parse_dates=['Date'])
            df = df.sort_values(by="Date")

            print(f"Columns in CSV: {df.columns}")
            
            # Prepare the data
            train_data, test_data = split_train_test(df, 'Date', 'Total_Occurrences', test_months=3)

            # Train SARIMA model
            sarima_model = SARIMAX(train_data, order=DEFAULT_ORDER, seasonal_order=DEFAULT_SEASONAL_ORDER)
            model_fit = sarima_model.fit(disp=False)

            # Generate predictions on the test set
            predictions = model_fit.predict(start=len(train_data), end=len(train_data) + len(test_data) - 1)

            # Calculate metrics
            r2 = r2_score(test_data, predictions)
            mae = mean_absolute_error(test_data, predictions)
            mse = mean_squared_error(test_data, predictions)

            # Generate unique model ID
            model_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())
            last_training_date = pd.to_datetime(train_data.index[-1]).strftime("%Y-%m-%d")

            # Save model and metadata to GCS
            model_gcs_path = f"{model_storage_path}/model_for_{disease_code}/{model_id}.joblib"
            metadata_gcs_path = f"{model_storage_path}/model_for_{disease_code}/{model_id}_metadata.json"
            save_model_and_metadata_to_gcs(model_fit, bucket_name, model_gcs_path, metadata_gcs_path, last_training_date)

            # Log metadata, metrics, and parameters to BigQuery
            log_model_metadata(bigquery_client, model_id, disease_code, model_gcs_path, r2, mae, mse)
            log_model_parameters(bigquery_client, model_id, DEFAULT_ORDER, DEFAULT_SEASONAL_ORDER)

            # Append result for this disease code
            results.append({
                "model_id": model_id,
                "disease_code": disease_code,
                "r2": r2,
                "mae": mae,
                "mse": mse,
                "model_path": f"gs://{bucket_name}/{model_gcs_path}"
            })

        except Exception as e:
            print(f"Error processing disease code {disease_code}: {e}")

    return {"results": results}, 200

def split_train_test(df, date_column, target_column, test_months=3):
    """Splits a time series DataFrame into train and test sets."""
    latest_date = df[date_column].max()
    cutoff_date = latest_date - pd.DateOffset(months=test_months)
    
    train_data = df[df[date_column] <= cutoff_date][target_column]
    test_data = df[df[date_column] > cutoff_date][target_column]
    return train_data, test_data

def save_model_and_metadata_to_gcs(model, bucket_name, model_path, metadata_path, last_training_date):
    """
    Saves the trained model and metadata to GCS.

    The model is saved as a .joblib file, and an additional metadata JSON file is created
    to store the `last_training_date`. This metadata file allows future prediction functions
    to know the end date of the training data, ensuring predictions start from the correct date.
    
    Parameters:
    - model: The trained SARIMA model.
    - bucket_name: GCS bucket where the files will be stored.
    - model_path: Path in GCS to save the model file.
    - metadata_path: Path in GCS to save the metadata file.
    - last_training_date: The last date in the training data, used to determine the prediction start date.
    """
    # Initialize GCSFileSystem for saving files to GCS
    gcs = GCSFileSystem()
    
    # Save the model
    full_model_path = f"gs://{bucket_name}/{model_path}"
    with gcs.open(full_model_path, 'wb') as f:
        joblib.dump(model, f)
    print(f"Model successfully uploaded to GCS at {full_model_path}")
    
    # Save metadata with last training date
    metadata = {
        "last_training_date": last_training_date,
        "model_id": model_path.split('/')[-1].replace(".joblib", ""),
        "disease_code": model_path.split('/')[-2].split("_")[-1]
    }
    full_metadata_path = f"gs://{bucket_name}/{metadata_path}"
    with gcs.open(full_metadata_path, 'w') as f:
        json.dump(metadata, f)
    print(f"Metadata successfully uploaded to GCS at {full_metadata_path}")

def log_model_metadata(client, model_id, disease_code, model_path, r2, mae, mse):
    """Logs model metadata and metrics to BigQuery."""
    table_id = f"{project_id}.{dataset_id}.model_runs"
    rows_to_insert = [{
        "model_id": model_id,
        "name": "SARIMA Model",
        "gcs_path": f"gs://{bucket_name}/{model_storage_path}",
        "model_path": f"gs://{bucket_name}/{model_path}",
        "disease_code": disease_code,
        "created_at": datetime.datetime.now().isoformat()  # Convert to ISO format for JSON
    }]
    client.insert_rows_json(table_id, rows_to_insert)

    # Log metrics
    metrics_table_id = f"{project_id}.{dataset_id}.model_metrics"
    metrics = [
        {"model_id": model_id, "metric_name": "r2", "metric_value": r2},
        {"model_id": model_id, "metric_name": "mae", "metric_value": mae},
        {"model_id": model_id, "metric_name": "mse", "metric_value": mse}
    ]
    client.insert_rows_json(metrics_table_id, metrics)

def log_model_parameters(client, model_id, order, seasonal_order):
    """Logs model parameters to BigQuery."""
    parameters_table_id = f"{project_id}.{dataset_id}.model_parameters"
    parameters = [
        {"model_id": model_id, "parameter_name": "order", "parameter_value": str(order)},
        {"model_id": model_id, "parameter_name": "seasonal_order", "parameter_value": str(seasonal_order)}
    ]
    client.insert_rows_json(parameters_table_id, parameters)




