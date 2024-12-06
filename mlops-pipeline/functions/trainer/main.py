"""
Cloud Function to train SARIMA models on CDC disease occurrences data.

Updates:
1. Retrieves the best hyperparameters for each disease code from GCS (`tunning_results/{disease_code}/{disease_code}_params.json`).
2. Skips training for disease codes that do not have a corresponding best parameters JSON file.
3. Trains a SARIMA model for each disease code using the retrieved best parameters.
4. Splits the data into training and testing sets based on the date.
5. Evaluates the trained SARIMA model using R2, MAE, and MSE metrics.
6. Stores the trained SARIMA model and metadata in GCS.
7. Logs metadata, metrics, and hyperparameters for each trained model into BigQuery.

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
from google.cloud import storage, bigquery
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from gcsfs import GCSFileSystem

# Settings
project_id = 'ba882-group-10'
bucket_name = 'ba882-group-10-mlops'
dataset_id = 'cdc_data'
model_storage_path = 'pipeline'
best_params_path = 'tunning_results'  # Path to JSON files with best parameters

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

            # Load best parameters from GCS
            best_params = get_best_params_from_gcs(bucket_name, disease_code)
            if not best_params:
                print(f"No best parameters found for disease code {disease_code}. Skipping...")
                continue

            # Prepare the data
            train_data, test_data = split_train_test(df, 'Date', 'Total_Occurrences', test_months=3)

            # Train SARIMA model
            sarima_model = SARIMAX(
                train_data,
                order=(best_params['p'], best_params['d'], best_params['q']),
                seasonal_order=(best_params['P'], best_params['D'], best_params['Q'], best_params['s']),
            )
            model_fit = sarima_model.fit(disp=False)

            # Generate predictions on the test set
            predictions = model_fit.predict(start=len(train_data), end=len(train_data) + len(test_data) - 1)

            # Calculate metrics
            r2 = r2_score(test_data, predictions)
            mae = mean_absolute_error(test_data, predictions)
            mse = mean_squared_error(test_data, predictions)

            # Generate unique model ID
            model_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())
            last_training_date = df['Date'].max().strftime("%Y-%m-%d")

            # Save model and metadata to GCS
            model_gcs_path = f"{model_storage_path}/model_for_{disease_code}/{model_id}.joblib"
            metadata_gcs_path = f"{model_storage_path}/model_for_{disease_code}/{model_id}_metadata.json"
            save_model_and_metadata_to_gcs(model_fit, bucket_name, model_gcs_path, metadata_gcs_path, last_training_date)

            # Log metadata, metrics, and parameters to BigQuery
            log_model_metadata(bigquery_client, model_id, disease_code, model_gcs_path, r2, mae, mse)
            log_model_parameters(bigquery_client, model_id, (best_params['p'], best_params['d'], best_params['q']),
                                 (best_params['P'], best_params['D'], best_params['Q'], best_params['s']))

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

def get_best_params_from_gcs(bucket_name, disease_code):
    """Fetches the best parameters for a given disease code from GCS."""
    storage_client = storage.Client()
    file_path = f"{best_params_path}/{disease_code}/{disease_code}_params.json"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    if blob.exists():
        params_json = blob.download_as_text()
        best_params = json.loads(params_json).get('best_params')
        return best_params
    else:
        print(f"Best parameters file not found for disease code {disease_code}.")
        return None

def split_train_test(df, date_column, target_column, test_months=3):
    """Splits a time series DataFrame into train and test sets."""
    latest_date = df[date_column].max()
    cutoff_date = latest_date - pd.DateOffset(months=test_months)
    
    train_data = df[df[date_column] <= cutoff_date][target_column]
    test_data = df[df[date_column] > cutoff_date][target_column]
    return train_data, test_data

def save_model_and_metadata_to_gcs(model, bucket_name, model_path, metadata_path, last_training_date):
    """Saves the trained model and metadata to GCS."""
    gcs = GCSFileSystem()
    full_model_path = f"gs://{bucket_name}/{model_path}"
    with gcs.open(full_model_path, 'wb') as f:
        joblib.dump(model, f)
    metadata = {
        "last_training_date": last_training_date,
        "model_id": model_path.split('/')[-1].replace(".joblib", ""),
        "disease_code": model_path.split('/')[-2].split("_")[-1]
    }
    full_metadata_path = f"gs://{bucket_name}/{metadata_path}"
    with gcs.open(full_metadata_path, 'w') as f:
        json.dump(metadata, f)

def log_model_metadata(client, model_id, disease_code, model_path, r2, mae, mse):
    """Logs model metadata and metrics to BigQuery."""
    table_id = f"{project_id}.{dataset_id}.model_runs"
    rows_to_insert = [{
        "model_id": model_id,
        "name": "SARIMA Model",
        "gcs_path": f"gs://{bucket_name}/{model_storage_path}",
        "model_path": f"gs://{bucket_name}/{model_path}",
        "disease_code": disease_code,
        "created_at": datetime.datetime.now().isoformat()
    }]
    client.insert_rows_json(table_id, rows_to_insert)
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





