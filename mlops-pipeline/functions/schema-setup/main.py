"""
Cloud Function to create BigQuery tables for model runs, metrics, and parameters in the `cdc_data` dataset.
These tables will store information for each model, including metadata, metrics, and hyperparameters.
The function checks if each table exists before creating it to prevent duplicate creation.

Tables created:
1. model_runs: Stores model metadata, including model_id, name, GCS path, model path, disease code, and timestamp.
2. model_metrics: Stores model evaluation metrics such as MSE, MAE, with their values and model_id.
3. model_parameters: Stores model hyperparameters used in training, along with model_id and parameter values.

BigQuery Dataset: cdc_data
"""

# Imports
import functions_framework
from google.cloud import bigquery

# Settings
project_id = 'ba882-group-10'
dataset_id = 'cdc_data'

# Cloud Function to create the required tables
@functions_framework.http
def create_schema(request):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the tables and their schemas
    tables = {
        "model_runs": [
            bigquery.SchemaField("model_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("gcs_path", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("model_path", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("disease_code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE", default_value_expression="CURRENT_TIMESTAMP")
        ],
        "model_metrics": [
            bigquery.SchemaField("model_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("metric_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("metric_value", "FLOAT", mode="NULLABLE")
        ],
        "model_parameters": [
            bigquery.SchemaField("model_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("parameter_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("parameter_value", "STRING", mode="NULLABLE")
        ]
    }

    # Create the dataset if it does not exist
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        print(f"Dataset {dataset_id} does not exist. Creating it...")
        client.create_dataset(bigquery.Dataset(dataset_ref))

    # Loop through tables and create them if they don't exist
    for table_name, schema in tables.items():
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.get_table(table_id)  # Check if table exists
            print(f"Table {table_name} already exists.")
        except Exception:
            print(f"Table {table_name} does not exist. Creating it...")
            client.create_table(table)
            print(f"Table {table_name} created successfully.")

    return {"status": "Tables created or verified successfully."}, 200

