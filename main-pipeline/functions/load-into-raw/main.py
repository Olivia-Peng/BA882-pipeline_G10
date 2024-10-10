import pandas as pd
import pyarrow  # Import pyarrow to ensure Parquet file support
from google.cloud import bigquery
from google.cloud import storage
import functions_framework
import io
import json

# Google Cloud project and bucket/table information
project_id = 'ba882-group-10'
bucket_name = 'cdc-extract-dataframe'  # Parquet file storage bucket
dataset_id = 'cdc_data'  # Your BigQuery dataset
table_id = 'cdc_occurrences_raw'  # Your BigQuery raw table

@functions_framework.http
def load_to_bigquery(request):
    # Parse request to get job_id
    request_json = request.get_json()
    job_id = request_json.get('job_id') if request_json else None
    if not job_id:
        return "Missing job_id in request payload", 400

    # Initialize BigQuery and GCS clients
    bigquery_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)
    
    # Get the bucket and blob (parquet file)
    output_blob_name = f'cdc_data_{job_id}.parquet'
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(output_blob_name)
    
    # Download the parquet file into memory
    parquet_data = io.BytesIO(blob.download_as_bytes())
    
    # Read the parquet data into a DataFrame
    df = pd.read_parquet(parquet_data)
    
    # Define the BigQuery table to write to
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    
    # Write the DataFrame to BigQuery (append the data)
    job = bigquery_client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait for the job to complete
    
    print(f"Loaded {len(df)} rows into BigQuery table {table_id}.")
    
    return {}, 200
