import pandas as pd
import pyarrow  # Import pyarrow to ensure Parquet file support
from google.cloud import bigquery
from google.cloud import storage
import functions_framework
import io

# Google Cloud project and bucket/table information
project_id = 'ba882-group-10'
bucket_name = 'cdc-extract-dataframe'  # Parquet file storage bucket
output_blob_name = 'cdc_data.parquet'  # Parquet file name
dataset_id = 'cdc_data'  # Your BigQuery dataset
table_id = 'disease_occurrences'  # Your BigQuery table

@functions_framework.http
def load_to_bigquery(request):
    # Initialize BigQuery and GCS clients
    bigquery_client = bigquery.Client(project=project_id)
    storage_client = storage.Client(project=project_id)
    
    # Get the bucket and blob (parquet file)
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
    
    return "Data successfully loaded into BigQuery."
