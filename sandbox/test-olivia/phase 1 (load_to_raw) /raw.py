import pandas as pd
import pyarrow  # Import pyarrow to ensure Parquet file support
from google.cloud import bigquery
from google.cloud import storage
import functions_framework
import io
import json

# Google Cloud project and bucket/table information
project_id = 'ba882-pipeline-olivia'
bucket_name = 'ba882_olivia'  # Parquet file storage bucket
dataset_id = 'CDC'  # Your BigQuery dataset
table_name = 'raw'  # Your BigQuery raw table

@functions_framework.http
def load_to_bigquery(request):
    # Parse request to get job_id
    request_json = request.get_json()
    job_id = request_json.get('job_id') if request_json else None
    if not job_id:
        return "Missing job_id in request payload", 400

    try:
        # Initialize BigQuery and GCS clients
        bigquery_client = bigquery.Client(project=project_id)
        storage_client = storage.Client(project=project_id)
        
        # Adjust file naming to match what's in your GCS bucket
        output_blob_name = f'parse_to_parquet/{job_id}.parquet'  # Update to include folder path
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(output_blob_name)

        # Verify if the file exists
        if not blob.exists():
            return f"Parquet file {output_blob_name} not found.", 404

        # Download the parquet file into memory
        parquet_data = io.BytesIO(blob.download_as_bytes())

        # Read the parquet data into a DataFrame
        df = pd.read_parquet(parquet_data)

        # Convert `Date` column to datetime format
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        
        # Define the BigQuery table to write to
        table_ref = bigquery_client.dataset(dataset_id).table(table_name)

        # Write the DataFrame to BigQuery (append the data)
        job = bigquery_client.load_table_from_dataframe(df, table_ref)
        job.result()  # Wait for the job to complete

        print(f"Loaded {len(df)} rows into BigQuery table {table_name}.")
        return json.dumps({"message": f"Loaded {len(df)} rows into BigQuery table {table_name}.", "job_id": job_id}), 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return json.dumps({"error": str(e)}), 500
