from prefect import flow, task
import requests
import json

# Helper function - Generic GCF invoker
def invoke_gcf(url: str, payload: dict = None):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

# Task to call the CDC data extraction cloud function
@task(retries=2, retry_delay_seconds=60)
def extract():
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/download-cdc-data"
    resp = invoke_gcf(cloud_function_url, payload={})
    job_id = resp.get("job_id")
    if not job_id:
        raise ValueError("Failed to obtain job_id from extraction function")
    return job_id

# Task to call the BigQuery schema creation cloud function
@task(retries=2, retry_delay_seconds=60)
def create_schema():
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/create-schema"
    invoke_gcf(cloud_function_url, payload={})
    print("Schema creation cloud function executed successfully.")

# Task to call the transformation and saves the dataframe as a parquet file
@task(retries=2, retry_delay_seconds=60)
def transform_txt_to_dataframe(job_id):
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/transform_txt_to_dataframe"
    invoke_gcf(cloud_function_url, payload={"job_id": job_id})
    print("Transformation cloud function executed successfully.")

# Task to call the new function that loads Parquet data into BigQuery in the raw table version
@task(retries=2, retry_delay_seconds=60)
def load_to_bigquery(job_id):
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/load-to-bigquery"
    invoke_gcf(cloud_function_url, payload={"job_id": job_id})
    print("Data loaded to BigQuery successfully.")

# Define the combined flow
@flow
def combined_pipeline_flow():
    # Step 1: Extract CDC data
    job_id = extract()

    # Step 2: Create BigQuery Schema
    create_schema()

    # Step 3: Transform and upload text data to GCS
    transform_txt_to_dataframe(job_id)

    # Step 4: Load Parquet data into BigQuery
    load_to_bigquery(job_id)

# Run the Prefect flow
if __name__ == "__main__":
    combined_pipeline_flow()
