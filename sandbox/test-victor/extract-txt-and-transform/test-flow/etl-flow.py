from prefect import flow, task
import requests
import json

# Task to call the CDC data extraction cloud function
@task
def extract():
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/download-cdc-data"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        response_data = response.json()
        job_id = response_data.get("job_id")
        if job_id:
            print("Cloud function executed successfully: ", response_data)
            return job_id
        else:
            raise ValueError("Job ID missing in cloud function response")
    else:
        print(f"Failed to execute cloud function. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Extraction failed")

# Task to call the BigQuery schema creation cloud function
@task(retries=2, retry_delay_seconds=60)
def create_schema():
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/create-schema"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        print("Schema creation cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute schema creation cloud function. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Schema creation failed")

# Task to call the transformation and saves the dataframe as a parquet file
@task(retries=2, retry_delay_seconds=60)
def transform_txt_to_dataframe(job_id):
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/transform_txt_to_dataframe"
    payload = json.dumps({"job_id": job_id})
    headers = {"Content-Type": "application/json"}
    response = requests.post(cloud_function_url, data=payload, headers=headers)
    if response.status_code == 200:
        print("Transformation cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute transformation cloud function. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Data transformation failed")

# Task to call the new function that loads Parquet data into BigQuery in the raw table version
@task(retries=2, retry_delay_seconds=60)
def load_to_bigquery(job_id):
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/load-to-bigquery"
    payload = json.dumps({"job_id": job_id})
    headers = {"Content-Type": "application/json"}
    response = requests.post(cloud_function_url, data=payload, headers=headers)
    if response.status_code == 200:
        print("Data loaded to BigQuery successfully: ", response.text)
    else:
        print(f"Failed to load data into BigQuery. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Data load to BigQuery failed")

# Define the combined flow
@flow
def combined_pipeline_flow():
    # Step 1: Extract CDC data
    job_id = extract()

    # Step 2: Create BigQuery Schema
    create_schema()

    # Step 3: Transform and upload text data to BigQuery
    transform_txt_to_dataframe(job_id)

    # Step 4: Load Parquet data into BigQuery
    load_to_bigquery(job_id)

# Run the Prefect flow
if __name__ == "__main__":
    combined_pipeline_flow()