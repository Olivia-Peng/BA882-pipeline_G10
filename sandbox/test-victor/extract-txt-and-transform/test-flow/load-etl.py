from prefect import flow, task
import requests

# Define the Prefect task to call the BigQuery schema creation cloud function
@task(retries=3, retry_delay_seconds=60)
def create_schema():
    cloud_function_url = "https://create-schema-162771833878.us-central1.run.app"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        print("Schema creation cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute schema creation cloud function. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Schema creation failed")

# Define the Prefect task to call the BigQuery upload cloud function
@task(retries=3, retry_delay_seconds=60)
def upload_txt_to_bigquery():
    cloud_function_url = "https://upload-txt-to-bigquery-162771833878.us-central1.run.app"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        print("Upload cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute upload cloud function. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Data upload failed")

# Define the Prefect flow
@flow
def bigquery_pipeline_flow():
    # Step 1: Create BigQuery Schema
    schema_result = create_schema()

    # Step 2: Upload text files to BigQuery
    upload_txt_to_bigquery(wait_for=[schema_result])

# Run the Prefect flow
if __name__ == "__main__":
    bigquery_pipeline_flow()
