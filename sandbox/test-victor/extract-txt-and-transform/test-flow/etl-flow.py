from prefect import flow, task
import requests

# Task to call the CDC data extraction cloud function
@task
def extract():
    cloud_function_url = "https://download-cdc-data-162771833878.us-central1.run.app"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        print("Cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute cloud function. Status code: {response.status_code}, Response: {response.text}")

# Task to call the BigQuery schema creation cloud function
@task(retries=3, retry_delay_seconds=60)
def create_schema():
    cloud_function_url = "https://create-schema-162771833878.us-central1.run.app"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        print("Schema creation cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute schema creation cloud function. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Schema creation failed")

# Task to call the transformation and upload to BigQuery cloud function
@task(retries=3, retry_delay_seconds=60)
def transform_txt_to_dataframe():
    cloud_function_url = "https://transform-txt-to-dataframe-162771833878.us-central1.run.app"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        print("Upload cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute upload cloud function. Status code: {response.status_code}, Response: {response.text}")
        raise Exception("Data upload failed")

# Define the combined flow
@flow
def combined_pipeline_flow():
    # Step 1: Extract CDC data
    extract_result = extract()

    # Step 2: Create BigQuery Schema
    schema_result = create_schema(wait_for=[extract_result])

    # Step 3: Transform and upload text data to BigQuery
    transform_txt_to_dataframe(wait_for=[schema_result])

# Run the Prefect flow
if __name__ == "__main__":
    combined_pipeline_flow()
