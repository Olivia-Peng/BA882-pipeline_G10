from prefect import flow, task
import requests

# Task to call the schema creation cloud function
@task(retries=2, retry_delay_seconds=60)
def create_schema():
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/create-symptom-schema"
    response = requests.post(cloud_function_url)
    response.raise_for_status()
    print(f"Schema creation cloud function executed successfully. Response: {response.text}")

# Task to call the cloud function that populates the symptom table
@task(retries=2, retry_delay_seconds=60)
def populate_symptom_table():
    cloud_function_url = "https://us-central1-ba882-group-10.cloudfunctions.net/populate-symptoms-table"
    response = requests.post(cloud_function_url)
    response.raise_for_status()
    print(f"Symptom table population cloud function executed successfully. Response: {response.text}")

# Define the Prefect flow to create and populate the BigQuery table
@flow
def create_and_populate_symptoms_table_flow():
    # Step 1: Create BigQuery Schema
    schema_task = create_schema()

    # Step 2: Populate BigQuery table with predefined data (waits for schema creation)
    populate_symptom_table(wait_for=[schema_task])

# Run the Prefect flow
if __name__ == "__main__":
    create_and_populate_symptoms_table_flow()

