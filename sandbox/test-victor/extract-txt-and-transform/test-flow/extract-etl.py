from prefect import flow, task
import requests

# Define the Prefect task to call the cloud function
@task
def extract():
    cloud_function_url = "https://download-cdc-data-162771833878.us-central1.run.app"
    response = requests.post(cloud_function_url)
    if response.status_code == 200:
        print("Cloud function executed successfully: ", response.text)
    else:
        print(f"Failed to execute cloud function. Status code: {response.status_code}, Response: {response.text}")

# Define the Prefect flow
@flow
def cdc_data_extraction_flow():
    extract()

# Run the Prefect flow
if __name__ == "__main__":
    cdc_data_extraction_flow()