import requests
from google.cloud import storage
import os
import functions_framework
from retrying import retry
from datetime import datetime
import uuid
import json

# Define project and bucket information
project_id = 'ba882-pipeline-olivia'
bucket_name = 'ba882_olivia'  # Only the bucket name, no folder name here

@retry(stop_max_attempt_number=2, wait_fixed=1000)  
def download_txt_file(year, week, disease_table, bucket_name, job_id):
    url = f'https://wonder.cdc.gov/nndss/static/{year}/{week:02d}/{year}-{week:02d}-table{disease_table:02d}.txt'
    response = requests.get(url)

    if response.status_code == 200:  # Successful request
        # Initialize Google Cloud Storage client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.get_bucket(bucket_name)

        # Define the filename and blob path
        filename = f'{year}_week{week:02d}_table{disease_table:02d}.txt'
        # Include the specific folder in the blob path
        blob_path = f'extract_to_txt/{job_id}/{filename}'
        blob = bucket.blob(blob_path)

        # Upload the content to the bucket
        blob.upload_from_string(response.content)
        print(f"Successfully uploaded: {filename} to bucket {bucket_name}/{blob_path}")
    elif response.status_code == 404:
        print(f"File not found for Year {year}, Week {week}, Table {disease_table} (Status Code: 404)")
    else:
        print(f"Failed to download Year {year}, Week {week}, Table {disease_table} (Status Code: {response.status_code})")
        print(f"Download failed for Year {year}, Week {week}, Table {disease_table} (Status Code: {response.status_code})")

# Google Cloud Function entry point
@functions_framework.http
def task(request):
    # Generate a unique job ID
    job_id = datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + str(uuid.uuid4())

    # Parameters for the function
    years = [2023, 2024]
    current_week = datetime.now().isocalendar()[1] - 1
    disease_tables = [370,560,350,1122,392,1140,550,1130] # 8 diseases

    for disease_table in disease_tables:
        for year in years:
            if year == 2024:
                weeks_range = range(1, current_week + 1)
            else:
                weeks_range = range(1, 53)

            for week in weeks_range:
                try:
                    print(f"Attempting download for Year {year}, Week {week}, Table {disease_table}")
                    download_txt_file(year, week, disease_table, bucket_name, job_id)
                except requests.exceptions.RequestException as e:
                    # Log network-related errors and continue
                    print(f"Network error while downloading file for Year {year}, Week {week}, Table {disease_table}: {e}")
                    pass  # Continue with the next iteration
                except Exception as e:
                    # Log all other errors and continue
                    print(f"Error downloading file for Year {year}, Week {week}, Table {disease_table}: {e}")
                    pass  # Continue with the next iteration

    return {"job_id": job_id}, 200
