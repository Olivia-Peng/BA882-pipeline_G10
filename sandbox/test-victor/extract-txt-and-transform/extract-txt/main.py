# This script is designed to be run as a Google Cloud Function. It downloads disease data in .txt format from the CDC website for the years 2023 and 2024.
# It iterates over specified disease tables and weeks for each year to download the corresponding data files.
# The downloaded files are saved to a Google Cloud Storage bucket.
# If a file cannot be downloaded, an error message is printed with the status code.

import requests
from google.cloud import storage
import os
import functions_framework
from retrying import retry
from datetime import datetime

# Define project and bucket information
project_id = 'ba882-group-10'
bucket_name = 'cdc-extract-txt'

@retry(stop_max_attempt_number=3, wait_fixed=2000)  # Retry up to 3 times, wait 2 seconds between retries
def download_txt_file(year, week, disease_table, bucket_name):
    url = f'https://wonder.cdc.gov/nndss/static/{year}/{week:02d}/{year}-{week:02d}-table{disease_table:02d}.txt'
    response = requests.get(url)

    if response.status_code == 200:  # Successful request
        # Initialize Google Cloud Storage client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.get_bucket(bucket_name)

        # Define the filename and blob path
        filename = f'{year}_week{week:02d}_table{disease_table:02d}.txt'
        blob = bucket.blob(filename)

        # Upload the content to the bucket
        blob.upload_from_string(response.content)
        print(f"Successfully uploaded: {filename} to bucket {bucket_name}")
    else:
        print(f"Failed to download Year {year}, Week {week}, Table {disease_table} (Status Code: {response.status_code})")
        raise Exception(f"Download failed for Year {year}, Week {week}, Table {disease_table}")

# Google Cloud Function entry point
@functions_framework.http
def task(request):
    # Respond immediately to acknowledge the request
    from concurrent.futures import ThreadPoolExecutor

    def process():
        # Parameters for the function
        years = [2023, 2024]
        current_week = datetime.now().isocalendar()[1] - 1  # Get the previous week of the current year
        disease_tables = [10, 60]

        # Loop over disease tables first, then iterate over years and weeks to download the files
        for disease_table in disease_tables:
            for year in years:
                if year == 2024:
                    weeks_range = range(1, current_week + 1)  # Up to the current week for 2024
                else:
                    weeks_range = range(1, 53)  # All weeks for 2023

                for week in weeks_range:
                    download_txt_file(year, week, disease_table, bucket_name)

    # Run the processing in a separate thread so the function can respond immediately
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(process)

    return "CDC data download task initiated."

