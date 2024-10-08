# This script is designed to be run as a Google Cloud Function. It downloads disease data in .txt format from the CDC website for the years 2023 and 2024.
# It iterates over specified disease tables and weeks for each year to download the corresponding data files.
# The downloaded files are saved to a Google Cloud Storage bucket.
# If a file cannot be downloaded, an error message is printed with the status code.

import requests
from google.cloud import storage
import os
import functions_framework

# Define project and bucket information
project_id = 'ba882-group-10'
bucket_name = 'cdc-extract-txt'

# Define the function to download the .txt files and save them to GCP bucket
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

# Google Cloud Function entry point
@functions_framework.http
def task(request):
    # Parameters for the function
    years = [2024]
    weeks_range = range(1, 53)  # 52 weeks
    disease_tables = [10, 60]

    # Loop over the years, weeks, and disease tables to download the files
    for year in years:
        for week in weeks_range:
            for disease_table in disease_tables:
                download_txt_file(year, week, disease_table, bucket_name)

    return "CDC data download completed."