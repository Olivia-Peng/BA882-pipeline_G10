# This script is designed to be run as a Google Cloud Function. It transforms disease data in .txt format stored in a Google Cloud Storage bucket into a structured DataFrame.
# The script extracts relevant information from the text files, filters out specific regions, and stores the processed data as a Parquet file in another Google Cloud Storage bucket.
# The script performs the following tasks:
# 1. Parses the request payload to retrieve a job ID.
# 2. Lists all text files in the specified Google Cloud Storage bucket for the given job ID.
# 3. Defines a set of regions to exclude from processing.
# 4. Extracts the date information from a specific line in the text file using a regular expression.
# 5. Processes each text file to extract disease name, region, and occurrence count, skipping lines for excluded regions.
# 6. Converts the collected data into a Pandas DataFrame.
# 7. Stores the DataFrame as a Parquet file in the output Google Cloud Storage bucket.

import pandas as pd
from google.cloud import storage
import functions_framework
import pyarrow
import io
import json
import re
from datetime import datetime

# Google Cloud Function entry point
@functions_framework.http
def transform_txt_to_dataframe(request):
    # Parse request to get job_id
    request_json = request.get_json()
    job_id = request_json.get('job_id') if request_json else None
    if not job_id:
        return "Missing job_id in request payload", 400

    # Define project and bucket information
    project_id = 'ba882-group-10'
    bucket_name = 'cdc-extract-txt'
    output_bucket_name = 'cdc-extract-dataframe'
    output_blob_name = f'cdc_data_{job_id}.parquet'

    # Initialize Google Cloud Storage client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # List all text files in the bucket for the given job_id
    blobs = list(bucket.list_blobs(prefix=f"{job_id}/"))
    txt_blobs = [blob for blob in blobs if blob.name.endswith('.txt')]

    # Collect data from all files
    all_data = []

    # Define regions to exclude
    excluded_regions = {
        'U.S. Residents, excluding U.S. Territories',
        'New England',
        'Middle Atlantic',
        'East North Central',
        'West North Central',
        'South Atlantic',
        'East South Central',
        'West South Central',
        'Mountain',
        'Pacific',
        'U.S. Territories',
        'Non-U.S. Residents',
        'Total'
    }

    # Helper function to extract date from a specific line
    def extract_date_from_line(line):
        # Define the regular expression pattern
        pattern = r"Non-U\.S\. Residents week ending (\w+ \d{1,2}, \d{4})"
        match = re.search(pattern, line)

        if match:
            date_str = match.group(1)  # Extract the date part
            # Convert the extracted date string to a standardized date format
            extracted_date = datetime.strptime(date_str, "%B %d, %Y").strftime('%Y-%m-%d')
            return extracted_date
        else:
            return None

    # Process each text file
    for blob in txt_blobs:
        print(f"Processing file: {blob.name}")

        # Download file content
        file_content = blob.download_as_text(encoding='ISO-8859-1')
        lines = file_content.splitlines()

        # Extract relevant information from the text
        disease_name = lines[4].split(";")[0].strip()  # Extracting disease name, e.g., Cryptosporidiosis

        # Extract the date from the specific line containing "Non-U.S. Residents week ending"
        date_line = lines[0]  # Assuming the date is in the first line
        Date = extract_date_from_line(date_line)
        if not Date:
            print(f"Date not found in file {blob.name}")
            continue  # Skip this file if date extraction fails
        print(f"Parsed Date: {Date}")  # Debugging log

        # Start reading tab-delimited data from the appropriate line
        start_index = 7  # Data starts from line 7 in the provided file
        for line in lines[start_index:]:
            if line.strip() == "" or line.startswith("Total") or len(line.strip().split("\t")) < 2:
                continue  # Skip empty lines, the 'Total' line, and lines with fewer than 2 columns

            columns = line.strip().split("\t")
            region = columns[0].strip()  # Ensure to strip spaces for
