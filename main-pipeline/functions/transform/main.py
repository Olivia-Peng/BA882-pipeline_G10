# This script is designed to be run as a Google Cloud Function. It transforms disease data in .txt format stored in a Google Cloud Storage bucket into a structured DataFrame.
# The script extracts relevant information from the text files, filters out specific regions, and stores the processed data as a Parquet file in another Google Cloud Storage bucket.
# The script performs the following tasks:
# 1. Parses the request payload to retrieve a job ID.
# 2. Lists all text files in the specified Google Cloud Storage bucket for the given job ID.
# 3. Defines a set of regions to exclude from processing.
# 4. Extracts the date information from a specific line in the text file using a regular expression.
# 5. Processes each text file to extract disease code, region, and occurrence count, skipping lines for excluded regions.
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
        print("Error: Missing job_id in request payload")
        return "Missing job_id in request payload", 400
    print(f"Received job ID: {job_id}")

    # Define project and bucket information
    project_id = 'ba882-group-10'
    bucket_name = 'cdc-extract-txt'
    output_bucket_name = 'cdc-extract-dataframe'
    output_blob_name = f'cdc_data_{job_id}.parquet'

    # Initialize Google Cloud Storage client
    try:
        print("Initializing Google Cloud Storage client...")
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
    except Exception as e:
        print(f"Error initializing Google Cloud Storage client: {str(e)}")
        return f"Error initializing Google Cloud Storage client: {str(e)}", 500

    # List all text files in the bucket for the given job_id
    try:
        print(f"Listing blobs in bucket {bucket_name} with prefix {job_id}...")
        blobs = list(bucket.list_blobs(prefix=f"{job_id}/"))
        txt_blobs = [blob for blob in blobs if blob.name.endswith('.txt')]
        if not txt_blobs:
            print(f"No text files found for job ID {job_id} in bucket {bucket_name}")
            return f"No text files found for job ID {job_id}", 404
    except Exception as e:
        print(f"Error listing blobs: {str(e)}")
        return f"Error listing blobs: {str(e)}", 500

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
            try:
                extracted_date = datetime.strptime(date_str, "%B %d, %Y").strftime('%Y-%m-%d')
                return extracted_date
            except ValueError as e:
                print(f"Error parsing date '{date_str}': {str(e)}")
                return None
        else:
            print("Date pattern not found in line")
            return None

    # Process each text file
    for blob in txt_blobs:
        try:
            print(f"Processing file: {blob.name}")

            # Extract disease code from the filename
            disease_code_match = re.search(r"table(\d+)", blob.name)
            if disease_code_match:
                disease_code = disease_code_match.group(1)
                print(f"Extracted disease code: {disease_code}")
            else:
                print(f"Disease code not found in file name {blob.name}")
                continue  # Skip this file if disease code extraction fails

            # Download file content
            file_content = blob.download_as_text(encoding='ISO-8859-1')
            lines = file_content.splitlines()
            if len(lines) < 5:
                print(f"File {blob.name} does not contain enough lines to extract data")
                continue

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
                region = columns[0].strip()  # Ensure to strip spaces for accurate matching

                # Skip if the region is in the excluded list
                if region in excluded_regions:
                    print(f"Skipping excluded region: {region}")  # Debugging log
                    continue

                current_week_count = columns[1]

                # Handle non-numeric counts (e.g., '-', 'N', 'U')
                try:
                    current_week_count = int(current_week_count)
                except ValueError:
                    print(f"Non-numeric count '{current_week_count}' found, setting to 0")
                    current_week_count = 0  # Set to 0 if the value is not a valid integer

                # Append the extracted data to the list
                all_data.append({
                    "Disease": disease_code,
                    "Region": region,
                    "Current_Week_Occurrence_Count": current_week_count,
                    "Date": Date  # Use dynamically extracted date
                })

        except Exception as e:
            print(f"Error processing file {blob.name}: {str(e)}")
            continue

    # Convert list to DataFrame
    try:
        print("Converting collected data to DataFrame...")
        df = pd.DataFrame(all_data)
    except Exception as e:
        print(f"Error converting data to DataFrame: {str(e)}")
        return f"Error converting data to DataFrame: {str(e)}", 500

    # Store DataFrame in Google Cloud Storage as a Parquet file
    if not df.empty:
        try:
            print("Storing DataFrame in Google Cloud Storage as Parquet file...")
            output_bucket = storage_client.bucket(output_bucket_name)
            output_buffer = io.BytesIO()
            df.to_parquet(output_buffer, index=False)
            output_buffer.seek(0)  # Reset buffer position to the beginning
            output_blob = output_bucket.blob(output_blob_name)
            output_blob.upload_from_file(output_buffer, content_type='application/octet-stream')
            print(f"Stored DataFrame to {output_bucket_name}/{output_blob_name}")
        except Exception as e:
            print(f"Error storing DataFrame to Parquet file: {str(e)}")
            return f"Error storing DataFrame to Parquet file: {str(e)}", 500
    else:
        print("No data to store.")

    return json.dumps({"message": "Data processing and storage completed.", "job_id": job_id})


