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
    project_id = 'ba882-pipeline-olivia'
    bucket_name = 'ba882_olivia'
    output_bucket_name = 'ba882_olivia'
    output_blob_name = f'parse_to_parquet/{job_id}.parquet'

    # Initialize Google Cloud Storage client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # List all text files in the 'extract_to_txt/{job_id}' folder in the bucket
    blobs = list(bucket.list_blobs(prefix=f"extract_to_txt/{job_id}/"))
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
        date_line = lines[0]  # Assuming the date is in the first line as shown in the example image
        Date = extract_date_from_line(date_line)
        if not Date:
            print(f"Date not found in file {blob.name}")
            continue  # Skip this file if date extraction fails
        print(f"Parsed Date: {Date}")  # Debugging log

        # Start reading tab-delimited data from the appropriate line
        start_index = 7  # Data starts from line 7 in the provided file
        for line in lines[start_index:]:
            if line.strip() == "" or len(line.strip().split("\t")) < 2:
                continue  # Skip empty lines and lines with fewer than 2 columns

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
                current_week_count = 0  # Set to 0 if the value is not a valid integer

            # Append the extracted data to the list
            all_data.append({
                "Disease": disease_name,
                "Region": region,
                "Weekly_incidence": current_week_count,
                "Date": Date  # Use dynamically extracted date
            })

    # Convert list to DataFrame
    df = pd.DataFrame(all_data)

    # Store DataFrame in Google Cloud Storage as a Parquet file
    if not df.empty:
        output_bucket = storage_client.bucket(output_bucket_name)
        output_buffer = io.BytesIO()
        df.to_parquet(output_buffer, index=False)
        output_buffer.seek(0)  # Reset buffer position to the beginning
        output_blob = output_bucket.blob(output_blob_name)
        output_blob.upload_from_file(output_buffer, content_type='application/octet-stream')
        print(f"Stored DataFrame to {output_bucket_name}/{output_blob_name}")
    else:
        print("No data to store.")

    return json.dumps({"message": "Data processing and storage completed.", "job_id": job_id})
