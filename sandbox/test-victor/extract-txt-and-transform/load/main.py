import pandas as pd
from google.cloud import storage
import functions_framework
import pyarrow
import io

# Google Cloud Function entry point
@functions_framework.http
def upload_txt_to_bigquery(request):
    # Define project and bucket information
    project_id = 'ba882-group-10'
    bucket_name = 'cdc-extract-txt'
    output_bucket_name = 'cdc-extract-dataframe'
    output_blob_name = 'cdc_data.parquet'

    # Initialize Google Cloud Storage client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # List all text files in the bucket
    blobs = list(bucket.list_blobs())
    txt_blobs = [blob for blob in blobs if blob.name.endswith('.txt')]

    # Collect data from all files
    all_data = []

    # Process each text file
    for blob in txt_blobs:
        print(f"Processing file: {blob.name}")

        # Download file content
        file_content = blob.download_as_text(encoding='ISO-8859-1')
        lines = file_content.splitlines()

        # Extract relevant information from the text
        disease_name = lines[4].split(";")[0].strip()  # Extracting disease name, e.g., Cryptosporidiosis

        # Extract week and year from filename
        parts = blob.name.split('_')
        year = parts[0]
        week = parts[1]
        week_year = f"{year} {week}"
        print(f"Parsed week_year: {week_year}")  # Debugging log

        # Start reading tab-delimited data from the appropriate line
        start_index = 7  # Data starts from line 7 in the provided file
        for line in lines[start_index:]:
            if line.strip() == "" or line.startswith("Total") or len(line.strip().split("\t")) < 2:
                continue  # Skip empty lines, the 'Total' line, and lines with fewer than 2 columns

            columns = line.strip().split("\t")
            region = columns[0]
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
                "Current_Week_Occurrence_Count": current_week_count,
                "Week_Year": week_year
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

    return "Data processing and storage completed."
