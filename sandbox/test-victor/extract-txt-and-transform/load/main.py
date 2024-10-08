import pandas as pd
from google.cloud import storage, bigquery
import functions_framework
import pyarrow

# Google Cloud Function entry point
@functions_framework.http
def upload_txt_to_bigquery(request):
    # Define project, bucket, and dataset information
    project_id = 'ba882-group-10'
    bucket_name = 'cdc-extract-txt'
    dataset_id = "ba882-group-10.cdc_data"
    table_id = f"{dataset_id}.cdc_occurrences"

    # Initialize Google Cloud Storage client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # List all text files in the bucket
    blobs = list(bucket.list_blobs())
    txt_blobs = [blob for blob in blobs if blob.name.endswith('.txt')]

    # Initialize BigQuery client
    bigquery_client = bigquery.Client(project=project_id)

    # Process each text file and upload to BigQuery
    for blob in txt_blobs:
        print(f"Processing file: {blob.name}")

        # Download file content
        file_content = blob.download_as_text(encoding='ISO-8859-1')
        lines = file_content.splitlines()

        # Extract relevant information from the text
        disease_name = lines[4].split(";")[0].strip()  # Extracting disease name, e.g., Cryptosporidiosis
        week_year = blob.name.split('_')[0] + " " + blob.name.split('_')[1]  # Extract Week_Year from filename

        data = []

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
            data.append({
                "Disease": disease_name,
                "Region": region,
                "Current_Week_Occurrence_Count": current_week_count,
                "Week_Year": week_year
            })

        # Convert list to DataFrame
        df = pd.DataFrame(data)

        # Load DataFrame into BigQuery
        job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig())
        job.result()  # Wait for the load job to complete

        print(f"Uploaded data from {blob.name} to BigQuery table {table_id}")

    return "Upload to BigQuery completed."