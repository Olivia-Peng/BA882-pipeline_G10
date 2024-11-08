"""
Cloud Function to create or update BigQuery views for multiple CDC disease codes, 
convert each view to a pandas DataFrame with the Date column as a datetime type, 
and save them as CSV files in Google Cloud Storage.
The function filters data for each specified disease code, groups by date, and sums 
the `Current_Week_Occurrence_Count` to get total occurrences per date.

Function Steps:
1. Loop through a list of disease codes.
2. For each disease code:
   - Define and execute a SQL query to create or replace a view with aggregated data.
   - Query the view, convert the data to a pandas DataFrame, and convert `Date` to datetime.
   - Save the DataFrame as a CSV file in a specified GCS bucket, with a unique path for each disease code.

Requirements:
- Google Cloud Project ID: ba882-group-10
- BigQuery dataset: cdc_data
- BigQuery staging table: cdc_occurrences_staging
- GCS bucket: ba882-group-10-mlops
"""

# Imports
import functions_framework
from google.cloud import bigquery, storage
import pandas as pd

# Settings
project_id = 'ba882-group-10'
dataset_id = 'cdc_data'
table_id = 'cdc_occurrences_staging'
ml_bucket_name = 'ba882-group-10-mlops'
disease_codes = ['370']  # Add disease codes to this list as needed

# Cloud Function to create or update BigQuery views and save them as CSVs in GCS
@functions_framework.http
def task(request):
    # Initialize BigQuery and GCS clients
    client = bigquery.Client()
    storage_client = storage.Client()
    csv_paths = []

    for disease_code in disease_codes:
        # Define the view name and SQL query for the current disease code
        view_name = f"cdc_occurrences_view_{disease_code}"
        view_sql = f"""
        CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{view_name}` AS
        SELECT
            Date,
            SUM(Current_Week_Occurrence_Count) AS Total_Occurrences
        FROM
            `{project_id}.{dataset_id}.{table_id}`
        WHERE
            Disease = '{disease_code}'
        GROUP BY
            Date
        ORDER BY
            Date;
        """
        
        # Create or update the view in BigQuery
        print(f"Creating or updating the BigQuery view for disease code {disease_code}...")
        job = client.query(view_sql)
        job.result()  # Wait for the job to complete

        # Query the view and load it into a pandas DataFrame
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{view_name}`"
        df = client.query(query).to_dataframe()

        # Convert the Date column from string to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

        # Define the GCS path for the CSV file
        csv_path = f"training-data/code-{disease_code}/cdc_occurrences_{disease_code}.csv"
        blob = storage_client.bucket(ml_bucket_name).blob(csv_path)
        
        # Write DataFrame to CSV in-memory and upload to GCS
        print(f"Writing the DataFrame for disease code {disease_code} to GCS as CSV...")
        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

        # Add the CSV path to the list of output paths
        csv_paths.append(f"gs://{ml_bucket_name}/{csv_path}")

    return {
        "status": "Views created or updated successfully and saved as CSV",
        "csv_paths": csv_paths
    }, 200
