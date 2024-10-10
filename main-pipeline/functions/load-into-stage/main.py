# Google Cloud Function to upsert records from raw table to staging table in BigQuery
# 
# This function takes records from the raw table (`cdc_occurrences_raw`) and inserts them into the staging table (`cdc_occurrences_staging`) in BigQuery.
# It ensures that only new records are copied from the raw table to the staging table, based on the unique combination of Disease, Region, and Week_Year.
# Records that already exist in the staging table are not duplicated.

# imports
import functions_framework
from google.cloud import bigquery
import json

# Project variables
project_id = 'ba882-group-10'  # Replace with your project ID

dataset_id = f"{project_id}.cdc_data"  # Dataset ID

# Google Cloud Function entry point
@functions_framework.http
def task(request):
    # Parse the request data
    request_json = request.get_json(silent=True)
    print(f"request: {json.dumps(request_json)}")

    # Initialize BigQuery client
    client = bigquery.Client()

    # Define table names
    raw_table_id = f"{dataset_id}.cdc_occurrences_raw"
    stage_table_id = f"{dataset_id}.cdc_occurrences_staging"

    # Query to get records from raw table that are not in the staging table
    query = f"""
    INSERT INTO `{stage_table_id}` (Disease, Region, Current_Week_Occurrence_Count, Week_Year)
    SELECT r.Disease, r.Region, r.Current_Week_Occurrence_Count, r.Week_Year
    FROM `{raw_table_id}` r
    LEFT JOIN `{stage_table_id}` s
    ON r.Disease = s.Disease AND r.Region = s.Region AND r.Week_Year = s.Week_Year
    WHERE s.Disease IS NULL
    """

    # Execute the query
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete
    print(f"Inserted new records from {raw_table_id} into {stage_table_id}")

    return {}, 200