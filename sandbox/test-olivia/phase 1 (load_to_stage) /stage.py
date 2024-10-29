# imports
import functions_framework
from google.cloud import bigquery
import json

# Project variables
project_id = 'ba882-pipeline-olivia'
dataset_id = 'CDC'  # Just use the dataset name, not the full path

# Google Cloud Function entry point
@functions_framework.http
def task(request):
    try:
        # Parse the request data (optional)
        request_json = request.get_json(silent=True)
        print(f"Request: {json.dumps(request_json)}")

        # Initialize BigQuery client
        client = bigquery.Client()

        # Define table names
        raw_table_id = f"{project_id}.{dataset_id}.raw"
        stage_table_id = f"{project_id}.{dataset_id}.staging"

        # Query to get records from raw table that are not in the staging table
        query = f"""
        INSERT INTO `{stage_table_id}` (Disease, Region, Weekly_incidence, Date)
        SELECT r.Disease, r.Region, r.Weekly_incidence, r.Date
        FROM `{raw_table_id}` r
        LEFT JOIN `{stage_table_id}` s
        ON r.Disease = s.Disease AND r.Region = s.Region AND r.Date = s.Date
        WHERE s.Disease IS NULL
        """

        # Execute the query
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        print(f"Inserted new records from {raw_table_id} into {stage_table_id}")

        return json.dumps({"message": "Data inserted successfully"}), 200

    except Exception as e:
        # Log the error for debugging
        print(f"Error: {str(e)}")
        return json.dumps({"error": str(e)}), 500
