from google.cloud import bigquery
import functions_framework
from flask import jsonify

def create_bigquery_schema():
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # Define dataset and table information
        dataset_id = "ba882-pipeline-olivia.CDC"

        # Define schema for the raw table
        raw_table_id = f"{dataset_id}.raw"
        raw_schema = [
            bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Region", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Weekly_incidence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED")
        ]

        # Set up raw table information
        raw_table = bigquery.Table(raw_table_id, schema=raw_schema)

        # Create raw table in BigQuery (if not exists)
        raw_table = client.create_table(raw_table, exists_ok=True)
        print(f"Created or updated raw table {raw_table_id} with schema")

        # Attempt to truncate the raw table, if it exists
        try:
            query = f"TRUNCATE TABLE `{raw_table_id}`"
            client.query(query).result()
            print(f"Cleared raw table {raw_table_id}")
        except Exception as e:
            print(f"Failed to truncate table {raw_table_id}: {e}")

        # Define schema for the staging table
        staging_table_id = f"{dataset_id}.staging"
        staging_schema = [
            bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Region", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Weekly_incidence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED")
        ]

        # Set up staging table information
        staging_table = bigquery.Table(staging_table_id, schema=staging_schema)

        # Create staging table in BigQuery (if not exists)
        staging_table = client.create_table(staging_table, exists_ok=True)
        print(f"Created or updated staging table {staging_table_id} with schema")

        return {"status": "success", "message": "Schema created successfully."}

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"status": "error", "message": str(e)}

# Google Cloud Function entry point
@functions_framework.http
def create_schema(request):
    try:
        # Call the function and get the result
        result = create_bigquery_schema()

        # Explicitly log the output for debugging
        print(f"Returning JSON response: {result}")

        # Create a proper JSON response
        response = jsonify(result)
        response.status_code = 200 if result["status"] == "success" else 500
        response.headers["Content-Type"] = "application/json"
        return response
    except Exception as e:
        print(f"Cloud Function error: {e}")
        error_response = jsonify({"status": "error", "message": str(e)})
        error_response.status_code = 500
        error_response.headers["Content-Type"] = "application/json"
        return error_response
