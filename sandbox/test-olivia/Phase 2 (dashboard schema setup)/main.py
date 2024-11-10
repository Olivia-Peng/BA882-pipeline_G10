from google.cloud import bigquery
import functions_framework
from flask import jsonify

# Core function to create the dashboard schema in BigQuery
def create_dashboard_schema_core():
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # Define dataset and table information
        dataset_id = "ba882-group-10.cdc_data"

        # Define schema for the dashboard table
        dashboard_table_id = f"{dataset_id}.dashboard"
        dashboard_schema = [
            bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("Incidence", "FLOAT", mode="NULLABLE")  # Or "INTEGER" if Incidence should be an integer
        ]

        # Set up dashboard table information
        dashboard_table = bigquery.Table(dashboard_table_id, schema=dashboard_schema)

        # Create dashboard table in BigQuery (if it does not exist)
        dashboard_table = client.create_table(dashboard_table, exists_ok=True)
        print(f"Created or updated dashboard table {dashboard_table_id} with schema")

        return {"status": "success", "message": "Schema created successfully for dashboard table."}

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"status": "error", "message": str(e)}

# Google Cloud Function entry point
@functions_framework.http
def create_dashboard_schema(request=None):
    try:
        # Call the core function and get the result
        result = create_dashboard_schema_core()

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
