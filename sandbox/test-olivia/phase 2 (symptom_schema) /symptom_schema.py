from google.cloud import bigquery
import functions_framework
from flask import jsonify

def create_bigquery_schema():
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # Define dataset and table information
        dataset_id = "ba882-pipeline-olivia.CDC"

        # Define schema for the symptom table
        symptom_table_id = f"{dataset_id}.symptom"
        symptom_schema = [
            bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Overview", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("How it Spreads", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Symptoms in Women", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Symptoms in Men", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Symptoms from Rectal Infections", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Prevention", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Treatment and Recovery", "STRING", mode="NULLABLE")
        ]

        # Set up symptom table information
        symptom_table = bigquery.Table(symptom_table_id, schema=symptom_schema)

        # Create symptom table in BigQuery (if not exists)
        symptom_table = client.create_table(symptom_table, exists_ok=True)
        print(f"Created or updated symptom table {symptom_table_id} with schema")

        return {"status": "success", "message": "Schema created successfully for symptom table."}

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"status": "error", "message": str(e)}

# Google Cloud Function entry point
@functions_framework.http
def create_symptom_schema(request):
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
