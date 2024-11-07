from google.cloud import bigquery
import functions_framework
from flask import jsonify

def create_bigquery_schema():
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # Define dataset and table information
        dataset_id = "ba882-pipeline-olivia.CDC"

        # Define schema for the SARIMA table
        sarima_table_id = f"{dataset_id}.SARIMA"
        sarima_schema = [
            bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Weekly_Average_incidence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED")
        ]

        # Set up SARIMA table information
        sarima_table = bigquery.Table(sarima_table_id, schema=sarima_schema)

        # Create SARIMA table in BigQuery (if not exists)
        sarima_table = client.create_table(sarima_table, exists_ok=True)
        print(f"Created or updated SARIMA table {sarima_table_id} with schema")

        return {"status": "success", "message": "SARIMA schema created successfully."}

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"status": "error", "message": str(e)}

@functions_framework.http
def create_schema_SARIMA(request):
    try:
        result = create_bigquery_schema()
        return (result["message"], 200) if result["status"] == "success" else (result["message"], 500)
    except Exception as e:
        return (f"Cloud Function error: {str(e)}", 500)
