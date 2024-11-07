from google.cloud import bigquery
import functions_framework
from flask import jsonify

def create_bigquery_schema():
    try:
        # Initialize BigQuery client
        client = bigquery.Client()

        # Define dataset and table information
        dataset_id = "ba882-pipeline-olivia.CDC"

        # Define schema for the SARIMA_2 table
        sarima_table_id = f"{dataset_id}.SARIMA_2"
        sarima_schema = [
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Region", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Original_incidence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Predicted_incidence", "INTEGER", mode="REQUIRED"),
        ]

        # Set up SARIMA_2 table information
        sarima_table = bigquery.Table(sarima_table_id, schema=sarima_schema)

        # Create SARIMA_2 table in BigQuery (if not exists)
        sarima_table = client.create_table(sarima_table, exists_ok=True)
        print(f"Created or updated SARIMA_2 table {sarima_table_id} with schema")

        return {"status": "success", "message": "SARIMA_2 schema created successfully."}

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"status": "error", "message": str(e)}

@functions_framework.http
def create_schema_SARIMA_2(request):
    try:
        result = create_bigquery_schema()
        return (result["message"], 200) if result["status"] == "success" else (result["message"], 500)
    except Exception as e:
        return (f"Cloud Function error: {str(e)}", 500)
