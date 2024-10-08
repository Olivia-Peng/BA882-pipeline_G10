# Cloud Function to create a schema in BigQuery for storing CDC data

from google.cloud import bigquery
import functions_framework

def create_bigquery_schema():
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Define dataset and table information
    dataset_id = "ba882-group-10.cdc_data"  # Replace with your dataset ID
    table_id = f"{dataset_id}.cdc_occurrences"
    
    # Define schema for the table
    schema = [
        bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Current_Week_Occurrence_Count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Week_Year", "STRING", mode="REQUIRED")
    ]
    
    # Set up table information
    table = bigquery.Table(table_id, schema=schema)
    
    # Create table in BigQuery (if not exists)
    table = client.create_table(table, exists_ok=True)
    print(f"Created table {table_id} with schema")

# Google Cloud Function entry point
@functions_framework.http
def create_schema(request):
    create_bigquery_schema()
    return "Schema created successfully."