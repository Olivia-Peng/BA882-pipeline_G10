# Cloud Function to create a schema in BigQuery for storing CDC data
# 
# This function creates two tables in BigQuery: a raw table and a staging table for storing CDC data.
# - If the raw table does not exist, it creates it; if it does exist, it clears the data to prepare for a new job run.
# - The staging table is also created if it does not already exist.
# - Both tables have the same schema, which includes fields for Disease, Region, Current Week Occurrence Count, and Week Year.
# This setup ensures that each new run starts with a clean raw table, while preserving the structure for both raw and staging tables.

from google.cloud import bigquery
import functions_framework

def create_bigquery_schema():
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Define dataset and table information
    dataset_id = "ba882-group-10.cdc_data"  # Replace with your dataset ID
    
    # Define schema for the raw table
    raw_table_id = f"{dataset_id}.cdc_occurrences_raw"
    raw_schema = [
        bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Current_Week_Occurrence_Count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Week_Year", "STRING", mode="REQUIRED")
    ]
    
    # Set up raw table information
    raw_table = bigquery.Table(raw_table_id, schema=raw_schema)
    
    # Create raw table in BigQuery (if not exists)
    raw_table = client.create_table(raw_table, exists_ok=True)
    print(f"Created or updated raw table {raw_table_id} with schema")

    # Clear the raw table if it already exists
    query = f"TRUNCATE TABLE `{raw_table_id}`"
    client.query(query).result()
    print(f"Cleared raw table {raw_table_id}")

    # Define schema for the staging table
    staging_table_id = f"{dataset_id}.cdc_occurrences_staging"
    staging_schema = [
        bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Current_Week_Occurrence_Count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Week_Year", "STRING", mode="REQUIRED")
    ]
    
    # Set up staging table information
    staging_table = bigquery.Table(staging_table_id, schema=staging_schema)
    
    # Create staging table in BigQuery (if not exists)
    staging_table = client.create_table(staging_table, exists_ok=True)
    print(f"Created or updated staging table {staging_table_id} with schema")

# Google Cloud Function entry point
@functions_framework.http
def create_schema(request):
    create_bigquery_schema()
    return {}, 200
