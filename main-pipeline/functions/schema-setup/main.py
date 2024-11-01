# Cloud Function to create a schema in BigQuery for storing CDC data
# 
# This function creates three tables in BigQuery: a raw table, a staging table for storing CDC data, and a disease dictionary table.
# - If the raw and staging tables do not exist, it creates them; if they do exist, it clears the data to prepare for a new job run.
# - The disease dictionary table (disease_dic) is created if it does not already exist.
# - The disease dictionary table contains mappings of disease codes to disease labels.

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
        bigquery.SchemaField("Date", "STRING", mode="REQUIRED")
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
        bigquery.SchemaField("Date", "STRING", mode="REQUIRED")
    ]
    
    # Set up staging table information
    staging_table = bigquery.Table(staging_table_id, schema=staging_schema)
    
    # Create staging table in BigQuery (if not exists)
    staging_table = client.create_table(staging_table, exists_ok=True)
    print(f"Created or updated staging table {staging_table_id} with schema")

    # Define schema for the disease dictionary table
    disease_dic_id = f"{dataset_id}.disease_dic"
    disease_dic_schema = [
        bigquery.SchemaField("disease_code", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("disease_label", "STRING", mode="REQUIRED")
    ]
    
    # Set up disease dictionary table information
    disease_dic_table = bigquery.Table(disease_dic_id, schema=disease_dic_schema)
    
    # Create disease dictionary table in BigQuery (if not exists)
    disease_dic_table = client.create_table(disease_dic_table, exists_ok=True)
    print(f"Created or updated disease dictionary table {disease_dic_id} with schema")

# Google Cloud Function entry point
@functions_framework.http
def create_schema(request):
    create_bigquery_schema()
    return {}, 200

