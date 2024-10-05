#This script will create the staging tables for our project on BigQuery. Not tested yet.

import functions_framework
from google.cloud import bigquery

# settings
project_id = 'ba882-group-10'
dataset_id = 'disease_tracking_stage'
table_id = 'disease_report'

@functions_framework.http
def task(request):

    # Create BigQuery client
    client = bigquery.Client()

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    try:
        client.get_dataset(dataset_ref)  # Check if dataset exists
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_id}")

    ##################################################### Create the table

    # Define the schema based on the image
    schema = [
        bigquery.SchemaField("area", "STRING"),             # area: Object -> STRING in BigQuery
        bigquery.SchemaField("disease", "STRING"),          # disease: Object -> STRING in BigQuery
        bigquery.SchemaField("disease_count", "INTEGER"),   # disease_count: Int -> INTEGER in BigQuery
        bigquery.SchemaField("year", "DATE"),               # year: Datetime -> DATE in BigQuery
        bigquery.SchemaField("week", "DATE"),               # week: Datetime -> DATE in BigQuery
    ]

    # Check if the table exists, if not create it
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)  # Check if table exists
    except:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # Create the table
        print(f"Created table {table_id} in dataset {dataset_id}")

    return "Table creation complete", 200
