from google.cloud import bigquery
from google.oauth2 import service_account
import datetime  # Import datetime module

# Define project and dataset ID
project_id = "ba882-group-10"
dataset_id = "cdc_data"
dashboard_table_id = f"{project_id}.{dataset_id}.dashboard"

# Initialize BigQuery client with explicit project ID
client = bigquery.Client(project=project_id)

# Query data from the staging table
def get_staging_data():
    staging_query = """
    SELECT 
        Disease, 
        Date, 
        SUM(Current_Week_Occurrence_Count) AS Incidence
    FROM `ba882-group-10.cdc_data.cdc_occurrences_staging`
    WHERE Disease LIKE '370'
    GROUP BY Disease, Date
    ORDER BY Date
    """
    staging_data = client.query(staging_query).result()
    return [{"Disease": row.Disease, "Date": row.Date, "Incidence": row.Incidence} for row in staging_data]

# Query data from the predictions table
def get_prediction_data():
    prediction_query = """
    SELECT 
        date AS Date, 
        Disease, 
        predicted_occurrence AS Incidence
    FROM 
        `ba882-group-10.cdc_data.predictions`
    WHERE date > '2024-11-01'
    """
    prediction_data = client.query(prediction_query).result()
    return [{"Disease": row.Disease, "Date": row.Date, "Incidence": row.Incidence} for row in prediction_data]

# Merge data and insert it into the dashboard table
def insert_into_dashboard():
    staging_data = get_staging_data()
    prediction_data = get_prediction_data()
    
    # Combine the data from both sources
    combined_data = staging_data + prediction_data  # Concatenate the two lists of dictionaries

    # Prepare rows for insertion, converting Date to string format if it's a date object
    rows_to_insert = [
        {
            "Disease": data["Disease"],
            "Date": data["Date"].isoformat() if isinstance(data["Date"], (datetime.date, datetime.datetime)) else data["Date"],
            "Incidence": data["Incidence"]
        }
        for data in combined_data
    ]

    # Define schema for the dashboard table
    dashboard_schema = [
        bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("Incidence", "FLOAT", mode="NULLABLE")
    ]
    
    # Set up dashboard table information
    dashboard_table = bigquery.Table(dashboard_table_id, schema=dashboard_schema)
    
    # Create dashboard table in BigQuery (if it doesn't exist)
    try:
        client.get_table(dashboard_table_id)  # Check if the table exists
        print(f"Table {dashboard_table_id} already exists.")
    except Exception:
        dashboard_table = client.create_table(dashboard_table)  # Create table if it doesn't exist
        print(f"Table {dashboard_table_id} created successfully.")
    
    # Insert rows into the dashboard table
    errors = client.insert_rows_json(dashboard_table_id, rows_to_insert)
    if errors:
        print(f"Failed to insert rows: {errors}")
    else:
        print("Data successfully stored in the dashboard table.")

# Run the insertion function
insert_into_dashboard()
