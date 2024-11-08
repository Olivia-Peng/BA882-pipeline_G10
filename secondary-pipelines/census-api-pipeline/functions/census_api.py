import functions_framework
import requests
from google.cloud import bigquery
import logging
from flask import jsonify

# Set up logging
logging.basicConfig(level=logging.INFO)

def get_fips_mapping():
    url = "https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt"
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    data = response.text.strip().splitlines()  # Split the text into lines
    
    fips_mapping = {}
    
    for line in data[1:]:  # Skip the header line
        columns = line.split('|')  # Split by the pipe character
        if len(columns) >= 4:  # Ensure there are enough columns
            state_name = columns[3].strip()  # STATE_NAME
            state_fips = columns[1].strip()  # STATEFP
            fips_mapping[state_name] = state_fips  # Map state name to FIPS code
    return fips_mapping

def get_racial_data_by_location(api_key, state_fips):
    base_url = "https://api.census.gov/data/2022/acs/acs5"
    variables = "B01003_001E,B02001_002E,B02001_003E,B02001_004E,B02001_005E,B19013_001E"
    
    params = {
        "get": variables,
        "for": f"state:{state_fips}",  # Use the state FIPS code
        "key": api_key
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()  # Parse the JSON response
        headers = data[0]  # The first item contains the headers
        values = data[1]   # The second item contains the values
        racial_data = dict(zip(headers, values))  # Combine headers and values into a dictionary
        return racial_data  # Return the retrieved data
    except requests.RequestException as e:
        logging.error(f"HTTP request failed for state FIPS {state_fips}: {e}")  # Log request errors
    except ValueError:
        logging.error("Failed to decode JSON from response.")  # Log JSON decoding errors
    
    return None  # Return None if there is an error

def create_table_if_not_exists():
    project_id = 'ba882-group-10'
    client = bigquery.Client(project=project_id)
    dataset_id = "demographic_data"
    table_id = "census_data"
    
    schema = [
        bigquery.SchemaField("White", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Black", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Native_American", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Asian", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Income", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Total_Population", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("State", "STRING", mode="REQUIRED")
    ]
    
    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)  # Check if the table exists
        logging.info("Table already exists.")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # Create table if it does not exist
        logging.info("Table created.")

def insert_data_to_bigquery(racial_data_list):
    project_id = 'ba882-group-10'
    client = bigquery.Client(project=project_id)
    dataset_id = "demographic_data"
    table_id = "census_data"
    table_ref = client.dataset(dataset_id).table(table_id)

    rows_to_insert = []
    for state_name, data in racial_data_list.items():
        row = {
            "White": float(data["B02001_002E"]),
            "Black": float(data["B02001_003E"]),
            "Native_American": float(data["B02001_004E"]),
            "Asian": float(data["B02001_005E"]),
            "Income": int(data["B19013_001E"]),
            "Total_Population": int(data["B01003_001E"]),
            "State": state_name
        }
        rows_to_insert.append(row)

    # Insert the rows into BigQuery
    errors = client.insert_rows_json(table_ref, rows_to_insert)  # Directly append JSON data to BigQuery

    if not errors:
        logging.info("Data has been inserted successfully into BigQuery.")
    else:
        logging.error("Encountered errors while inserting rows: %s", errors)

@functions_framework.http
def census_api(request):
    logging.info("Received request: %s", request.data)

    api_key = "554ab3869e8e9f3d0140721412325e8e9f1385ef"
    
    # Get the state FIPS mapping
    state_fips_mapping = get_fips_mapping()
    logging.info("Fetched FIPS mapping: %s", state_fips_mapping)
    
    # Get state names from the FIPS mapping
    state_names = state_fips_mapping.keys()
    
    racial_data_list = {}
    for state_name in state_names:
        state_fips = state_fips_mapping.get(state_name)
        if state_fips:
            racial_data = get_racial_data_by_location(api_key, state_fips)
            if racial_data:
                racial_data_list[state_name] = racial_data
            else:
                logging.warning("No racial data found for state FIPS %s", state_fips)

    # Create table if not exists and insert the racial data into BigQuery
    create_table_if_not_exists()
    insert_data_to_bigquery(racial_data_list)

    return jsonify({'status': 'Data processing completed.'})
