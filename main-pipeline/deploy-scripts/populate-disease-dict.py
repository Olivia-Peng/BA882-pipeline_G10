from google.cloud import bigquery

# Function to populate the disease_dic table with disease codes and labels, avoiding duplicates
def populate_disease_dic():
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define dataset and table information
    dataset_id = "ba882-group-10.cdc_data"  # Replace with your dataset ID
    disease_dic_id = f"{dataset_id}.disease_dic"

    # Data to be inserted
    rows_to_insert = [
        {"disease_code": 250, "disease_label": "Babesiosis"},
        {"disease_code": 350, "disease_label": "Campylobacteriosis"},
        {"disease_code": 354, "disease_label": "Candida auris"},
        {"disease_code": 370, "disease_label": "Chlamydia trachomatis infection"}
    ]

    # Query to check for existing records
    query = f"SELECT disease_code FROM `{disease_dic_id}`"
    existing_codes = {row.disease_code for row in client.query(query).result()}

    # Filter rows to insert that do not already exist in the table
    new_rows = [row for row in rows_to_insert if row["disease_code"] not in existing_codes]

    # Insert new rows if there are any
    if new_rows:
        errors = client.insert_rows_json(disease_dic_id, new_rows)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"Inserted disease codes and labels into {disease_dic_id}")
    else:
        print(f"No new rows to insert into {disease_dic_id}")

# Run the function if this script is executed
def main():
    populate_disease_dic()

if __name__ == "__main__":
    main()
