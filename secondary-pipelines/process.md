1. Workflow for symptom table schema setup

**Initialize BigQuery Client**
  - Set up a BigQuery client to allow communication with Google BigQuery.

**Define Dataset and Table Information**
  - Specify the dataset ID in BigQuery where the table will reside.
  - Specify the table ID named symptom.

**Define Table Schema**
  - Specify the schema fields for the symptom table, such as `Disease`, `Overview`, `How it Spreads`, `Symptoms in Women` ,
    `Symptoms in Men"`, `Symptoms from Rectal Infections`, `Prevention`, `Treatment and Recovery`.

**Summary**
This workflow outlines setting up a BigQuery schema using a Google Cloud Function, including initializing the BigQuery client, defining the schema, creating the table, and handling responses.

2. Workflow for symptom info

**Configure Dataset**
  - Define a dictionary `predefined_data` containing various diseases and their information for each field in the schema.

**Define Functions**
  - `create_bigquery_table`: Creates the symptom table in BigQuery with a predefined schema if it doesn't already exist.
  - `store_data_in_bigquery`: Inserts data for each disease into the BigQuery table.
  - `run_scraping_to_bigquery`: Acts as the main entry point (triggered via an HTTP request) to initiate the entire workflow.

**Insert Predefined Data into BigQuery**
  - Loop through each disease in the `predefined_data` dictionary in `run_scraping_to_bigquery`.
  - For each disease, add the disease name to the data dictionary and call `store_data_in_bigquery` to insert the data as a row in BigQuery.
  - Inside `store_data_in_bigquery`, prepare `rows_to_insert` using the disease information from `predefined_data`, matching the fields in the BigQuery schema.

**Summary**
This workflow allows automated schema creation and data insertion into BigQuery for a symptom table. Each time `run_scraping_to_bigquery` is triggered, it verifies the table's existence, creates it if absent, and populates it with predefined data for various diseases.
