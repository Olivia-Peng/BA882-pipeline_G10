## Project Pipeline Overview

<span style='color:blue'>The main pipeline, now executing weekly,</span> follows these steps:

1. **Extract .txt files with data on disease occurrences from the CDC website** and store these files in a Google Cloud Storage bucket.
2. **<span style='color:blue'>Create the necessary schema for three BigQuery tables</span>**, including the `disease_dic` table that links disease codes with their names. if they do not already exist: one table to hold raw data and one for the stage and final table. The raw table is cleared with every pipeline iteration.
3. **<span style='color:blue'>Parse and transform the extracted .txt data using disease codes rather than names</span> into a DataFrame format and store it as a Parquet file in another Google Cloud Storage bucket.
4. **Load the transformed data into the raw BigQuery table**.
5. **Compare the records in the raw table with the stage table**. If records do not exist in the stage table, they are inserted there, avoiding duplicates.

---

### Step-by-Step Pipeline Process

### 1. **Extract Data**
- **Function**: `download-cdc-data`
- **File Location**: `/functions/extract-txt/main.py`
- **Process**:
  - The function downloads `.txt` files from the CDC website for specific diseases, weeks, and years.
  - The files are stored in the Google Cloud Storage bucket named `cdc-extract-txt`, and a unique `job_id` is generated for each pipeline run.
  - In case of network issues or file unavailability, it retries and logs errors but continues processing.

### 2. **Schema Setup**
- **Function**: `create-schema`
- **File Location**: `/functions/schema-setup/main.py`
- **Process**:
  - This function ensures that two BigQuery tables are created if they do not already exist:
    - **Raw Table**: Stores raw disease data extracted from the CDC.
    - **Stage Table**: Holds the stage and final processed data.
  - The raw table is cleared at the start of every pipeline run to avoid duplications and outdated data.

### 3. **Transform Data**
- **Function**: `transform_txt_to_dataframe`
- **File Location**: `/functions/transform/main.py`
- **Process**:
  - This function processes the `.txt` files stored in `cdc-extract-txt`, parses the data, and converts it into a structured **Pandas DataFrame**.
  - It extracts relevant data such as disease name, week, year, region, and occurrence counts.
  - The resulting DataFrame is stored as a **Parquet file** in the `cdc-extract-dataframe` Google Cloud bucket.

### 4. **Load Data into Raw Table**
- **Function**: `load-to-raw`
- **File Location**: `/functions/load-into-raw/main.py`
- **Process**:
  - The Parquet file created in the transformation step is read into a Pandas DataFrame.
  - This data is then appended to the raw BigQuery table (`cdc_occurrences_raw`) to store the newly extracted disease data.

### 5. **Upsert Data into Stage Table**
- **Function**: `load_to_stage`
- **File Location**: `/functions/load-into-stage/main.py`
- **Process**:
  - This function compares records in the raw table (`cdc_occurrences_raw`) and inserts any new records into the stage table (`cdc_occurrences_staging`).
  - It ensures that only unique records are added, avoiding duplicates by using a `LEFT JOIN` between the raw and stage tables based on disease, region, and week-year combination.

---

### Orchestration with Prefect

The flow of the pipeline is orchestrated using **Prefect**, ensuring that each step runs sequentially and handles retries or errors as necessary.

- **Prefect Flow Script**: `/flows/etl-flow.py`
- **Flow Steps**:
  1. **Extract**: Calls the `download-cdc-data` function to extract `.txt` files.
  2. **Schema Creation**: Triggers the `create-schema` function to set up or verify the BigQuery schema.
  3. **Transform**: Runs the `transform_txt_to_dataframe` function to convert `.txt` files into a DataFrame and store it as Parquet.
  4. **Load to Raw Table**: Invokes the `load-to-raw` function to upload the Parquet data to the raw BigQuery table.
  5. **Upsert to Stage Table**: Calls the `load_to_stage` function to upsert records from the raw table into the stage table.

The flow uses retries for robustness in case of network or other operational issues. 

### Changes from Phase 2

1. **Scheduling Update**  
   - Updated the main pipeline deployment in `scheduler/scheduler.py` to execute weekly instead of daily. The execution day is set to Sunday, as the CDC releases updated information each Saturday.

2. **Data Storage Optimization**  
   - Modified `/functions/transform/main.py` to store disease codes instead of disease names in both raw and stage BigQuery tables. This change addresses issues caused by inconsistent disease names in CDC's extracted `.txt` files.

3. **Schema Expansion**  
   - Added a third table, `disease_dic`, in BigQuery through `/functions/schema-setup/main.py`. This new table links disease codes with their corresponding disease names.

4. **Dictionary Population Script**  
   - Created a function in `deploy-scripts/populate-disease-dict.py` to populate the `disease_dic` table with the selected diseases for tracking.

5. **Expanded Disease Tracking**  
   - Increased the number of diseases tracked from four to ten to broaden our data monitoring.

6. **Code-based Loading Adjustments**  
   - Updated all load functions to use disease codes rather than names when loading data into BigQuery, ensuring consistency across tables.

