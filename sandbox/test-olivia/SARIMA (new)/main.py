from google.cloud import bigquery
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd
import numpy as np
import itertools
import os
import time
from google.api_core.exceptions import ServiceUnavailable

def load_and_aggregate_data_from_bigquery(table_id="CDC.staging", project_id="ba882-pipeline-olivia"):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT Disease, Weekly_incidence, Date
    FROM `{project_id}.{table_id}`
    WHERE Disease LIKE 'Chlamydia trachomatis infection §'
    """
    query_job = client.query(query)
    df = query_job.to_dataframe()  # Load data normally
    
    # Ensure correct data types after loading
    df['Date'] = pd.to_datetime(df['Date'])              # Convert Date to datetime
    df['Weekly_incidence'] = df['Weekly_incidence'].astype(float)  # Convert Weekly_incidence to float
    df['Disease'] = df['Disease'].astype(str)             # Convert Disease to string

    # Group by 'Date' to aggregate data for SARIMA
    df = df.groupby('Date').sum().reset_index()
    df['Disease'] = 'Chlamydia trachomatis infection §'
    df.set_index('Date', inplace=True)
    return df

def split_data(df):
    print("Splitting data into train, validation, and test sets...")
    train_end_date = pd.to_datetime('2024-02-29')
    val_end_date = pd.to_datetime('2024-07-31')
    train_data = df[df.index <= train_end_date].copy()
    val_data = df[(df.index > train_end_date) & (df.index <= val_end_date)].copy()
    test_data = df[df.index > val_end_date].copy()
    return train_data, val_data, test_data

def hyperparameter_tuning(train_data, val_data, param_combinations, seasonal_param_combinations):
    best_params, best_seasonal_params, best_mse = None, None, float('inf')
    for param in param_combinations:
        for seasonal_param in seasonal_param_combinations:
            try:
                model = SARIMAX(train_data['Weekly_incidence'], order=param, seasonal_order=seasonal_param)
                model_fit = model.fit(disp=False)
                val_pred = model_fit.predict(start=len(train_data), end=len(train_data) + len(val_data) - 1)
                mse = mean_squared_error(val_data['Weekly_incidence'], val_pred)
                if mse < best_mse:
                    best_mse, best_params, best_seasonal_params = mse, param, seasonal_param
            except Exception as e:
                print(f"Error with parameters {param} and seasonal {seasonal_param}: {e}")
    return best_params, best_seasonal_params

def train_and_evaluate(train_data, val_data, test_data, best_params, best_seasonal_params):
    train_val_data = pd.concat([train_data, val_data])
    endog = train_val_data['Weekly_incidence'].values
    final_model = SARIMAX(endog, order=best_params, seasonal_order=best_seasonal_params)
    final_model_fit = final_model.fit(disp=False)
    test_pred = final_model_fit.predict(start=len(endog), end=len(endog) + len(test_data) - 1)
    test_pred = np.round(test_pred)

    test_mse = mean_squared_error(test_data['Weekly_incidence'], test_pred)
    test_r2 = r2_score(test_data['Weekly_incidence'], test_pred)
    result_df = pd.DataFrame({
        'Date': test_data.index,
        'Disease': test_data['Disease'].values,
        'Original_incidence': test_data['Weekly_incidence'].values,
        'Predicted_incidence': test_pred
    })
    print(f"Test MSE: {test_mse}")
    print(f"Test R²: {test_r2}")
    return result_df

def save_to_bigquery(result_df, project_id, dataset_id, table_name):
    print(f"Saving data to BigQuery table {dataset_id}.{table_name}...")
    client = bigquery.Client(project=project_id)
    table_id = f"{dataset_id}.{table_name}"

    # Convert columns to float to avoid nullable integer issues
    result_df['Original_incidence'] = result_df['Original_incidence'].fillna(0).astype(float)
    result_df['Predicted_incidence'] = result_df['Predicted_incidence'].fillna(0).astype(float)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("Disease", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Original_incidence", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Predicted_incidence", "FLOAT", mode="REQUIRED"),
        ],
        write_disposition="WRITE_TRUNCATE"
    )

    max_retries = 5
    for attempt in range(max_retries):
        try:
            load_job = client.load_table_from_dataframe(result_df, table_id, job_config=job_config)
            load_job.result()
            print(f"Data saved to BigQuery table {table_id}")
            break
        except ServiceUnavailable as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print("Max retries reached. Could not save data to BigQuery.")
                raise

def main(request):
    project_id = os.environ.get("PROJECT_ID", "ba882-pipeline-olivia")
    dataset_id = os.environ.get("DATASET_ID", "CDC")
    table_name = os.environ.get("TABLE_NAME", "SARIMA_2")
    try:
        # Load data and split it
        data = load_and_aggregate_data_from_bigquery()
        train_data, val_data, test_data = split_data(data)

        # Define parameter ranges for hyperparameter tuning
        p = d = q = range(0, 2)
        P = D = Q = range(0, 2)
        s = 52  # Weekly seasonality
        param_combinations = list(itertools.product(p, d, q))
        seasonal_param_combinations = [(x[0], x[1], x[2], s) for x in itertools.product(P, D, Q)]

        # Perform hyperparameter tuning
        best_params, best_seasonal_params = hyperparameter_tuning(train_data, val_data, param_combinations, seasonal_param_combinations)

        # Print the best parameters for reference
        print(f"Best SARIMA parameters: {best_params}")
        print(f"Best Seasonal parameters: {best_seasonal_params}")

        # Train and evaluate the model on the test set
        result_df = train_and_evaluate(train_data, val_data, test_data, best_params, best_seasonal_params)

        # Save result to BigQuery
        save_to_bigquery(result_df, project_id, dataset_id, table_name)

        return "SARIMA model executed successfully", 200
    except Exception as e:
        return f"An error occurred during execution: {e}", 500
