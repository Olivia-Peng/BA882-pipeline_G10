from google.cloud import bigquery
import functions_framework
from statsmodels.tsa.statespace.sarimax import SARIMAX
import pandas as pd
from datetime import timedelta
import logging
import gc

# Configure logging
logging.basicConfig(level=logging.INFO)

# Project variables
project_id = 'ba882-pipeline-olivia'
dataset_id = 'CDC'
staging_table_id = f"{project_id}.{dataset_id}.staging"
sarima_table_id = f"{project_id}.{dataset_id}.SARIMA"

# Hard-coded disease name
specific_disease = "Chlamydia trachomatis infection §"

def get_data_from_staging(disease=specific_disease):
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        logging.info("BigQuery client initialized")

        # Define query with filtering by the specific disease and date, limiting to the last 6 months
        query = f"""
        SELECT Disease, Weekly_incidence, Date
        FROM `{staging_table_id}`
        WHERE Disease = @disease
        AND Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
        ORDER BY Date
        """
        
        # Use a parameterized query to avoid SQL injection
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("disease", "STRING", disease)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()

        # Check if data is empty
        if df.empty:
            logging.warning("No data found for the specified disease.")
            return df

        # Optimize DataFrame memory usage
        df['Weekly_incidence'] = df['Weekly_incidence'].astype('int16')
        logging.info("Data retrieved and processed")
        return df
    except Exception as e:
        logging.error(f"Error retrieving data from BigQuery: {e}")
        raise

def train_sarima_model(data):
    try:
        # Configure and train SARIMA model with simplified parameters to reduce memory usage
        model = SARIMAX(data['Weekly_incidence'], order=(1, 1, 0), seasonal_order=(1, 1, 1, 52))
        model_fit = model.fit(disp=False)
        logging.info("SARIMA model trained successfully")
        return model_fit
    except Exception as e:
        logging.error(f"Error training SARIMA model: {e}")
        raise

def make_predictions(model_fit, periods=1):
    try:
        # Generate future predictions in a memory-efficient way
        forecast = model_fit.get_forecast(steps=periods).predicted_mean
        logging.info("Predictions generated successfully")
        return forecast
    except Exception as e:
        logging.error(f"Error generating predictions: {e}")
        raise

def store_predictions_in_sarima(predictions, disease, start_date):
    try:
        # Convert predictions to DataFrame for direct BigQuery insertion
        df_predictions = pd.DataFrame({
            "Disease": [disease] * len(predictions),
            "Weekly_Average_incidence": predictions.astype(int),
            "Date": [start_date + timedelta(weeks=i) for i in range(len(predictions))]
        })

        # Insert DataFrame directly into BigQuery
        df_predictions.to_gbq(destination_table=sarima_table_id, project_id=project_id, if_exists='append')
        logging.info("Predictions successfully stored in BigQuery")
    except Exception as e:
        logging.error(f"Error storing predictions in BigQuery: {e}")
        raise

@functions_framework.http
def run_sarima_prediction(request):
    try:
        logging.info("Starting SARIMA prediction function")

        # Fetch data for the specific disease
        data = get_data_from_staging()
        if data.empty:
            logging.warning("No data available for the specified disease")
            return "No data available for the specified disease.", 400

        # Train SARIMA model
        model_fit = train_sarima_model(data)
        
        # Retrieve the last date before deleting `data`
        last_date = data['Date'].iloc[-1] if not data.empty else None
        del data  # Free up memory
        gc.collect()  # Force garbage collection

        # Generate predictions
        predictions = make_predictions(model_fit, periods=1)
        del model_fit  # Free up memory
        gc.collect()

        # Ensure last_date is valid before storing predictions
        if last_date:
            store_predictions_in_sarima(predictions, specific_disease, last_date + timedelta(weeks=1))
            logging.info("Prediction completed and stored in BigQuery")
            return "Prediction completed and stored in BigQuery.", 200
        else:
            logging.warning("No valid date for predictions")
            return "No valid date for predictions.", 400
    except Exception as e:
        logging.error(f"Error in SARIMA prediction function: {e}")
        return f"Internal server error: {e}", 500
