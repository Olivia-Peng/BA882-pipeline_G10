from google.cloud import bigquery
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from datetime import timedelta
import logging
import gc
import argparse

# Configure logging to output in a standard format
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_data_from_staging(data_source, project_id):
    try:
        # Initialize BigQuery client with the specified project ID
        client = bigquery.Client(project=project_id)
        logger.info("BigQuery client initialized")

        # Define query to get distinct diseases and their data from the last available date
        query = f"""
        SELECT Disease, Weekly_incidence, Date
        FROM `{data_source}`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
        ORDER BY Disease, Date
        """

        query_job = client.query(query)
        df = query_job.to_dataframe()

        if df.empty:
            logger.warning("No data found in the staging table.")
        else:
            df['Weekly_incidence'] = df['Weekly_incidence'].astype('int16')
            logger.info(f"Data retrieved and processed with shape: {df.shape}")

        return df
    except Exception as e:
        logger.error(f"Error retrieving data from BigQuery: {e}")
        raise RuntimeError("Failed to retrieve data from BigQuery")

def train_sarima_model(data, disease):
    try:
        logger.info(f"Training SARIMA model for disease: {disease}")
        model = SARIMAX(data['Weekly_incidence'], order=(1, 1, 0), seasonal_order=(1, 1, 1, 52))
        model_fit = model.fit(disp=False)
        logger.info(f"SARIMA model trained successfully for disease: {disease}")
        return model_fit
    except Exception as e:
        logger.error(f"Error training SARIMA model for {disease}: {e}")
        raise RuntimeError(f"Failed to train SARIMA model for {disease}")

def make_predictions(model_fit, periods=4):
    try:
        forecast = model_fit.get_forecast(steps=periods).predicted_mean
        logger.info(f"Predictions generated: {forecast}")
        return forecast
    except Exception as e:
        logger.error(f"Error generating predictions: {e}")
        raise RuntimeError("Failed to generate predictions")

def store_predictions_in_sarima(predictions, disease, start_date, project_id, sarima_table_id):
    try:
        # Convert predictions to DataFrame for direct BigQuery insertion
        df_predictions = pd.DataFrame({
            "Disease": [disease] * len(predictions),
            "Weekly_Average_incidence": predictions.astype(int),  # Ensure INTEGER type
            "Date": [start_date + timedelta(weeks=i) for i in range(len(predictions))]
        })

        # Ensure the 'Date' column is of type 'datetime64[ns]' and convert columns to match BigQuery types
        df_predictions['Date'] = pd.to_datetime(df_predictions['Date']).dt.date  # Convert to DATE type
        df_predictions['Disease'] = df_predictions['Disease'].astype(str)  # Ensure STRING type
        df_predictions['Weekly_Average_incidence'] = df_predictions['Weekly_Average_incidence'].astype(int)  # Ensure INTEGER type

        # Log the data before inserting
        logger.info(f"Storing predictions for {disease}:\n{df_predictions.head()}")
        logger.info(f"DataFrame types:\n{df_predictions.dtypes}")

        # Insert DataFrame directly into BigQuery
        df_predictions.to_gbq(
            destination_table=sarima_table_id,
            project_id=project_id,
            if_exists='append',
            table_schema=[
                {'name': 'Disease', 'type': 'STRING'},
                {'name': 'Weekly_Average_incidence', 'type': 'INTEGER'},
                {'name': 'Date', 'type': 'DATE'}
            ]
        )
        logger.info(f"Predictions successfully stored in BigQuery for disease: {disease}")
    except Exception as e:
        logger.error(f"Error storing predictions for {disease} in BigQuery: {e}")
        raise RuntimeError(f"Failed to store predictions for {disease} in BigQuery due to {e}")

def main(data_source, project_id, dataset_id):
    try:
        logger.info("Starting the SARIMA model training job")
        sarima_table_id = f"{project_id}.{dataset_id}.SARIMA"

        # Fetch data from the staging table
        data = get_data_from_staging(data_source, project_id)
        if data.empty:
            logger.error("No data available in the staging table")
            return

        # Iterate over each unique disease and perform SARIMA prediction
        for disease in data['Disease'].unique():
            logger.info(f"Processing disease: {disease}")
            disease_data = data[data['Disease'] == disease]

            if disease_data.empty:
                logger.warning(f"No data found for disease: {disease}")
                continue

            model_fit = train_sarima_model(disease_data, disease)
            last_date = disease_data['Date'].iloc[-1]
            logger.info(f"Last date in data for {disease}: {last_date}")

            del disease_data
            gc.collect()

            predictions = make_predictions(model_fit, periods=4)
            del model_fit
            gc.collect()

            if last_date:
                store_predictions_in_sarima(predictions, disease, last_date + timedelta(weeks=1), project_id, sarima_table_id)
                logger.info(f"Prediction completed and stored in BigQuery for {disease}")
            else:
                logger.warning(f"No valid date for predictions for {disease}")

    except RuntimeError as e:
        logger.error(f"Runtime error occurred: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-source", type=str, required=True, help="BigQuery table for training data")
    parser.add_argument("--project-id", type=str, required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset-id", type=str, required=True, help="BigQuery dataset ID")
    args = parser.parse_args()

    main(args.data_source, args.project_id, args.dataset_id)
