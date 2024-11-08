"""
Weekly Train and Prediction Flow for CDC Disease Occurrences

This Prefect flow is designed to automate the weekly process of ensuring the necessary BigQuery tables exist,
training SARIMA models on CDC disease occurrence data, and generating predictions based on the latest models.
The flow consists of three main tasks:
1. Ensure Schema: Calls a Cloud Function to create BigQuery tables if they don't already exist.
2. Train Models: Calls a Cloud Function to train SARIMA models for each disease code based on recent data.
3. Generate Predictions: Calls a Cloud Function to generate predictions for the next 8 weeks using the latest SARIMA models.

Flow Trigger:
This flow is triggered after the "cdc-disease-ml-datasets" flow completes. This ensures that the data required
for training and prediction is up-to-date before executing this flow.

Cloud Function URLs:
Replace the placeholder URLs with the actual endpoints of your deployed Cloud Functions:
1. ensure_schema - Cloud Function to create BigQuery tables if they do not exist.
2. train_sarima_models - Cloud Function to train SARIMA models for each disease.
3. predict_sarima_models - Cloud Function to generate weekly predictions.

Logging:
Basic logging is implemented at each task and flow level to help with debugging and monitoring.
Each task logs the start, success, and failure states. The flow also logs the result of each task
and provides a summary upon completion.

Dependencies:
Make sure that your Prefect environment has the required dependencies, including `requests` and `prefect`.

Usage:
Run this script in an environment with access to Prefect and Google Cloud resources. The flow is deployed
and set to trigger weekly after the completion of the "cdc-disease-ml-datasets" flow.

"""

# Imports
import requests
from prefect import flow, task, get_run_logger

# Helper function to invoke a Cloud Function
def invoke_gcf(url: str, payload: dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

@task(retries=2)
def ensure_schema():
    """Invoke the Cloud Function to create BigQuery tables if they don't exist."""
    logger = get_run_logger()
    url = "https://your-create-schema-cloud-function-url"  # Replace with your actual URL
    logger.info("Invoking Cloud Function for schema creation: %s", url)
    try:
        resp = invoke_gcf(url, payload={})
        logger.info("Schema creation completed successfully: %s", resp)
        return resp
    except Exception as e:
        logger.error("Schema creation failed: %s", e)
        raise

@task(retries=2)
def train_sarima_models():
    """Invoke the Cloud Function to train SARIMA models."""
    logger = get_run_logger()
    url = "https://your-train-sarima-models-cloud-function-url"  # Replace with your actual URL
    logger.info("Invoking Cloud Function for model training: %s", url)
    try:
        resp = invoke_gcf(url, payload={})
        logger.info("Model training completed successfully: %s", resp)
        return resp
    except Exception as e:
        logger.error("Model training failed: %s", e)
        raise

@task(retries=2)
def predict_sarima_models():
    """Invoke the Cloud Function to generate predictions using the latest SARIMA models."""
    logger = get_run_logger()
    url = "https://your-predict-sarima-models-cloud-function-url"  # Replace with your actual URL
    logger.info("Invoking Cloud Function for prediction generation: %s", url)
    try:
        resp = invoke_gcf(url, payload={})
        logger.info("Prediction generation completed successfully: %s", resp)
        return resp
    except Exception as e:
        logger.error("Prediction generation failed: %s", e)
        raise

# Main flow definition
@flow(name="weekly-train-and-prediction", log_prints=True)
def weekly_train_and_prediction():
    logger = get_run_logger()
    logger.info("Starting the weekly train-and-prediction flow.")

    # Ensure all necessary BigQuery tables are created
    schema_result = ensure_schema()
    logger.info("Schema creation result: %s", schema_result)

    # Train SARIMA models for each disease code
    train_result = train_sarima_models()
    logger.info("Training result: %s", train_result)

    # Generate predictions for the next 8 weeks
    predict_result = predict_sarima_models()
    logger.info("Prediction result: %s", predict_result)

    logger.info("Weekly train-and-prediction flow completed successfully.")

# Allow for local testing
if __name__ == "__main__":
    weekly_train_and_prediction()

