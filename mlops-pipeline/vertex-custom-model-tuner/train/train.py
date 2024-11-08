"""
SARIMA Model Hyperparameter Tuning with Vertex AI and GCS Integration

This script performs SARIMA model hyperparameter tuning using Google Vertex AI 
and stores the results in a Google Cloud Storage (GCS) bucket. It utilizes 
environment variables for configuring GCP settings, making the script adaptable 
to different environments and simplifying configuration management.

### Key Components

1. **Environment Variables for GCP Configurations**:
   - `GCP_PROJECT`: Specifies the Google Cloud project ID. Default: `'ba882-group-10'`.
   - `GCP_REGION`: Specifies the GCP region for Vertex AI resources. Default: `'us-central1'`.
   - `GCS_BUCKET`: GCS bucket where the results will be stored. Default: `'ba882-group-10-mlops'`.
   - `MODEL_OUTPUT_PATH`: Path within the GCS bucket for saving the tuning results.
     Default: `'tunning_results/{disease_code}'`.

2. **SARIMA Model Tuning Parameters**:
   - SARIMA parameters (p, d, q, P, D, Q, s) are tuned using Vertex AI, testing 
     quarterly, annual, and weekly seasonality patterns.

3. **UUID for Model ID**:
   - A unique `model_id` is generated using `uuid.uuid4()` to ensure that each model 
     run has a distinct identifier.

4. **Functions**:
   - **`split_data`**: Splits the data into training and validation sets, with the 
     last 3 months reserved for validation.
   - **`train_sarima`**: Trains the SARIMA model using specified parameters and 
     calculates MSE, MAE, and R-squared metrics on the validation set.
   - **`run_vertex_ai_tuning`**: Configures and launches the Vertex AI hyperparameter 
     tuning job for SARIMA.
   - **`save_best_params`**: Saves the best parameters, model ID, and metrics to GCS 
     as a JSON file.
   - **`main`**: Loads data, splits it, runs tuning, and saves the results.

5. **Logging**:
   - Logging is configured to `INFO` level, providing runtime feedback, such as the 
     completion of file uploads to GCS.

### Usage

- Ensure the environment variables are set, or rely on defaults.
- Customize the `data_path` for your local data source before running the script.

### Example GCS Path for JSON Results
The JSON file with the tuning results will be saved to:
`gs://ba882-group-10-mlops/tunning_results/{disease_code}/{disease_code}_params.json`

"""

import json
import os
import pandas as pd
import itertools
import uuid
import logging
from google.cloud import storage, aiplatform
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from datetime import datetime

#Set what diease codes we want to train for
disease_code = '370'

# Set up logging
logging.basicConfig(level=logging.INFO)

# Load environment variables for GCP configurations
project_id = os.getenv('GCP_PROJECT', 'ba882-group-10')
gcp_region = os.getenv('GCP_REGION', 'us-central1')
bucket_name = os.getenv('GCS_BUCKET', 'ba882-group-10-mlops')
output_path = os.getenv('MODEL_OUTPUT_PATH', f'tunning_results/{disease_code}')

# Define other variables
disease_code = '370'
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
model_id = f'sarima_model_{disease_code}_{timestamp}_{str(uuid.uuid4())}_tuned'

# Vertex AI tuning ranges for SARIMA parameters
param_distributions = {
    'p': (0, 2),
    'd': (0, 1),
    'q': (0, 2),
    'P': (0, 1),
    'D': (0, 1),
    'Q': (0, 1),
    's': [4, 12, 52]  # Quarterly, annual, and weekly seasonality
}

# Function to split data
def split_data(df):
    cutoff_date = df['Date'].max() - pd.DateOffset(months=3)
    train_df = df[df['Date'] < cutoff_date]
    val_df = df[df['Date'] >= cutoff_date]
    return train_df, val_df

# Function to train SARIMA model and calculate metrics
def train_sarima(train_df, val_df, params):
    model = SARIMAX(
        train_df['Total_Occurrences'], 
        order=(params['p'], params['d'], params['q']),
        seasonal_order=(params['P'], params['D'], params['Q'], params['s']),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    fit_model = model.fit(disp=False)
    pred = fit_model.get_forecast(steps=len(val_df))
    pred_values = pred.predicted_mean

    # Calculate evaluation metrics
    mse = mean_squared_error(val_df['Total_Occurrences'], pred_values)
    mae = mean_absolute_error(val_df['Total_Occurrences'], pred_values)
    r2 = r2_score(val_df['Total_Occurrences'], pred_values)
    
    return mse, mae, r2

# Vertex AI Hyperparameter Tuning
def run_vertex_ai_tuning():
    aiplatform.init(project=project_id, location=gcp_region)

    # Define the Vertex AI hyperparameter tuning job
    job = aiplatform.HyperparameterTuningJob(
        display_name='sarima-hyperparameter-tuning',
        model_display_name='sarima',
        parameter_spec={
            'p': {'parameterType': 'INTEGER', 'minValue': 0, 'maxValue': 2},
            'd': {'parameterType': 'INTEGER', 'minValue': 0, 'maxValue': 1},
            'q': {'parameterType': 'INTEGER', 'minValue': 0, 'maxValue': 2},
            'P': {'parameterType': 'INTEGER', 'minValue': 0, 'maxValue': 1},
            'D': {'parameterType': 'INTEGER', 'minValue': 0, 'maxValue': 1},
            'Q': {'parameterType': 'INTEGER', 'minValue': 0, 'maxValue': 1},
            's': {'parameterType': 'CATEGORICAL', 'categoricalValues': [4, 12, 52]},
        },
        metric_spec={'mse': 'MINIMIZE'},
        custom_job_spec={
            'worker_pool_specs': [{
                'machine_spec': {
                    'machine_type': 'n1-standard-4'
                },
                'replica_count': 1,
                'container_spec': {
                    'image_uri': 'gcr.io/path-to-your-custom-image'  # Replace with your custom training container
                }
            }]
        },
        max_trial_count=20,
        parallel_trial_count=5
    )
    
    # Execute the tuning job
    job.run()

# Save best parameters and metrics to GCS
def save_best_params(best_params, best_mse, best_mae, best_r2):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{output_path}/{disease_code}_params.json")
    
    result = {
        'disease_code': disease_code,
        'model_id': model_id,
        'created_at': datetime.now().isoformat(),
        'best_params': best_params,
        'MSE': best_mse,
        'MAE': best_mae,
        'R_squared': best_r2
    }
    
    blob.upload_from_string(json.dumps(result), content_type="application/json")
    logging.info(f"Saved best parameters to gs://{bucket_name}/{output_path}/{disease_code}_params.json")

# Main function to load data, split, and run tuning
def main(data_path):
    # Load data
    df = pd.read_csv(data_path)
    df['Date'] = pd.to_datetime(df['Date'])  # Ensure date is in datetime format
    
    # Split data
    train_df, val_df = split_data(df)
    
    # Run Vertex AI hyperparameter tuning
    run_vertex_ai_tuning()

    # After tuning, retrieve the best parameters and metrics (pseudo-code)
    best_params = {
        # This is a placeholder, replace with actual best params from tuning job
        'p': 1, 'd': 1, 'q': 1,
        'P': 1, 'D': 0, 'Q': 1, 's': 12
    }
    best_mse, best_mae, best_r2 = train_sarima(train_df, val_df, best_params)
    
    # Save best parameters and metrics
    save_best_params(best_params, best_mse, best_mae, best_r2)

# Entry point
if __name__ == "__main__":
    data_path = 'path_to_your_data.csv'  # Path to your data file
    main(data_path)
