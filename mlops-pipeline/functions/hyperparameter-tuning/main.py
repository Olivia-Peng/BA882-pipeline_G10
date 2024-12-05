import json
import pandas as pd
import uuid
import logging
from itertools import product
from google.cloud import storage
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from datetime import datetime
import functions_framework
import tempfile

## Set up logging
logging.basicConfig(level=logging.INFO)

# Constants
BUCKET_NAME = 'ba882-group-10-mlops'
TRAINING_DATA_PATH = 'training-data'

# SARIMA Parameter Ranges
param_distributions = {
    'p': range(0, 3),  # 0 to 2
    'd': range(0, 2),  # 0 to 1
    'q': range(0, 3),  # 0 to 2
    'P': range(0, 2),  # 0 to 1
    'D': range(0, 2),  # 0 to 1
    'Q': range(0, 2),  # 0 to 1
    's': [4, 12, 52]   # Quarterly, annual, and weekly seasonality
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

# Function to perform grid search for hyperparameter tuning
def run_grid_search(train_df, val_df):
    logging.info("Running grid search for SARIMA hyperparameter tuning...")

    # Generate all combinations of parameters
    param_combinations = list(product(
        param_distributions['p'], param_distributions['d'], param_distributions['q'],
        param_distributions['P'], param_distributions['D'], param_distributions['Q'],
        param_distributions['s']
    ))

    best_params = None
    best_mse = float('inf')

    # Iterate through all parameter combinations
    for params in param_combinations:
        param_set = {
            'p': params[0], 'd': params[1], 'q': params[2],
            'P': params[3], 'D': params[4], 'Q': params[5], 's': params[6]
        }
        try:
            logging.info(f"Testing parameters: {param_set}")

            # Train SARIMA model and calculate metrics
            mse, mae, r2 = train_sarima(train_df, val_df, param_set)

            # Check if this parameter set is the best so far
            if mse < best_mse:
                best_mse = mse
                best_params = param_set
                logging.info(f"New best parameters found: {best_params} with MSE={best_mse}")

        except Exception as e:
            logging.error(f"Failed to fit SARIMA model with parameters {param_set}: {str(e)}")
            continue

    logging.info(f"Grid search completed. Best parameters: {best_params} with MSE={best_mse}")
    return best_params

# Save best parameters and metrics to GCS
def save_best_params(bucket_name, disease_code, model_id, best_params, best_mse, best_mae, best_r2):
    storage_client = storage.Client()
    output_path = f'tunning_results/{disease_code}/{disease_code}_params.json'
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_path)
    
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
    logging.info(f"Saved best parameters to gs://{bucket_name}/{output_path}")

# Function to load data from GCS
def load_data_from_gcs(bucket_name, disease_code):
    storage_client = storage.Client()
    file_path = f"{TRAINING_DATA_PATH}/code-{disease_code}/cdc_occurrences_{disease_code}.csv"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Download the CSV to a temporary file
    with tempfile.NamedTemporaryFile() as temp_file:
        blob.download_to_filename(temp_file.name)
        df = pd.read_csv(temp_file.name)
        df['Date'] = pd.to_datetime(df['Date'])  # Ensure date is in datetime format
    
    return df

# Cloud Function entry point
@functions_framework.http
def sarima_hyperparameter_tuning(request):
    request_json = request.get_json(silent=True)
    
    # Get disease_code from the request
    disease_code = request_json.get('disease_code', '370')
    
    # Generate unique model ID
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    model_id = f'sarima_model_{disease_code}_{timestamp}_{str(uuid.uuid4())}_tuned'
    
    # Load data from GCS
    try:
        df = load_data_from_gcs(BUCKET_NAME, disease_code)
    except Exception as e:
        logging.error(f"Failed to load training data for disease code {disease_code}: {str(e)}")
        return {"error": "Failed to load training data"}, 500

    # Split data
    train_df, val_df = split_data(df)
    
    # Run grid search for hyperparameter tuning
    best_params = run_grid_search(train_df, val_df)

    # Train SARIMA model with best parameters
    best_mse, best_mae, best_r2 = train_sarima(train_df, val_df, best_params)

    # Save the best parameters and metrics
    save_best_params(BUCKET_NAME, disease_code, model_id, best_params, best_mse, best_mae, best_r2)
    
    return {"message": "SARIMA hyperparameter tuning completed", "model_id": model_id}, 200

