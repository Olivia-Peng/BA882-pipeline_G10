## MLOps Pipeline Overview

This MLOps pipeline automates the retrieval, schema setup, hyperparameter tuning, model training, and predictions for CDC disease occurrence data. Structured to ensure timely predictions, the pipeline aligns with CDC’s weekly data release schedule to provide accurate forecasts on disease trends. As of 11/09/24, the pipeline is handling one disease code, but it has been structured to allow more diseases to be easily added and managed in future phases.

---

### Step-by-Step Pipeline Process

#### Functions - Google Cloud Functions

1. **`retrieve-train-data`**
   - **Main Script**: `main.py`
   - **File Location**: `./mlops-pipeline/functions/retrieve-train-data/`
   - **Process**:
     - Retrieves processed CDC disease data from BigQuery and saves it as a CSV file in Google Cloud Storage (GCS).
     - This CSV file serves as the input for downstream model training and prediction workflows.

2. **`schema-setup`**
   - **Main Script**: `main.py`
   - **File Location**: `./mlops-pipeline/functions/schema-setup/`
   - **Process**:
     - Creates and manages the necessary BigQuery tables within the `cdc_data` dataset to store model outputs and metadata.
     - The function establishes the following tables:
       - **`model_runs`**: Stores metadata for each trained model, including identifiers, GCS paths, and timestamps.
       - **`model_metrics`**: Logs evaluation metrics (e.g., MSE, MAE) for each model, linked by `model_id`.
       - **`model_parameters`**: Holds hyperparameters for each model run, ensuring reproducibility.
       - **`predictions`**: Stores weekly forecasts, capturing `model_id`, prediction date, forecasted values, and disease codes.

3. **`hyperparameter-tuning`**
   - **Main Script**: `main.py`
   - **File Location**: `./mlops-pipeline/functions/hyperparameter-tuning/`
   - **Process**:
     - Performs grid search to find the best SARIMA hyperparameters (`p`, `d`, `q`, `P`, `D`, `Q`, `s`) for each disease code.
     - Stores the best parameters as a JSON file in GCS under `tunning_results/{disease_code}/{disease_code}_params.json`.
     - Configured to run periodically every three months via a Prefect deployment.

4. **`trainer`**
   - **Main Script**: `main.py`
   - **File Location**: `./mlops-pipeline/functions/trainer/`
   - **Process**:
     - Trains a SARIMA model for each unique disease code found in the training data stored in GCS.
     - Dynamically retrieves the best hyperparameters for each disease code from the JSON file stored by the `hyperparameter-tuning` function.
     - Skips training for disease codes without best parameter files.
     - Stores trained models, metadata, and metrics in BigQuery and Google Cloud Storage for future predictions.

5. **`predictions`**
   - **Main Script**: `main.py`
   - **File Location**: `./mlops-pipeline/functions/predictions/`
   - **Process**:
     - Loads the latest trained SARIMA model to generate weekly forecasts on disease occurrences.
     - Stores the prediction outputs in BigQuery, allowing for ongoing tracking and analysis of disease trends.

---

#### Flows

1. **`create-cdc-views.py`**
   - **File Location**: `./mlops-pipeline/flows/`
   - **Purpose**: Initializes CDC data views in BigQuery to aid data preprocessing.
   - **Process**:
     - Generates the necessary views to manage and organize CDC data, optimizing it for use in subsequent pipeline steps.

2. **`deploy-create-cdc-views.py`**
   - **File Location**: `./mlops-pipeline/flows/`
   - **Purpose**: Deploys the CDC data view creation functions in BigQuery to support data organization and transformation.
   - **Process**:
     - Creates views in BigQuery for streamlined CDC data ingestion and analysis.
     - Triggered by the main ETL pipeline of the project (./main-pipeline) to set up views needed for data preparation.

3. **`weekly-train-and-prediction.py`**
   - **File Location**: `./mlops-pipeline/flows/`
   - **Purpose**: Schedules weekly model training and predictions in alignment with CDC’s update schedule.
   - **Process**:
     - Configured to execute every Sunday following CDC’s Saturday updates.
     - Triggers the `retrieve-train-data`, `trainer`, and `predictions` functions in sequence, facilitating an end-to-end pipeline workflow.

4. **`deploy-train-and-prediction.py`**
   - **File Location**: `./mlops-pipeline/flows/`
   - **Purpose**: Deploys the model training and prediction pipeline.
   - **Process**:
     - Configures environment settings and dependencies for both model training and prediction processes.
     - This deployment is triggered by `deploy-create-cdc-views.py` to ensure all necessary data views are available prior to training and prediction.

5. **`deploy-hyperparameter-tuning.py`**
   - **File Location**: `./mlops-pipeline/flows/`
   - **Purpose**: Deploys the SARIMA hyperparameter tuning pipeline to Prefect.
   - **Process**:
     - Executes the `hyperparameter-tuning` function every three months to update the best SARIMA hyperparameters for each disease code.
     - Configures Prefect's cron-based scheduling and dependencies for automated execution.

---

#### Deployment Script

- **Script**: `deploy-mlops.sh`
- **File Location**: `./mlops-pipeline/`
- **Purpose**: Automates the deployment of all MLOps functions to Google Cloud.
- **Process**:
  - Uses the `gcloud functions deploy` command to deploy each function:
    - `create_cdc_views`: Sets up views for CDC data. Triggered by running the `deploy-create-cdc-views.py` flow.
    - `mlops_create_schema`: Configures BigQuery schemas to store model runs, metrics, parameters, and predictions.
    - `hyperparameter-tuning`: Deploys the SARIMA hyperparameter tuning function to GCS.
    - `train_sarima_models`: Deploys the SARIMA model training function, now enhanced with dynamic hyperparameter usage.
    - `predict_sarima_models`: Deploys the prediction function, which is triggered weekly to forecast disease occurrences.
  - Configures each function with runtime specifications, memory allocations, and permissions for optimal performance.

---

### Summary of Updates

- **New Hyperparameter Tuning Cloud Function**: Automates SARIMA parameter tuning using grid search and stores the results in GCS.
- **Enhanced Trainer Cloud Function**: Dynamically retrieves and uses the best parameters for each disease code during training.
- **Prefect Deployment for Tuning**: Added a deployment to run the `hyperparameter-tuning` function every three months.

