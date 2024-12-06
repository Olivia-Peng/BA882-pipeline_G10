from prefect import flow

if __name__ == "__main__":
    # Create a deployment from the GitHub repository and specify the entry point
    flow.from_source(
        source="https://github.com/Olivia-Peng/BA882-pipeline_G10.git",  
        entrypoint="mlops-pipeline/flows/sarima-tuning.py:sarima_tuning_flow",  # Path to flow in the repo
    ).deploy(
        name="sarima-hyperparameter-tuning",  # Deployment name
        work_pool_name="victor-pool1",  # Work pool
        job_variables={
            "env": {"ENVIRONMENT": "production"},
            "pip_packages": ["pandas", "requests", "scikit-learn", "statsmodels", "google-cloud-storage"]  # Required dependencies
        },
        cron="0 0 1 */3 *",  # Schedule: Runs on the first day of every 3rd month at midnight UTC
        tags=["prod"],
        description="Run SARIMA hyperparameter tuning for specified disease codes every three months.",
        version="1.0.0",
    )
