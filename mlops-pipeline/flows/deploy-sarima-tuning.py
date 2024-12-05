from prefect import flow
from prefect.events import DeploymentEventTrigger
from prefect.orion.schemas.schedules import CronSchedule

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Olivia-Peng/BA882-pipeline_G10.git",
        entrypoint="mlops-pipeline/flows/sarima_tuning_flow.py:sarima_tuning_flow",
    ).deploy(
        name="sarima-hyperparameter-tuning",
        work_pool_name="victor-pool1",
        job_variables={
            "env": {"ENVIRONMENT": "production"},
            "pip_packages": ["pandas", "requests", "scikit-learn", "statsmodels", "google-cloud-storage"]
        },
        tags=["prod"],
        description="Run SARIMA hyperparameter tuning for specified disease codes every three months.",
        version="1.0.0",
        schedule=CronSchedule(cron="0 0 1 */3 *", timezone="UTC")  # First day of every third month
    )
