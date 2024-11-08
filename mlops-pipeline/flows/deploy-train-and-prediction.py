from prefect import flow
from prefect.events import DeploymentEventTrigger

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Olivia-Peng/BA882-pipeline_G10.git",
        entrypoint="mlops-pipeline/flows/weekly-train-and-prediction.py:weekly_train_and_prediction",
    ).deploy(
        name="weekly-train-and-prediction",
        work_pool_name="victor-pool1",
        job_variables={
            "env": {"ENVIRONMENT": "production"},
            "pip_packages": ["pandas", "requests", "prefect"]
        },
        tags=["prod"],
        description="Pipeline to ensure schema, train SARIMA models, and generate predictions weekly.",
        version="1.0.0",
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "cdc-disease-ml-datasets"}
            )
        ]
    )
