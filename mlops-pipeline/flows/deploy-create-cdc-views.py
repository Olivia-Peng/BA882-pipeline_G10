from prefect import flow
from prefect.events import DeploymentEventTrigger

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/Olivia-Peng/BA882-pipeline_G10.git",
        entrypoint="mlops-pipeline/flows/create-cdc-views.py:cdc_ml_datasets",
    ).deploy(
        name="cdc-disease-ml-datasets",
        work_pool_name="victor-pool1",
        job_variables={
            "env": {"ENVIRONMENT": "production"},
            "pip_packages": ["pandas", "requests"]
        },
        tags=["prod"],
        description="Pipeline to create CDC disease occurrences ML datasets after data extraction to BigQuery.",
        version="1.0.0",
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "weekly-cdc-pipeline"}
            )
        ]
    )
