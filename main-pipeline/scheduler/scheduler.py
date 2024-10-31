from prefect import flow

if __name__ == "__main__":
    # Create a deployment from the GitHub repository and specify the entry point
    flow.from_source(
        source="https://github.com/Olivia-Peng/BA882-pipeline_G10.git",  
        entrypoint="main-pipeline/flows/etl-flow.py:combined_pipeline_flow",  # Path to flow in the repo
    ).deploy(
        name="weekly-cdc-pipeline",  # deployment name
        work_pool_name="victor-pool1",  # work pool
        job_variables={
            "env": {"ENVIRONMENT": "production"},
            "pip_packages": ["pandas", "requests"]  # Add any other dependencies your flow requires
        },
        cron="0 23 * * 0",  # Schedule: Runs at 11 PM EST on Sundays
        tags=["production"],
        description="The pipeline to extract, transform, and load CDC data into BigQuery",
        version="1.0.0",
    )
