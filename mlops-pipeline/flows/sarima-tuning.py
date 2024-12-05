# SARIMA Hyperparameter Tuning Job

# imports
import requests
from prefect import flow, task
from prefect.events import DeploymentEventTrigger

# helper function - generic invoker
def invoke_gcf(url: str, payload: dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

@task(retries=2)
def execute_sarima_tuning(disease_code: str):
    """Invoke the SARIMA Hyperparameter Tuning Cloud Function."""
    url = "https://sarima-hyperparameter-tuning-162771833878.us-central1.run.app"
    payload = {"disease_code": disease_code}
    resp = invoke_gcf(url, payload=payload)
    return resp

# the flow
@flow(name="sarima-hyperparameter-tuning", log_prints=True)
def sarima_tuning_flow(disease_code: str = "370"):
    """Execute the SARIMA Hyperparameter Tuning process."""
    # Execute the SARIMA tuning Cloud Function
    result = execute_sarima_tuning(disease_code)
    print(f"Result: {result}")

# Set up for local testing
if __name__ == "__main__":
    sarima_tuning_flow(disease_code="370")
