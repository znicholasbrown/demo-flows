"""
Triggers the run-deployment-testing-callee and run-deployment-testing-third
"""

from prefect import flow, task
from prefect.deployments import run_deployment


@task
def trigger_third_flow():
    """
    Task that triggers the third flow's deployment.
    Demonstrates calling run_deployment from within a task.
    """
    print("Task: Triggering run-deployment-testing-third...")
    
    flow_run = run_deployment(
        name="run-deployment-testing-third/default",
        parameters={"message": "Triggered by caller task!"},
        timeout=0  # Don't wait for completion
    )
    
    print(f"Task: Third flow deployment triggered with run ID: {flow_run.id}")
    return flow_run.id


@flow(name="run-deployment-testing-caller", log_prints=True)
def second_flow():
    """
    Triggers the run-deployment-testing-callee from flow body
    and run-deployment-testing-third from a task.
    """
    print("Triggering run-deployment-testing-callee from flow body...")
    
    # Run the first flow's deployment from the flow body
    # The deployment name format is: flow-name/deployment-name
    flow_run = run_deployment(
        name="run-deployment-testing-callee/default",
        parameters={"message": "Triggered by caller flow!"},
        timeout=0  # Don't wait for completion
    )
    
    print(f"Callee flow deployment triggered with run ID: {flow_run.id}")
    
    # Now trigger the third flow from within a task
    third_run_id = trigger_third_flow()
    
    return {"callee_run_id": flow_run.id, "third_run_id": third_run_id}


if __name__ == "__main__":
    # Serve this flow when the script is run directly
    second_flow.serve(
        name="default",
        tags=["bug"],
        description="Triggers the run-deployment-testing-callee"
    )
