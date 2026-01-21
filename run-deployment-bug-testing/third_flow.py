"""
Third flow - another simple processing flow.
"""

from prefect import flow, task
import time


@task
def process_third_data(data: str) -> str:
    """Process data in the third flow."""
    print(f"Third flow processing: {data}")
    time.sleep(1)
    return f"Third flow processed: {data}"


@flow(name="run-deployment-testing-third", log_prints=True)
def third_flow(message: str = "Hello from third flow!"):
    """
    Third flow - performs simple data processing.
    
    Args:
        message: A message to process
    """
    print(f"Third flow started with message: {message}")
    result = process_third_data(message)
    print(f"Third flow completed with result: {result}")
    return result


if __name__ == "__main__":
    # Serve this flow when the script is run directly
    third_flow.serve(
        name="default",
        tags=["bug"],
        description="Third flow for testing run_deployment from task"
    )
