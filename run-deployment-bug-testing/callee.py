"""
Performs simple data processing.
"""

from prefect import flow, task
import time


@task
def process_data(data: str) -> str:
    """Process some data."""
    print(f"Processing data: {data}")
    time.sleep(1)
    return f"Processed: {data}"


@flow(name="run-deployment-testing-callee", log_prints=True)
def first_flow(message: str = "Hello from first flow!"):
    """
    Performs a simple data processing task.
    
    Args:
        message: A message to process
    """
    print(f"Processing with message: {message}")
    result = process_data(message)
    print(f"Completed with result: {result}")
    return result


if __name__ == "__main__":
    # Serve this flow when the script is run directly
    first_flow.serve(
        name="default",
        tags=["bug"],
        description="Processes data"
    )
