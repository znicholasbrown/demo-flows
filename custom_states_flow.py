import time
from typing import Literal
from prefect import flow, task
from prefect.states import Completed, Failed, State


StateType = Literal[
    "Success", "Validated", "Processed", "Finalized",
    "Error", "Invalid", "Timeout", "Rejected"
]


@task
def custom_state_task(state_type: StateType) -> None | State:
    """
    Task that returns one of 8 custom states based on the state_type parameter.

    Custom Completed states: Success, Validated, Processed, Finalized
    Custom Failed states: Error, Invalid, Timeout, Rejected
    """
    state_configs: dict[str, State] = {
        # Custom Completed states
        "Success": Completed(name="Success", message="Task completed successfully"),
        "Validated": Completed(name="Validated", message="Data validation passed"),
        "Processed": Completed(name="Processed", message="Processing completed"),
        "Finalized": Completed(name="Finalized", message="Task finalized"),

        # Custom Failed states
        "Error": Failed(name="Error", message="An error occurred during execution"),
        "Invalid": Failed(name="Invalid", message="Invalid input data detected"),
        "Timeout": Failed(name="Timeout", message="Task execution timed out"),
        "Rejected": Failed(name="Rejected", message="Task was rejected"),
    }

    # Set the task run state to the selected custom state
    state = state_configs[state_type]
    return state

@flow
def custom_states_flow(iterations: int = 1, pause_seconds: int = 5):
    """
    Flow that demonstrates 8 different custom state names.

    Args:
        iterations: Number of batches to run (default: 1)
        pause_seconds: Seconds to wait between each batch (default: 5)
    """
    all_states = ["Success", "Validated", "Processed", "Finalized",
                  "Error", "Invalid", "Timeout", "Rejected"]

    for iteration in range(iterations):
        print(f"Running iteration {iteration + 1} of {iterations}")

        # Submit all 8 tasks for this iteration
        for state_name in all_states:
            custom_state_task(state_name)

        # Pause between iterations (except after the last one)
        if iteration < iterations - 1:
            print(f"Pausing for {pause_seconds} seconds...")
            time.sleep(pause_seconds)

    print(f"Flow completed! Ran {len(all_states) * iterations} tasks total.")


if __name__ == "__main__":
    # Example usage
    custom_states_flow(iterations=2, pause_seconds=3)