from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dask import DaskTaskRunner
import time
import sys
from datetime import datetime

@task
def long_running_task(branch_id: int, task_id: int, duration_seconds: int = 1):
    """
    A long-running task that generates logs approximately once per second.

    Args:
        branch_id: Identifier for the parallel branch
        task_id: Identifier for this task within the branch
        duration_seconds: How many seconds the task should run
    """
    logger = get_run_logger()

    logger.info(f"Branch {branch_id}, Task {task_id} starting - will run for {duration_seconds} second(s)")

    for second in range(duration_seconds):
        logger.info(f"Branch {branch_id}, Task {task_id} - Second {second + 1}/{duration_seconds} - {datetime.now().isoformat()}")
        time.sleep(1)

    logger.info(f"Branch {branch_id}, Task {task_id} completed after {duration_seconds} second(s)")
    return f"Branch {branch_id}, Task {task_id} finished"

@flow(task_runner=DaskTaskRunner())
def parallel_sequential(num_parallel: int = 30, num_sequential: int = 20, duration_seconds: int = 10):
    """
    Runs multiple parallel branches, each containing sequential tasks.

    Args:
        num_parallel: Number of parallel branches (default: 2)
        num_sequential: Number of sequential tasks per branch (default: 2)
        duration_seconds: Duration in seconds for each task (default: 5)
    """
    branches = []

    for branch in range(num_parallel):
        branch_tasks = []

        for seq_task in range(num_sequential):
            if seq_task == 0:
                # First task in branch - submit immediately
                result = long_running_task.submit(branch + 1, seq_task + 1, duration_seconds)
            else:
                # Subsequent tasks wait for previous task
                result = long_running_task.submit(branch + 1, seq_task + 1, duration_seconds, wait_for=[branch_tasks[-1]])

            branch_tasks.append(result)

        branches.append(branch_tasks)

    # Wait for all branches to complete
    all_results = []
    for branch_tasks in branches:
        for task_result in branch_tasks:
            all_results.append(task_result.result())

    return all_results

if __name__ == "__main__":
    parallel_sequential()