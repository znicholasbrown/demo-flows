from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dask import DaskTaskRunner
import time
import sys
from datetime import datetime

@task
def long_running_task(task_id: int, duration_minutes: float = 1.0):
    """
    A long-running task that generates logs approximately once per second.

    Args:
        task_id: Identifier for this task instance
        duration_minutes: How many minutes the task should run
    """
    total_seconds = int(duration_minutes * 60)
    logger = get_run_logger()

    logger.info(f"Task {task_id} starting - will run for {duration_minutes} minute(s)")

    for second in range(total_seconds):
        logger.info(f"Task {task_id} - Second {second + 1}/{total_seconds} - {datetime.now().isoformat()}")
        time.sleep(1)

    logger.info(f"Task {task_id} completed after {duration_minutes} minute(s)")
    return f"Task {task_id} finished"

@flow(task_runner=DaskTaskRunner())
def parallel_logging(num_tasks: int = 2, duration_minutes: float = 0.1):
    """
    Runs multiple tasks in parallel using Dask, each generating logs per second.

    Args:
        num_tasks: Number of parallel tasks to run (default: 2)
        duration_minutes: Duration in minutes for each task (default: 1)
    """
    results = []

    for i in range(num_tasks):
        result = long_running_task.submit(i + 1, duration_minutes)
        results.append(result)

    return [r.result() for r in results]

if __name__ == "__main__":
    parallel_logging(num_tasks=3000)
