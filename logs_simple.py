from prefect import flow, get_run_logger, task
import time

@task
def log_task(task_id: int) -> str:
    """Task that emits different types of logs"""
    logger = get_run_logger()
    
    # Emit one of each log level
    logger.info(f"Task {task_id}: Starting execution")
    logger.debug(f"Task {task_id}: Debug information - processing data")
    logger.warning(f"Task {task_id}: Warning - this is a test warning message")
    logger.critical(f"Task {task_id}: Critical - this is a test critical message")
    
    return f"Task {task_id} completed"

@flow
def logs_simple(n_tasks: int = 5):
    """
    Parameter-configurable execution of n number of tasks.
    Each task emits info, debug, warning, and critical logs.
    Sleeps for 1 second between each task call.
    """
    logger = get_run_logger()
    
    # Log before the task loop
    logger.info(f"Starting execution of {n_tasks} tasks")
    logger.debug("Initializing task execution environment")
    logger.warning("This is a pre-execution warning")
    logger.critical("This is a pre-execution critical message")

    # Execute n tasks with sleep between each
    results: list[str] = []
    for i in range(n_tasks):
        logger.info(f"Executing task {i+1}/{n_tasks}")
        result = log_task(i+1)
        results.append(result)
        
        # Sleep for 1 second between tasks (except after the last one)
        if i < n_tasks - 1:
            time.sleep(1)
    
    # Log after the task loop
    logger.info(f"Completed execution of {n_tasks} tasks")
    logger.debug("Task execution environment cleanup")
    logger.warning("This is a post-execution warning")
    logger.critical("This is a post-execution critical message")
    
    return results

if __name__ == "__main__":
    # Example usage with default parameters
    logs_simple(n_tasks=10)
