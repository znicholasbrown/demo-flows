from prefect import flow, get_run_logger
from prefect.deployments import deployments
from pathlib import Path

@flow
def versiano_reggiano():
    logger = get_run_logger()
    logger.debug("Ayyyy 🤌🤌")


if __name__ == "__main__":
    versiano_reggiano()
    