from prefect import flow, get_run_logger, task
from prefect.deployments import deployments
from pathlib import Path
import time
import random

@task
def knead_dough():
    logger = get_run_logger()
    logger.info("🤌 Kneading the dough with passion...")
    time.sleep(random.uniform(5, 8))
    logger.info("Perfetto! The dough is ready")

@task
def grate_parmesan():
    logger = get_run_logger()
    logger.info("🧀 Grating the finest Parmigiano-Reggiano...")
    time.sleep(random.uniform(2, 4))
    logger.info("Mamma mia! That's some good cheese")

@task
def boil_pasta():
    logger = get_run_logger()
    logger.info("🍝 Boiling the pasta al dente...")
    time.sleep(random.uniform(8, 12))
    logger.info("Pasta is perfectly cooked!")

@task
def make_sauce():
    logger = get_run_logger()
    logger.info("🍅 Simmering the sauce with love...")
    time.sleep(random.uniform(10, 15))
    logger.info("Sauce is ready to make Nonna proud")

@flow
def versiano_reggiano():
    logger = get_run_logger()
    logger.info("Starting the authentic Italian cooking process...")
    
    # Start tasks in parallel
    pasta = boil_pasta()
    sauce = make_sauce()
    
    # Wait for pasta and sauce
    time.sleep(2)  # Let the flavors develop
    
    # Final touches
    knead_dough()
    grate_parmesan()
    
    logger.info("Dinner is served! Buon appetito! 🍝")
    logger.debug("Ayyyy 🤌🤌")

if __name__ == "__main__":
    versiano_reggiano()
    