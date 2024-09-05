# task
#  - logs
# subtask
#  - logs
# flow
#  - logs
# subflow
#  - logs
# artifact
#  - image
#  - link
#  - markdown
#  - table
#  - progress
# logs

# TODO: add paused / HITL flow

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_image_artifact, create_link_artifact, create_markdown_artifact, create_table_artifact, create_progress_artifact, update_progress_artifact
import time

@task
def LaunchPad():
    logger = get_run_logger()

    for i in range(10):
        logger.debug(f"Launch in {10 - i} seconds...")
        time.sleep(1)


@task
def OrbitInsert():
    logger = get_run_logger()
    logger.info("Starting data transformation...")

    logger.debug("Transforming:")
    logger.debug("Star coordinates")
    logger.debug("Planet classifications")
    logger.debug("Galaxy types")
    logger.debug("Nebula formations")
    
    logger.info("Data transformation complete")

@task
def StellarMapping():
    logger = get_run_logger()
    logger.info("Starting data modeling...")

    logger.debug("Mapping the following bodies: ")
    logger.debug("Epsilon Eridani")
    logger.debug("Proxima Centauri")
    logger.debug("Alpha Centauri")
    logger.debug("Barnard's Star")

    logger.info("Data modeling complete")

@task
def GravitySling():
    logger = get_run_logger()

    logger.info("Starting data aggregation...")

    logger.debug("Aggregating: ")
    logger.debug("Star masses")
    logger.debug("Planet sizes")
    logger.debug("Galaxy distances")
    logger.debug("Nebula compositions")

    logger.info("Data aggregation complete")


@task
def DeepSpaceScan(sleep_time: float = 0.1):
    progress = 0

    artifact_id = create_progress_artifact(key="deep-space-scan", progress=progress)
    
    logger = get_run_logger()

    logger.info("Sending data for analysis...")

    while progress < 100:
        progress += 10
        update_progress_artifact(artifact_id, progress=progress)
        time.sleep(sleep_time)
    

@flow
def NebulaNavi():
    logger = get_run_logger()
    logger.info("Starting data quality checks...")

    launch = LaunchPad()
    mapping = StellarMapping(wait_for=[launch])
    sling = GravitySling(wait_for=[mapping])

    logger.info("Data quality checks complete")
    
    return sling


@flow
def VoyagerMission():
    logger = get_run_logger()

    logger.info("Starting end-to-end ETL process...")

    launch = LaunchPad()
    insert = OrbitInsert(wait_for=[launch])
    sling = GravitySling(wait_for=[insert])

    create_link_artifact(key="voyager-mission", link="https://voyager.jpl.nasa.gov/", description="Site for Voyager mission details")

    logger.info("End-to-end ETL process complete")

    return sling


@flow
def AsteroidMining(sleep_time: float = 0.1):
    logger = get_run_logger()

    logger.info("Extracting specific data subsets...")

    launch = LaunchPad()
    insert = OrbitInsert(wait_for=[launch])
    mapping = StellarMapping(wait_for=[insert])
    scan = DeepSpaceScan(sleep_time, wait_for=[insert])

    table_data = [
        {"Star": "Epsilon Eridani", "Mass": '0.82', "Distance": '10.5'},
        {"Star": "Proxima Centauri", "Mass": '0.12', "Distance": '4.24'},
        {"Star": "Alpha Centauri", "Mass": '1.1', "Distance": '4.37'},
        {"Star": "Barnard's Star", "Mass": '0.144', "Distance": '5.96'}
    ]

    create_table_artifact(key="cosmic-catalog", table=table_data)

    logger.info("Data extraction complete")

@flow
def GalaxyQuest(sleep_time: float = 0.1, truncate: bool = False):
    progress_artifact_id = create_progress_artifact(key="quest-progress", progress=0)


    logger = get_run_logger()
    logger.info("Starting main data pipeline...")

    launch = LaunchPad()
    
    deep_space = DeepSpaceScan(sleep_time, wait_for=[launch])

    update_progress_artifact(progress_artifact_id, progress=10)

    insert = OrbitInsert(wait_for=[launch])

    update_progress_artifact(progress_artifact_id, progress=20)

    if not truncate:
        mapping = StellarMapping(wait_for=[insert])

        update_progress_artifact(progress_artifact_id, progress=30)

        sling = GravitySling(wait_for=[mapping])

        update_progress_artifact(progress_artifact_id, progress=40)

        nebula = NebulaNavi(wait_for=[sling])

        update_progress_artifact(progress_artifact_id, progress=50)

        voyager = VoyagerMission(wait_for=[sling])

        update_progress_artifact(progress_artifact_id, progress=60)

        mining = AsteroidMining(sleep_time / 2, wait_for=[sling])

        update_progress_artifact(progress_artifact_id, progress=70)

    create_image_artifact(key="teflermine", image_url="https://www.nasa.gov/wp-content/uploads/2024/08/teflermine-oli2-20231215-lrg.jpg")

    update_progress_artifact(progress_artifact_id, progress=80)

    create_image_artifact(key="spiral-galaxy", image_url="https://www.nasa.gov/wp-content/uploads/2024/07/hubble-ngc3430-spiral.jpg")

    update_progress_artifact(progress_artifact_id, progress=90)

    charter = f"""
## Star Charter

This document outlines the classification and mapping of stars in the galaxy.

### Star Types

1. **Main Sequence Stars**
2. **Red Giants**
3. **White Dwarfs**
4. **Neutron Stars**
5. **Black Holes**

### Star Mapping

- **Epsilon Eridani**: Main sequence star
- **Proxima Centauri**: Red dwarf
- **Alpha Centauri**: Binary star system
- **Barnard's Star**: Red dwarf

Lorem ipsum dolor sit amet, consectetur adipiscing elit
"""

    create_markdown_artifact(key="star-charter", markdown=charter)

    logger.info("Main data pipeline complete")

    update_progress_artifact(progress_artifact_id, progress=100)

    return sling


if __name__ == "__main__":
    GalaxyQuest(sleep_time=5.0)