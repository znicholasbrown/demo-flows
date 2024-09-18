from prefect import task
import asyncio
from time import sleep
from random import uniform
from prefect import get_run_logger
from prefect.artifacts import create_markdown_artifact

def simulate_failure(failure_rate=1):
    logger = get_run_logger()
    failure_chance = uniform(0, 1)
    logger.info(f"Simulating task failure (failure rate: {failure_rate}) - (failure chance: {failure_chance})")
    if failure_chance < failure_rate:
        raise Exception("Failed")

@task(retries=3, retry_jitter_factor=0.2, retry_delay_seconds=2)
async def thread_needle():
    logger = get_run_logger()
    logger.info("Attempting to thread the needle")
    sleep(uniform(0.5, 1.5))
    simulate_failure()
    
    artifact = """
# Needle Threading

- **Needle Type**: Embroidery Needle
- **Size**: 24
- **Eye**: Large
- **Thread**: Silk, 6-strand
    """
    await create_markdown_artifact(
        key="needle-threading",
        markdown=artifact,
        description="Details of needle threading process"
    )
    logger.info("Needle threaded")

@task(retries=2, retry_jitter_factor=0.1, retry_delay_seconds=3)
async def choose_fabric():
    logger = get_run_logger()
    logger.info("Selecting fabric for embroidery")
    sleep(uniform(0.3, 1.0))
    simulate_failure()
    
    artifact = """
# Fabric Selection

- **Type**: Linen
- **Count**: 28-count
- **Color**: Antique White
- **Size**: 12" x 12"
    """
    await create_markdown_artifact(
        key="fabric-selection",
        markdown=artifact,
        description="Details of selected embroidery fabric"
    )
    logger.info("Fabric selected: Linen")

@task(retries=5, retry_jitter_factor=0.3, retry_delay_seconds=1)
async def backstitch():
    logger = get_run_logger()
    logger.info("Performing backstitch")
    sleep(uniform(1.0, 2.0))
    simulate_failure()
    
    artifact = """
# Backstitch Technique

1. Bring needle up at A, down at B
2. Bring needle up at C (halfway between A and B)
3. Insert needle back down at A
4. Continue this pattern for a solid line
    """
    await create_markdown_artifact(
        key="backstitch-technique",
        markdown=artifact,
        description="Step-by-step guide for backstitch"
    )
    logger.info("Backstitch completed")

@task(retries=2, retry_jitter_factor=0.15, retry_delay_seconds=10)
async def french_knot():
    logger = get_run_logger()
    logger.info("Creating French knots")
    sleep(uniform(0.8, 1.8))
    simulate_failure()
    
    artifact = """
# French Knot Technique

1. Bring needle up through fabric
2. Wrap thread around needle 2-3 times
3. Insert needle close to exit point
4. Hold wraps in place and pull needle through
5. Secure knot on back of fabric
    """
    await create_markdown_artifact(
        key="french-knot-technique",
        markdown=artifact,
        description="Instructions for creating French knots"
    )
    logger.info("French knots added to the design")

@task(retries=10, retry_jitter_factor=0.25, retry_delay_seconds=1)
async def satin_stitch():
    logger = get_run_logger()
    logger.info("Applying satin stitch")
    sleep(uniform(1.2, 2.5))
    simulate_failure()
    
    artifact = """
# Satin Stitch Filling

- **Direction**: Work in parallel lines
- **Spacing**: Keep stitches close together
- **Length**: Vary stitch length for curved areas
- **Tip**: Use split stitch outline for crisp edges
    """
    await create_markdown_artifact(
        key="satin-stitch-technique",
        markdown=artifact,
        description="Guide for applying satin stitch"
    )
    logger.info("Satin stitch area filled")

async def main():
    await asyncio.gather(
        thread_needle.serve(),
        choose_fabric.serve(),
        backstitch.serve(),
        french_knot.serve(),
        satin_stitch.serve(),
    )
    

if __name__ == "__main__":
    asyncio.run(main())