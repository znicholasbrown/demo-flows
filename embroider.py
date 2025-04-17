from prefect import task, flow
from prefect.futures import wait
import asyncio
from time import sleep
from random import uniform
from prefect import get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect_dask.task_runners import DaskTaskRunner
from embroider_task_servers import thread_needle, choose_fabric, backstitch, french_knot, satin_stitch

@flow(retries=1, task_runner=DaskTaskRunner())
async def embroider():
    states = []

    tasks = [
        thread_needle,
        choose_fabric,
        backstitch,
        french_knot,
        satin_stitch
    ]

    for task_to_submit in tasks:
        states.append(task_to_submit.submit())

    resolved_states = wait(states)

    for state in resolved_states[0]:
        print(state)
    

if __name__ == "__main__":
    embroider.serve(
        name='embroidery-service'
    )