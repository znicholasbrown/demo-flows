import prefect
import time
from prefect import Flow, task
from datetime import timedelta, timezone, datetime
from prefect.environments.storage import Docker
from prefect.schedules import IntervalSchedule


@task
def branch1_1():
    print("Branch 1-1 Running")
    time.sleep(10)
    print("Branch 1-1 Finished")


@task
def branch1_2():
    print("Branch 1-2 Running")
    time.sleep(10)
    print("Branch 1-2 Finished")


@task
def branch2_1():
    print("Branch 2-1 Running")
    time.sleep(10)
    print("Branch 2-1 Finished")


@task
def branch2_2():
    print("Branch 2-2 Running")
    time.sleep(10)
    print("Branch 2-2 Finished")


@task
def terminal():
    print("Terminal Running")
    time.sleep(5)
    print("Terminal Finished")


schedule = IntervalSchedule(interval=timedelta(minutes=30))
with Flow("Moving Schematic", schedule=schedule) as flow:

    b1 = branch1_2(upstream_tasks=[branch1_1])
    b2 = branch2_2(upstream_tasks=[branch2_1])
    terminal(upstream_tasks=[b1, b2])


flow.register(project_name="Flow Schematics")
