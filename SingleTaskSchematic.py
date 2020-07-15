import prefect
import time
from prefect import Flow, task
from datetime import timedelta, timezone, datetime
from prefect.environments.storage import Docker
from prefect.schedules import IntervalSchedule


@task
def terminal():
    print("Terminal Running")
    time.sleep(25)
    print("Terminal Finished")


schedule = IntervalSchedule(interval=timedelta(minutes=1))
with Flow("Single Task Schematic", schedule=schedule) as flow:
    terminal()

print("before")
flow.run()
# flow.register(project_name="Flow Schematics")
print("after")
