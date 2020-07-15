import prefect
from prefect import Flow, Task
import time
import random
from datetime import timedelta, timezone, datetime
from prefect.schedules import IntervalSchedule
from prefect.environments.storage import Docker


class Version(Task):
    def run(self):
        self.logger.info(f"Running on Prefect v{prefect.__version__}")
        return

class Root(Task):
    def run(self):
        self.logger.info('Root running...')
        time.sleep(5)
        self.logger.info('Root complete.')
        return list(range(random.randint(1, 10)))

class Node(Task):
    def run(self):
        self.logger.info(f'{self.name} running...')
        time.sleep(5)
        return list(range(random.randint(1, 10)))

schedule = IntervalSchedule(interval=timedelta(minutes=30))
with Flow("No Dependencies Flow", schedule=schedule) as No_Dependencies_Flow:
    root = Root()
    version = Version()()
    node1_1 = Node(name="Node 1_1")()
    node1_2 = Node(name="Node 1_2")()
    node1_3 = Node(name="Node 1_3")()
    node1_4 = Node(name="Node 1_4")()
    node1_5 = Node(name="Node 1_5")()
    node1_6 = Node(name="Node 1_6")()
    node1_7 = Node(name="Node 1_7")()
    node1_8 = Node(name="Node 1_8")()
    node1_9 = Node(name="Node 1_9")()
    node1_10 = Node(name="Node 1_10")()
    node1_11 = Node(name="Node 1_11")()
    node1_12 = Node(name="Node 1_12")()
    node1_13 = Node(name="Node 1_13")()
    node1_14 = Node(name="Node 1_14")()
    node1_15 = Node(name="Node 1_15")()
    node1_16 = Node(name="Node 1_16")()


    
No_Dependencies_Flow.storage = Docker(base_image="python:3.7",
    python_dependencies=[],
    registry_url="znicholasbrown",
    image_name="prefect_flow",
    image_tag="no-dependencies-flow")

No_Dependencies_Flow.register(project_name='Flow Schematics')