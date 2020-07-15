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
        if random.random() > 0.9:
            raise ValueError(f'{self.name} failed :(')
        else:
            self.logger.info(f'{self.name} complete.')
            return list(range(random.randint(1, 10)))

schedule = IntervalSchedule(interval=timedelta(minutes=30))
with Flow("Wide Flow Run", schedule=schedule) as Wide_Flow_Run:
    root = Root()
    version = Version()(upstream_tasks=[root])
    node1_1 = Node(name="Node 1_1").map(upstream_tasks=[root])
    node1_2 = Node(name="Node 1_2").map(upstream_tasks=[root])
    node1_3 = Node(name="Node 1_3").map(upstream_tasks=[root])
    node1_4 = Node(name="Node 1_4").map(upstream_tasks=[root])
    node1_5 = Node(name="Node 1_5").map(upstream_tasks=[root])
    node1_6 = Node(name="Node 1_6").map(upstream_tasks=[root])
    node1_7 = Node(name="Node 1_7").map(upstream_tasks=[root])
    node1_8 = Node(name="Node 1_8").map(upstream_tasks=[root])
    node1_9 = Node(name="Node 1_9").map(upstream_tasks=[root])
    node1_10 = Node(name="Node 1_10").map(upstream_tasks=[root])
    node1_11 = Node(name="Node 1_11").map(upstream_tasks=[root])
    node1_12 = Node(name="Node 1_12").map(upstream_tasks=[root])
    node1_13 = Node(name="Node 1_13").map(upstream_tasks=[root])
    node1_14 = Node(name="Node 1_14").map(upstream_tasks=[root])
    node1_15 = Node(name="Node 1_15").map(upstream_tasks=[root])
    node1_16 = Node(name="Node 1_16").map(upstream_tasks=[root])


    
Wide_Flow_Run.storage = Docker(base_image="python:3.7",
    python_dependencies=[],
    registry_url="znicholasbrown",
    image_name="prefect_flow",
    image_tag="wide-flow-run")

Wide_Flow_Run.register(project_name='Flow Schematics')