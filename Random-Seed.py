import random
import itertools
from prefect import Task, Flow
from prefect.environments.storage import Docker

SEED = 94611
N_LEVELS = 11
MIN_WIDTH = 11
MAX_WIDTH = 11
PROB_EDGE = 0.1
VERSION = 6
random.seed(SEED)

flow = Flow(f"{SEED} Seed Flow {VERSION}")

LEVELS = dict()

for level in range(N_LEVELS):
    width = random.randint(MIN_WIDTH, MAX_WIDTH)
    LEVELS[level] = [Task(name=f"Task {level}-{i}") for i, _ in enumerate(range(width))]
    for task in LEVELS[level]:
        flow.add_task(task)
    if level:
        for a, b in itertools.product(LEVELS[level - 1], LEVELS[level]):
            if random.random() > PROB_EDGE:
                flow.add_edge(a, b)

# flow.storage = Docker(
#     base_image="python:3.8",
#     python_dependencies=[],
#     registry_url="znicholasbrown",
#     image_name=f"random_seed-{VERSION}",
#     image_tag=f"random-seed-flow-{VERSION}",
# )

flow.register(project_name="Community Support Flows")
