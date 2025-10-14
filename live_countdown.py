import time
from prefect import flow, task
from prefect.futures import wait
from prefect._experimental.artifacts.composable import Artifact
from prefect._experimental.artifacts.components import Markdown

@flow
def multi_component_flow():
    flow_artifact = Artifact(key="flow-artifact", description="Multi-component artifact")
    flow_artifact.append(Markdown(markdown="hello from flow"))
    futures = live_countdown.map(flow_artifact, [30, 45, 60])
    wait(futures)


@task
def live_countdown(artifact: Artifact, countdown: int):
    markdown = Markdown(markdown="")
    artifact.append(markdown)

    while countdown > 0:
        markdown.update(markdown=f"Counting down to zero: {countdown}")
        countdown -= 1
        time.sleep(1)
    markdown.update(markdown="Done!")


if __name__ == "__main__":
    multi_component_flow()