from prefect import flow, task, get_run_logger
from time import sleep
from random import uniform

@task
def Node(sleep_seconds_range: tuple[float, float] = 0.1):
    logger = get_run_logger()
    logger.info('Node is going to sleep')
    sleep_time = uniform(*sleep_seconds_range)
    sleep(sleep_time)
    logger.info("Node is awake!")


@flow
def process_node_map(sleep_seconds_range_per_node: tuple[float, float] = (1.0, 5.0), number_nodes: int = 150):
    Node.map([sleep_seconds_range_per_node for _ in range(number_nodes)]).wait()


if __name__ == '__main__':
    process_node_map(sleep_seconds_range_per_node=(5.0, 15.0), number_nodes=20)