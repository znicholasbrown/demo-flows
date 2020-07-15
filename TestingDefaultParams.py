import prefect
from prefect import Flow, task, Parameter
from prefect.environments.storage import GitHub
from prefect.environments import LocalEnvironment


@task
def print_param(param):
    logger = prefect.context.get("logger")

    logger.info(f"This is the value of the parameter that was passed: {param}")


with Flow("Testing Default Parameters") as flow:
    param_with_no_default = Parameter("config")

    print_param(param_with_no_default)


flow.storage = GitHub(
    repo="znicholasbrown/demo-flows",
    path="TestingDefaultParams.py",
    secrets=["NICHOLAS_GITHUB_ACCESS"],
)

flow.environment = LocalEnvironment(labels=[])

flow.register(project_name="Flow Schematics")
