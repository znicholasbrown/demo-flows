import prefect
from prefect import Flow, task, Parameter
from prefect
from prefect.environments.storage import GitHub



@task
def print_param(param):
    logger = prefect.context.get("logger")

    logger.info(f"This is the value of the parameter that was passed: {param}")


with Flow("Testing Default Parameters") as flow:
    param_with_no_default = Parameter("config")

    print_param(param_with_no_default)


flow.storage = GitHub(
    repo="org/repo",
    path="flows/my_flow.py",
    secrets=["GITHUB_ACCESS_TOKEN"] 
)

flow.register(project_name="Flow Schematics")
