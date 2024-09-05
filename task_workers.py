from prefect import task
import asyncio

@task
def corn():
    pass

@task
def green_beans():
    pass

@task
def sweet_peas():
    pass

@task
def tomatoes():
    pass

@task
def potatoes():
    pass

@task
def wheat():
    pass

@task
def soy():
    pass

@task
def rice():
    pass

@task
def barley():
    pass

@task
def oats():
    pass


async def main():
    await asyncio.gather(
        corn.serve(),
        green_beans.serve(),
        sweet_peas.serve(),
        tomatoes.serve(),
        potatoes.serve(),
        wheat.serve(),
        soy.serve(),
        rice.serve(),
        barley.serve(),
        oats.serve()
    )

if __name__ == "__main__":
    asyncio.run(main())