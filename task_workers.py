from prefect import task, flow
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


@task
async def legumes():
    green_beans.submit().wait()
    sweet_peas.submit().wait()
    soy.submit().wait()


@task
async def grains():
    wheat.submit().wait()
    rice.submit().wait()
    barley.submit().wait()
    oats.submit().wait()


@task
async def vegetables():
    corn.submit().wait()
    tomatoes.submit().wait()
    potatoes.submit().wait()
    

@flow
async def produce():
    legumes.submit().wait()
    grains.submit().wait()
    vegetables.submit().wait()


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
        oats.serve(),
        produce()
    )

if __name__ == "__main__":
    asyncio.run(main())