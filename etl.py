from prefect import flow, task
import asyncio
from sys import argv

@task
def extract():
    return [1, 2, 3]

@task
def transform(data):
    return [i * 10 for i in data]

@task
def load(data):
    print("Here's your data: {}".format(data))

@flow
def etl_flow():
    e = extract()
    t = transform(e)
    l = load(t)


async def serve_all():
    etl_flow_server = etl_flow.serve()
    extract_server =  extract.serve()
    transform_server = transform.serve()
    load_server =  load.serve()

    await asyncio.gather(
        etl_flow_server,
        extract_server,
        transform_server,
        load_server
    )

if __name__ == "__main__":
    args = argv[1:]

    if len(args) == 0:
        e = extract()
        t = transform(e)
        l = load(t)

    if '--flow' in args:
        etl_flow()

    if '--submit' in args:
        e = extract.submit()
        t = transform.submit(e)
        l = load.submit(t)
    
    if '--serve' in args:
        asyncio.run(serve_all())

