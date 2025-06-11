import asyncio
from prefect import get_client
from prefect.events import emit_event


async def main():
    async with get_client() as client:
        res =  await client.request("GET", "/assets/")
        res.raise_for_status()
        assets = res.json()
        for e in assets:
            emit_event(
                event="prefect.asset.deleted",
                resource={"prefect.resource.id": e["key"]}
            )


if __name__ == '__main__':
    asyncio.run(main())