from prefect import flow, task
from prefect_dask import DaskTaskRunner
from prefect.assets import Asset, AssetProperties
from prefect.context import get_run_context
from prefect.assets import materialize

from assets import elastic_search_db

@materialize(asset=elastic_search_db)
@task
def materialize_elastic_search():
    return {"status": "materialized"}

@task(asset_deps=[elastic_search_db])
def process_and_create_assets(number: int) -> list[Asset]:
    dynamic_assets = []
    
    for i in range(number):
        new_asset = Asset(
            key=f"s3://search-team/documents/document_{i}",
            properties=AssetProperties(
                name=f"Dynamic Asset {i}",
                description=f"Automatically generated asset {i}",
                owners=elastic_search_db.properties.owners,
                url=elastic_search_db.properties.url
            )
        )
        dynamic_assets.append(new_asset)
        
        @materialize(asset=new_asset)
        @task
        def materialize_dynamic_asset(asset_num: int):
            return {"asset_number": asset_num}
        
        materialize_dynamic_asset.submit(i)
    
    ctx = get_run_context()
    ctx.add_asset_metadata(elastic_search_db.key, {"number_of_rows": number})
    
    return dynamic_assets

@flow(task_runner=DaskTaskRunner())
def dynamic_asset_flow(number: int = 3):
    materialize_elastic_search()
    
    created_assets = process_and_create_assets(number)
    
    return created_assets

if __name__ == "__main__":
    dynamic_asset_flow()

