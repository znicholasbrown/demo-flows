from prefect import flow, task
from prefect_dask import DaskTaskRunner
from prefect.assets import Asset, AssetProperties
from prefect.context import get_run_context
from prefect.assets import materialize
import sys

from assets import elastic_search_db

@materialize(elastic_search_db)
def materialize_elastic_search():
    return {"status": "materialized"}

@task(asset_deps=[elastic_search_db])
def process_and_create_assets(number: int, depth: int = 1) -> list[list[Asset]]:
    all_layers = []
    
    for layer in range(depth):
        layer_assets = []
        
        for i in range(number):
            # Determine the key based on layer
            layer_suffix = f"_layer_{layer}" if layer > 0 else ""
            new_asset = Asset(
                key=f"s3://search-team/documents/document_{i}{layer_suffix}",
                properties=AssetProperties(
                    name=f"Dynamic Asset {i} Layer {layer}",
                    description=f"Automatically generated asset {i} at layer {layer}",
                    owners=elastic_search_db.properties.owners,
                    url=elastic_search_db.properties.url
                )
            )
            layer_assets.append(new_asset)
            
            # Determine dependencies based on layer
            if layer == 0:
                # First layer depends on elastic_search_db
                deps = [elastic_search_db]
            else:
                # Subsequent layers depend on the corresponding asset from the previous layer
                deps = [all_layers[layer - 1][i]]
            
            @materialize(new_asset, asset_deps=deps, by="python")
            def materialize_dynamic_asset(asset_num: int, layer_num: int):
                return {"asset_number": asset_num, "layer": layer_num}
            
            materialize_dynamic_asset(i, layer)
            materialize_dynamic_asset.submit(i, layer)
        
        all_layers.append(layer_assets)
    
    return all_layers

@flow(task_runner=DaskTaskRunner())
def dynamic_asset_flow(number: int = 3, depth: int = 1):
    materialize_elastic_search()
    
    created_assets = process_and_create_assets(number, depth)
    
    return created_assets

if __name__ == "__main__":
    # get number of nodes from the first non-script argument on the command line
    number_of_nodes = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    depth = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    dynamic_asset_flow(number_of_nodes, depth)

