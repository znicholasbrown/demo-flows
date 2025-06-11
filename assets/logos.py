from prefect import flow, task
from prefect_dask import DaskTaskRunner
from prefect.assets import Asset, AssetProperties, materialize
from asset_prefixes import prefixes
from typing import Literal

@task
def create_assets(dependency_pattern: Literal["fan_in", "fan_out"]) -> list[Asset]:
    prefix_assets = []
    
    for i, prefix in enumerate(prefixes):
        new_asset = Asset(
            key=f"{prefix}://data",
            properties=AssetProperties(
                name=f"{prefix} logo",
                description=f"Logo test for {prefix}",
                owners=["nicholas@prefect.io"],
            )
        )
        prefix_assets.append(new_asset)
        
        if dependency_pattern == "fan_in":
            deps = prefix_assets[:-1] if i > 0 else []
        else:
            level = i.bit_length() - 1
            if level > 0:
                prev_level_start = 2 ** (level - 1) - 1
                prev_level_end = 2 ** level - 1
                deps = prefix_assets[prev_level_start:prev_level_end]
            else:
                deps = []
        
        @materialize(new_asset, asset_deps=deps, by="python")
        def materialize_prefix_asset(prefix_name: str, deps: list[Asset] = None):
            return {
                "prefix": prefix_name,
                "status": "processed",
                "count": len(prefix_name),
                "metadata": {
                    "type": "logo",
                    "processed_at": "2025-06-11",
                    "dependency_pattern": dependency_pattern,
                    "dependencies": [dep.key for dep in (deps or [])],
                }
            }
        
        materialize_prefix_asset.submit(prefix, deps)
    
    return prefix_assets

@flow(task_runner=DaskTaskRunner())
def create_asset_per_logo(dependency_pattern: Literal["fan_in", "fan_out"] = "fan_in"):
    """
    Process prefixes with either fan-in or fan-out dependency patterns.
    
    Args:
        dependency_pattern: Either "fan_in" or "fan_out"
            - fan_in: Each asset depends on the previous one (sequential)
            - fan_out: Each asset depends on the first one (parallel)
    """
    created_assets = create_assets(dependency_pattern)
    return created_assets

if __name__ == "__main__":
    create_asset_per_logo("fan_in")
