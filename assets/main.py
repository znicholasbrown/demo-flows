from prefect import flow, task
from prefect.assets import materialize
from assets import (
    raw_customer_data,
    raw_product_data,
    staged_customer_data,
    staged_product_data,
    customer_segments,
    customer_analytics,
    data_quality_metrics,
    data_catalog
)

@materialize(staged_customer_data, 
             by="fivetran",
            asset_deps=[raw_customer_data])
def stage_customer_data():
    """Stage customer data from raw S3 to Snowflake"""
    return "Customer data staged successfully"

@materialize(staged_product_data,
             by="fivetran",
            asset_deps=[raw_product_data])
def stage_product_data():
    """Stage product data from raw S3 to Snowflake"""
    return "Product data staged successfully"

@flow(name="Data Ingestion")
def ingest_and_stage_data():
    """Flow to ingest and stage raw data"""
    customer_result = stage_customer_data()
    product_result = stage_product_data()
    return {
        "customer_staging": customer_result,
        "product_staging": product_result
    }

@task(asset_deps=[staged_customer_data], by="dbt")
def prepare_customer_features(fail=False):
    """Prepare features for customer segmentation"""
    if fail:
        raise Exception("Failed to prepare features")
    return "Features prepared"

@materialize(customer_segments, by="vertex_ai")
def train_customer_segments(features, fail=False):
    """Train customer segmentation model"""
    if fail:
        raise Exception("Failed to train customer segments")
    return "Model trained successfully"

@flow(name="ML Training")
def train_models(fail=False):
    """Flow to train ML models"""
    features = prepare_customer_features(fail)
    model_result = train_customer_segments(features, fail)
    return {
        "model_training": model_result
    }

@task(asset_deps=[customer_analytics,
                  customer_segments])
def get_analytics(fail=False):
    """Get analytics report using both analytics and ML outputs"""
    if fail:
        raise Exception("Failed to generate analytics report")
    return "Report generated"

@materialize(data_quality_metrics,
            asset_deps=[staged_customer_data,
                        staged_product_data],
            by="great_expectations")
def run_quality_checks(analytics, fail=False):
    """Run quality checks on all key data assets"""
    if fail:
        raise Exception("Failed to run quality checks")
    return "Quality checks completed"

@materialize(data_catalog)
def generate_catalog(analytics, quality):
    """Generate catalog of all data assets"""
    return "Catalog generated successfully"

@flow(name="Analytics and Quality Checks")
def run_analytics_and_quality(fail_analytics=False, fail_quality=False):
    """Flow to run analytics and quality checks"""
    analytics = get_analytics(fail_analytics)
    quality = run_quality_checks(analytics, fail_quality)
    catalog = generate_catalog(analytics, quality)
    
    return {
        "analytics": analytics,
        "quality": quality,
        "catalog": catalog
    }

@flow(name="Main Pipeline")
def run_pipeline(fail_ml=False, fail_analytics=False, fail_quality=False):
    """Main pipeline flow that orchestrates all sub-flows"""
    staging_results = ingest_and_stage_data()
    ml_results = train_models(fail_ml)
    analytics_results = run_analytics_and_quality(fail_analytics, fail_quality)
    
    return {
        "staging": staging_results,
        "ml": ml_results,
        "analytics": analytics_results
    }


if __name__ == "__main__":
    run_pipeline()