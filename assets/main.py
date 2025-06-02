from prefect import flow, task
from prefect.assets import materialize
import time
from assets import (
    raw_customer_data,
    raw_product_data,
    staged_customer_data,
    staged_product_data,
    customer_segments,
    customer_analytics,
    data_quality_metrics
)

# @materialize(staged_customer_data, 
#             asset_deps=[raw_customer_data])
# def stage_customer_data():
#     """Stage customer data from raw S3 to Snowflake"""
#     time.sleep(2)
#     return "Customer data staged successfully"

# @materialize(staged_product_data,
#             asset_deps=[raw_product_data])
# def stage_product_data():
#     """Stage product data from raw S3 to Snowflake"""
#     time.sleep(2)
#     return "Product data staged successfully"

# @flow(name="Data Ingestion")
# def ingest_and_stage_data():
#     """Flow to ingest and stage raw data"""
#     customer_result = stage_customer_data()
#     product_result = stage_product_data()
#     return {
#         "customer_staging": customer_result,
#         "product_staging": product_result
#     }

@task(asset_deps=[staged_customer_data])
def prepare_customer_features(fail=False):
    """Prepare features for customer segmentation"""
    if fail:
        raise Exception("Failed to prepare features")
    time.sleep(3)
    return "Features prepared"

@materialize(customer_segments,
            asset_deps=[staged_customer_data])
def train_customer_segments(features, fail=False):
    """Train customer segmentation model"""
    if fail:
        raise Exception("Failed to train customer segments")
    time.sleep(5)
    return "Model trained successfully"

@flow(name="ML Training")
def train_models(fail=False):
    """Flow to train ML models"""
    features = prepare_customer_features(fail)
    model_result = train_customer_segments(features, fail)
    return {
        "model_training": model_result
    }

@task(asset_deps=[
    customer_analytics,
    customer_segments
])
def generate_analytics_report(fail=False):
    """Generate analytics report using both analytics and ML outputs"""
    if fail:
        raise Exception("Failed to generate analytics report")
    time.sleep(2)
    return "Report generated"

@materialize(data_quality_metrics,
            asset_deps=[
                staged_customer_data,
                staged_product_data,
                customer_segments
            ])
def run_quality_checks(fail=False):
    """Run quality checks on all key data assets"""
    if fail:
        raise Exception("Failed to run quality checks")
    time.sleep(3)
    return "Quality checks completed"

@flow(name="Analytics and Quality Checks")
def run_analytics_and_quality(fail_report=False, fail_quality=False):
    """Flow to run analytics and quality checks"""
    report = generate_analytics_report(fail_report)
    quality = run_quality_checks(fail_quality)
    return {
        "analytics": report,
        "quality": quality
    }

@flow(name="Main Pipeline")
def run_pipeline(fail_ml=False, fail_analytics=False, fail_quality=False):
    """Main pipeline flow that orchestrates all sub-flows"""
    # staging_results = ingest_and_stage_data()
    ml_results = train_models(fail_ml)
    analytics_results = run_analytics_and_quality(fail_analytics, fail_quality)
    
    return {
        # "staging": staging_results,
        "ml": ml_results,
        "analytics": analytics_results
    }