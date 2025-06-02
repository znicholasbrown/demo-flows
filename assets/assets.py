from prefect.assets import Asset

# Raw data ingestion assets
raw_customer_data = Asset(
    key="s3://data-team-production/raw/customer_data",
    name="Raw Customer Data",
    description="Unprocessed customer transaction data from source systems",
    owners=["nicholas", "kevin-g"],
    url="https://prefect.io",
    by="fivetran"
)

raw_product_data = Asset(
    key="s3://data-team-production/raw/product_catalog",
    name="Raw Product Catalog",
    description="Source product catalog data including pricing and inventory",
    owners=["nicholas", "kevin-g"],
    url="https://prefect.io",
    by="fivetran"
)

# Staging layer assets
staged_customer_data = Asset(
    key="snowflake://data_team/staging/customer_data",
    name="Staged Customer Data",
    description="Cleaned and standardized customer data ready for transformation",
    owners=["adamprefectio", "jakeprefectio5"],
    url="https://prefect.io",
    by="dbt"
)

staged_product_data = Asset(
    key="snowflake://data_team/staging/product_catalog",
    name="Staged Product Catalog",
    description="Normalized product data with consistent formatting",
    owners=["adamprefectio", "jakeprefectio5"],
    url="https://prefect.io",
    by="dbt"
)

# Core business logic assets
customer_segments = Asset(
    key="bigquery://data-team-ml/customer_segments",
    name="Customer Segmentation Model",
    description="ML-based customer segmentation for targeted marketing",
    owners=["Data science", "chris"],
    url="https://prefect.io",
)

product_recommendations = Asset(
    key="bigquery://data-team-ml/product_recommendations",
    name="Product Recommendation Engine",
    description="Real-time product recommendation model outputs",
    owners=["Data science", "chris"],
    url="https://prefect.io",
    by="vertex_ai"
)

# Analytics layer assets
customer_analytics = Asset(
    key="snowflake://data_team/analytics/customer_insights",
    name="Customer Analytics Dashboard",
    description="Aggregated customer behavior and engagement metrics",
    owners=["nicholas", "kevin-g"],
    url="https://prefect.io",
    by="dbt"
)

sales_forecasting = Asset(
    key="bigquery://data-team-ml/sales_forecast",
    name="Sales Forecasting Model",
    description="Time-series based sales predictions and trend analysis",
    owners=["Data science", "chris"],
    url="https://prefect.io",
)

# Quality assurance assets
data_quality_metrics = Asset(
    key="postgres://data-team-monitoring/quality_metrics",
    name="Data Quality Metrics",
    description="Comprehensive data quality checks and validation results",
    owners=["adamprefectio", "jakeprefectio5"],
    url="https://prefect.io",
    by="great_expectations"
)

# Metadata and documentation assets
data_catalog = Asset(
    key="azure://data-team-metadata/catalog",
    name="Data Catalog",
    description="Centralized metadata repository for all data assets",
    owners=[],
    url="https://prefect.io",
)

# List of all assets for easy reference
all_assets = [
    raw_customer_data,
    raw_product_data,
    staged_customer_data,
    staged_product_data,
    customer_segments,
    product_recommendations,
    customer_analytics,
    sales_forecasting,
    data_quality_metrics,
    data_catalog
]