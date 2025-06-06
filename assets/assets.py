from prefect.assets import Asset, AssetProperties

# Raw data ingestion assets
raw_customer_data = Asset(
    key="s3://data-team-production/raw/customer_data",
    properties=AssetProperties(
        name="Raw Customer Data",
        description="Unprocessed customer transaction data from source systems",
        owners=["nicholas", "kevin-g"],
        url="https://prefect.io",
    )
)

raw_product_data = Asset(
    key="s3://data-team-production/raw/product_catalog",
    properties=AssetProperties(
        name="Raw Product Catalog",
        description="Source product catalog data including pricing and inventory",
        owners=["nicholas", "kevin-g"],
        url="https://prefect.io",
    )
)

# Staging layer assets
staged_customer_data = Asset(
    key="snowflake://data_team/staging/customer_data",
    properties=AssetProperties(
        name="Staged Customer Data",
        description="Cleaned and standardized customer data ready for transformation",
        owners=["adamprefectio", "jakeprefectio5"],
        url="https://prefect.io",
    )
)

staged_product_data = Asset(
    key="snowflake://data_team/staging/product_catalog",
    properties=AssetProperties(
        name="Staged Product Catalog",
        description="Normalized product data with consistent formatting",
        owners=["adamprefectio", "jakeprefectio5"],
        url="https://prefect.io",
    )
)

# Core business logic assets
customer_segments = Asset(
    key="bigquery://data-team-ml/customer_segments",
    properties=AssetProperties(
        name="Customer Segmentation Model",
        description="ML-based customer segmentation for targeted marketing",
        owners=["Data science", "chris"],
        url="https://prefect.io",
    )
)

product_recommendations = Asset(
    key="bigquery://data-team-ml/product_recommendations",
    properties=AssetProperties(
        name="Product Recommendation Engine",
        description="Real-time product recommendation model outputs",
        owners=["Data science", "chris"],
        url="https://prefect.io",
    )
)

# Analytics layer assets
customer_analytics = Asset(
    key="snowflake://data_team/analytics/customer_insights",
    properties=AssetProperties(
        name="Customer Analytics Dashboard",
        description="Aggregated customer behavior and engagement metrics",
        owners=["nicholas", "kevin-g"],
        url="https://prefect.io",
    )
)

sales_forecasting = Asset(
    key="bigquery://data-team-ml/sales_forecast",
    properties=AssetProperties(
        name="Sales Forecasting Model",
        description="Time-series based sales predictions and trend analysis",
        owners=["Data science", "chris"],
        url="https://prefect.io",
    )
)

# Quality assurance assets
data_quality_metrics = Asset(
    key="postgres://data-team-monitoring/quality_metrics",
    properties=AssetProperties(
        name="Data Quality Metrics",
        description="Comprehensive data quality checks and validation results",
        owners=["adamprefectio", "jakeprefectio5"],
        url="https://prefect.io",
    )
)

# Metadata and documentation assets
data_catalog = Asset(
    key="azure://data-team-metadata/catalog",
    properties=AssetProperties(
        name="Data Catalog",
        description="Centralized metadata repository for all data assets",
        owners=[],
        url="https://prefect.io",
    )
)


elastic_search_db = Asset(
    key="elasticsearch://search-team/documents",
    properties=AssetProperties(
        name="Elasticsearch Document DB",
        description="""
A comprehensive document database built on Elasticsearch that serves as the central repository for all searchable content across the organization. This database stores and indexes various types of documents including:

## Document Types
- Product specifications and technical documentation
- Customer support articles and FAQs
- Internal knowledge base entries
- Marketing materials and campaign documentation
- Research papers and whitepapers
- API documentation and developer guides

## Document Structure
Each document in the database follows a standardized schema with the following key fields:
- `document_id`: Unique identifier for each document
- `title`: Document title or name
- `content`: Main body of the document in markdown format
- `metadata`: JSON object containing document-specific metadata
- `tags`: Array of relevant tags for categorization
- `created_at`: Timestamp of document creation
- `updated_at`: Timestamp of last update
- `version`: Document version number
- `author`: Original author information
- `department`: Owning department or team
- `access_level`: Security classification level

## Indexing and Search
The database implements sophisticated full-text search capabilities with:
- Custom analyzers for different languages
- Fuzzy matching for typo tolerance
- Synonym support for industry-specific terminology
- Field boosting for prioritized search results
- Faceted search for filtering and aggregation

## Data Organization
Documents are organized into logical collections based on:
1. Document Type
2. Department
3. Access Level
4. Creation Date
5. Update Frequency

## Performance Optimizations
The database implements several performance optimizations:
- Sharding strategy based on document type and access patterns
- Custom routing rules for efficient query distribution
- Caching layer for frequently accessed documents
- Bulk indexing operations for efficient updates
- Regular index optimization and maintenance

## Security and Access Control
Security is implemented through:
- Role-based access control (RBAC)
- Document-level security policies
- Audit logging for all operations
- Encryption at rest and in transit
- Regular security audits and compliance checks

## Integration Points
The database integrates with:
- Content Management Systems
- Version Control Systems
- CI/CD Pipelines
- Monitoring and Alerting Systems
- Backup and Recovery Systems

## Maintenance and Operations
Regular maintenance tasks include:
- Daily index optimization
- Weekly backup operations
- Monthly capacity planning
- Quarterly performance reviews
- Annual security audits

## Monitoring and Metrics
Key metrics tracked include:
- Query response times
- Indexing latency
- Storage utilization
- Cache hit rates
- Error rates and types
- User activity patterns

## Disaster Recovery
The database implements a comprehensive disaster recovery plan with:
- Regular automated backups
- Cross-region replication
- Point-in-time recovery capabilities
- Failover testing procedures
- Recovery time objectives (RTO) and recovery point objectives (RPO)

## Future Enhancements
Planned improvements include:
- Enhanced machine learning capabilities for better search relevance
- Improved natural language processing
- Advanced analytics and reporting
- Extended API capabilities
- Additional language support

""",
        owners=["Search & Rescue"],
        url="https://prefect.io",
    )
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
    data_catalog,
    elastic_search_db
]