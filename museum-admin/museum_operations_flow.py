"""
Museum Operations Flow - Comprehensive Asset Dependency Demonstration

This flow demonstrates various asset dependency patterns:
1. Isolated assets (no upstream/downstream)
2. Single upstream, no downstream (leaf nodes)
3. Single downstream, no upstream (root nodes)
4. Multi-upstream, no downstream (aggregation leaf nodes)
5. Multi-downstream, no upstream (fan-out root nodes)
6. Multi-upstream/downstream combinations
7. Complex multi-multi nodes (hub nodes)

Theme: Running an art museum - business and administrative operations
Architecture: Flow of flows pattern with data ingestion, analytics, and reporting layers
"""
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict

from prefect import flow, task, get_client
from prefect.assets import Asset, AssetProperties, materialize
from prefect_dask import DaskTaskRunner
from prefect.states import Completed
from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterName
from prefect.client.schemas.sorting import TaskRunSort

# ============================================================================
# ISOLATED ASSETS (No upstream, no downstream dependencies)
# ============================================================================

annual_budget_upload = Asset(
    key="local://museum/annual-budget-upload",
    properties=AssetProperties(
        name="Annual Budget Upload",
        description="""**Purpose:** Manually uploaded annual budget planning document serving as financial reference data.

**Source:** Finance department Excel workbook uploaded quarterly to local filesystem for archival purposes.

**Format:** XLSX file containing departmental budget allocations, capital expenditure plans, and revenue targets. No automated processing or downstream dependencies - exists purely as reference documentation for auditing and historical tracking.

**Refresh:** Quarterly manual upload by finance team. Not ingested into analytics pipeline.""",
        owners=["nicholas", "crickpettish"],
    )
)

external_weather_data = Asset(
    key="external://weather-api/climate-data",
    properties=AssetProperties(
        name="External Weather API Data",
        description="""**Purpose:** External weather conditions archive for contextual analysis and regulatory compliance documentation.

**Source:** OpenWeatherMap API providing hourly temperature, humidity, and precipitation data for museum location. Collected via REST API calls with 15-minute polling interval.

**Usage:** Archived for potential correlation studies with visitor traffic and climate control system performance. Not currently integrated into active analytics pipelines but retained for ad-hoc analysis and environmental compliance reporting.

**Refresh:** Real-time polling, 15-minute intervals. 90-day retention.""",
        owners=["Data Science"],
    )
)

# ============================================================================
# DATA INGESTION LAYER (Root nodes - no upstream, various downstream)
# ============================================================================

raw_ticket_sales = Asset(
    key="s3://museum-raw/ticket-sales",
    properties=AssetProperties(
        name="Raw Ticket Sales Data",
        description="""**Purpose:** Foundation dataset for visitor analytics, revenue tracking, and attendance forecasting.

**Source:** Square POS system at admission desk, ingested via hourly batch CDC (change data capture) exports to S3.

**Schema:** Transaction-level records including timestamp, ticket type (adult/child/senior/student), admission price, payment method, discount codes applied, visitor zip code (when collected), and transaction ID.

**Volume:** ~500-1,500 transactions daily, 15-20k monthly. Peak periods: weekends, holidays, special exhibitions.

**Downstream Consumers:** Feeds cleaned_ticket_sales → daily_visitor_metrics, revenue_breakdown, visitor_demographics, and education_program_attendance analyses.

**Data Quality:** ~2% records missing zip code, occasional duplicate transactions from POS system timeouts (handled in cleaning layer).

**Refresh:** Hourly S3 landing, 15-minute SLA for availability.""",
        owners=["nicholas", "Data Science"],
    )
)

raw_membership_signups = Asset(
    key="s3://museum-raw/membership-signups",
    properties=AssetProperties(
        name="Raw Membership Signups",
        description="""**Purpose:** Core membership pipeline data driving retention analysis and donor cultivation strategies.

**Source:** Salesforce CRM membership module, extracted nightly via Fivetran connector to S3 raw zone.

**Schema:** Member profile records with contact details, membership tier (Individual/Family/Patron/Benefactor), start/renewal dates, payment history, communication preferences, and demographic attributes (age range, interests).

**Volume:** 50-200 new/renewed memberships weekly, ~25k active members total.

**Downstream Consumers:** Powers cleaned_memberships → member_engagement_scores, visitor_demographics, donor_lifetime_value, and integrated_visitor_profile.

**Business Context:** Membership revenue represents 15-20% of operating budget. Retention analysis critical for strategic planning.

**Refresh:** Daily at 2 AM ET, includes previous day's signups and profile updates.""",
        owners=["crickpettish"],
    )
)

raw_gift_shop_transactions = Asset(
    key="s3://museum-raw/gift-shop-transactions",
    properties=AssetProperties(
        name="Raw Gift Shop Transactions",
        description="""**Purpose:** Retail operations data enabling inventory optimization, revenue diversification, and cross-sell analysis.

**Source:** Shopify POS system, streamed to S3 via webhook integration with 5-minute micro-batches.

**Schema:** Line-item level sales with product SKU, category, quantity, unit price, discounts, payment method, timestamp, and optional membership ID linkage for member discount tracking.

**Volume:** 200-800 transactions daily, average basket size $42. High correlation with exhibition traffic.

**Downstream Consumers:** Feeds cleaned_gift_shop → revenue_breakdown, gift_shop_inventory_turnover, and financial analytics.

**Business Value:** Gift shop contributes 8-12% of non-admission revenue. Data drives product mix optimization and seasonal buying decisions.

**Refresh:** Near real-time streaming, 5-minute micro-batches with daily reconciliation.""",
        owners=["nicholas", "Data Science"],
    )
)

raw_exhibition_checkins = Asset(
    key="s3://museum-raw/exhibition-checkins",
    properties=AssetProperties(
        name="Raw Exhibition Check-ins",
        description="""**Purpose:** Gallery-level visitor movement tracking for exhibition popularity analysis and space planning.

**Source:** RFID badge scanners at gallery entrances, logged to MongoDB then exported hourly to S3.

**Schema:** Event stream with badge ID (anonymized), gallery name, check-in timestamp, check-out timestamp (when available), exhibition name, and floor level.

**Volume:** 5,000-15,000 checkin events daily across 12 galleries. Average 3.2 gallery visits per ticket holder.

**Technical Details:** RFID badges distributed at admission, returned at exit. Badge ID anonymized for privacy compliance (GDPR/CCPA). Incomplete check-outs (missing exit scan) handled via 4-hour timeout rule.

**Downstream Consumers:** Powers exhibition_visits → daily_visitor_metrics, member_engagement_scores, security_incident_summary, and education_program_attendance correlation.

**Business Value:** Drives exhibition curation decisions, gallery layout optimization, and crowd flow management.

**Refresh:** Hourly batch exports, 30-minute processing SLA.""",
        owners=["Data Science"],
    )
)

raw_environmental_sensors = Asset(
    key="s3://museum-raw/environmental-sensors",
    properties=AssetProperties(
        name="Raw Environmental Sensor Readings",
        description="""**Purpose:** Climate monitoring for artwork preservation compliance and conservation risk management.

**Source:** IoT sensor network (Onset HOBO data loggers) streaming telemetry via AWS IoT Core to S3.

**Schema:** Time-series sensor readings with device ID, gallery location, temperature (°F), relative humidity (%), light level (lux), timestamp. 60-second measurement intervals.

**Volume:** 45 sensors across 12 galleries + 3 storage vaults = 64,800 readings/day. ~2M records/month.

**Compliance:** Conservation standards require 68-72°F, 45-55% RH, <150 lux. Automated alerts for threshold violations.

**Downstream Consumers:** Feeds climate_control_logs → conservation_schedule, operations_mart. Critical input for artwork_condition_reports and insurance documentation.

**Business Impact:** Prevents conservation crises that could cost millions in restoration or insurance claims.

**Refresh:** Real-time streaming via IoT Core, 1-minute batch writes to S3.""",
        owners=["Search & Rescue", "nicholas"],
    )
)

raw_security_logs = Asset(
    key="s3://museum-raw/security-logs",
    properties=AssetProperties(
        name="Raw Security System Logs",
        description="""**Purpose:** Facility security monitoring and incident documentation for risk management and insurance compliance.

**Source:** Genetec Security Center unified platform aggregating access control (HID readers), video motion events (Axis cameras), and alarm system triggers. Exported to S3 via Syslog relay.

**Schema:** Event logs with timestamp, event type (access_granted/denied, motion_detected, alarm_triggered), location, badge/credential ID (staff only), device ID, and severity level.

**Volume:** 2,000-5,000 events daily. 90% routine (staff access, scheduled patrols), 10% anomalies requiring review.

**Retention:** 7-year retention for compliance (insurance, legal). Hot storage 90 days, cold storage thereafter.

**Downstream Consumers:** Feeds operations_mart, security_incident_summary. Correlated with exhibition_visits for anomaly detection.

**Business Context:** Required for insurance underwriting and incident investigation. Integration with visitor traffic patterns helps identify unusual activity.

**Refresh:** Near real-time syslog streaming, 1-minute S3 writes.""",
        owners=["Search & Rescue"],
    )
)

raw_conservation_work_orders = Asset(
    key="s3://museum-raw/conservation-work-orders",
    properties=AssetProperties(
        name="Raw Conservation Work Orders",
        description="""**Purpose:** Conservation project tracking enabling preventive maintenance and restoration budget planning.

**Source:** Monday.com project management system used by conservation lab, exported daily via API to S3.

**Schema:** Work order records with artwork ID, condition assessment, conservation actions needed, priority level (urgent/high/medium/low), estimated hours, assigned conservator, due date, and completion status.

**Volume:** 50-150 active work orders, 5-20 new requests weekly. Average 6-8 weeks from request to completion.

**Downstream Consumers:** Feeds conservation_schedule → artwork_condition_reports, artwork_insurance_valuations, operations_mart.

**Business Value:** Enables proactive conservation vs. reactive emergency repairs (10x cost difference). Critical for exhibition planning and loan agreements.

**Data Quality:** Urgent items flagged within 4 hours. Climate-related condition changes trigger automatic work order creation.

**Refresh:** Daily API export at 6 AM ET, includes previous day's updates.""",
        owners=["crickpettish", "nicholas"],
    )
)

raw_artwork_loans = Asset(
    key="s3://museum-raw/artwork-loans",
    properties=AssetProperties(
        name="Raw Artwork Loan Records",
        description="""**Purpose:** Inter-institutional loan tracking for collection management and insurance valuation updates.

**Source:** CollectionSpace museum collections management system, exported weekly via custom ETL script to S3.

**Schema:** Loan agreement records with artwork ID, lending/borrowing institution, loan period (start/end dates), insurance coverage amount, condition report references, shipping logistics, and loan purpose (exhibition, research, conservation).

**Volume:** 20-40 active loans at any time. 5-10 new loans quarterly. Typical loan duration: 3-12 months for exhibitions.

**Downstream Consumers:** Feeds artwork_insurance_valuations (combined with conservation data for appraisal updates).

**Business Context:** Loans generate exhibition revenue and enhance institutional prestige. Insurance values often updated based on loan appraisals ($5M-$50M per artwork). Loan condition reports influence conservation priorities.

**Compliance:** AIC (American Institute for Conservation) and AAM (American Alliance of Museums) standards require detailed documentation.

**Refresh:** Weekly extract on Sundays, covers loan status changes and new agreements.""",
        owners=["crickpettish"],
    )
)

raw_education_programs = Asset(
    key="s3://museum-raw/education-programs",
    properties=AssetProperties(
        name="Raw Education Program Registrations",
        description="""**Purpose:** Educational outreach tracking supporting grant reporting and community engagement metrics.

**Source:** Eventbrite registration platform for public programs, K-12 school tour booking system, exported daily to S3.

**Schema:** Registration records with program type (school tour, workshop, lecture, family day), date/time, attendance count, grade level (school programs), instructor/docent assigned, program fee, and participant contact info (opt-in only).

**Volume:** 50-200 participants weekly. School tours: 20-30 groups/month. Public programs: 5-10 events/month.

**Downstream Consumers:** Feeds education_program_attendance → cross-sell analysis with ticket sales, contributes to integrated_visitor_profile.

**Grant Reporting:** Data critical for NEA, NEH, and state arts council grant compliance. Demonstrates educational mission fulfillment.

**Business Value:** Programs generate modest revenue ($50-200/program) but drive membership conversions (15% of program attendees join within 3 months).

**Refresh:** Daily export at midnight, includes registrations and attendance confirmations from previous day.""",
        owners=["Data Science", "nicholas"],
    )
)

raw_donor_contributions = Asset(
    key="s3://museum-raw/donor-contributions",
    properties=AssetProperties(
        name="Raw Donor Contribution Records",
        description="""**Purpose:** Philanthropic fundraising pipeline powering major donor cultivation and campaign tracking.

**Source:** Blackbaud Raiser's Edge donor management system, extracted nightly via secure SFTP to S3.

**Schema:** Gift records with donor ID, contribution amount, gift type (cash, stock, in-kind), designation (unrestricted, capital campaign, endowment, program-specific), payment method, tribute information, tax receipt status, and solicitation source.

**Volume:** 200-500 gifts annually, ranging from $25 to $5M+. Major gifts ($10k+) represent 80% of contributed revenue.

**Confidentiality:** PII-sensitive data, restricted access, encrypted at rest and in transit. GDPR/CCPA compliant handling.

**Downstream Consumers:** Feeds financial_health_score, donor_lifetime_value models. Critical input for executive_dashboard and financial_reports.

**Business Impact:** Contributions represent 25-35% of operating budget. Predictive modeling enables targeted cultivation strategies for $100M capital campaign.

**Refresh:** Daily extract at 1 AM ET, includes previous day's gifts and pledge payments.""",
        owners=["nicholas", "crickpettish"],
    )
)

# ============================================================================
# FIRST TRANSFORMATION LAYER (Single upstream each)
# ============================================================================

cleaned_ticket_sales = Asset(
    key="snowflake://staging/cleaned-ticket-sales",
    properties=AssetProperties(
        name="Cleaned Ticket Sales",
        description="""**Purpose:** Production-ready ticket transaction dataset with quality validations and customer enrichment.

**Upstream Dependency:** raw_ticket_sales (s3://museum-raw/ticket-sales)

**Transformation Logic:**
- Deduplication: Remove duplicate transactions using transaction_id + timestamp window logic (handles POS timeout retries)
- Validation: Filter invalid records (null transaction_id, negative prices, future timestamps)
- Standardization: Normalize ticket type categories, standardize payment method codes
- Enrichment: Append customer segment classification (first-visit vs. repeat, local vs. tourist based on zip code)
- Derived Fields: Calculate discount_percentage, time_of_day_category (morning/afternoon/evening)

**Quality Metrics:** 98.5% record retention post-cleaning, <0.1% null critical fields, 100% referential integrity.

**Downstream Consumers:** Powers daily_visitor_metrics, revenue_breakdown, visitor_demographics, education_program_attendance, integrated_visitor_profile.

**Storage:** Snowflake staging schema, 24-month retention, partitioned by transaction_date for query optimization.

**Refresh:** Hourly incremental loads aligned with raw data ingestion, 30-minute transformation SLA.""",
        owners=["Data Science"],
    )
)

cleaned_memberships = Asset(
    key="snowflake://staging/cleaned-memberships",
    properties=AssetProperties(
        name="Cleaned Membership Records",
        description="""**Purpose:** Authoritative membership dataset with deduplicated profiles and standardized attributes.

**Upstream Dependency:** raw_membership_signups (s3://museum-raw/membership-signups)

**Transformation Logic:**
- Deduplication: Resolve duplicate profiles using fuzzy name matching + email/address similarity
- Standardization: Map membership tiers to standard hierarchy (Individual → Family → Patron → Benefactor), normalize contact info (phone, address)
- Enrichment: Calculate member_tenure_days, renewal_count, lifetime_value, churn_risk_score
- Data Quality: Fill missing demographic attributes using third-party enrichment (Experian append)
- Derived Fields: membership_status (active/lapsed/pending), days_until_renewal, upgrade_eligible_flag

**Quality Metrics:** 99.2% deduplication accuracy, 95% profile completeness, 100% tier classification.

**Downstream Consumers:** Feeds member_engagement_scores, visitor_demographics, donor_lifetime_value, integrated_visitor_profile.

**Business Rules:** Members with <30 days until expiration flagged for renewal campaigns. Lapsed members (>90 days past expiration) moved to reactivation pool.

**Storage:** Snowflake staging schema, full history maintained with SCD Type 2 (slowly changing dimensions).

**Refresh:** Daily at 3 AM ET following raw data arrival, full refresh with history preservation.""",
        owners=["Data Science", "nicholas"],
    )
)

cleaned_gift_shop = Asset(
    key="snowflake://staging/cleaned-gift-shop",
    properties=AssetProperties(
        name="Cleaned Gift Shop Sales",
        description="""**Purpose:** Analytics-ready retail transaction data with product hierarchy and inventory linkage.

**Upstream Dependency:** raw_gift_shop_transactions (s3://museum-raw/gift-shop-transactions)

**Transformation Logic:**
- Validation: Remove test transactions, filter returns/exchanges to separate dataset, validate price ranges
- Product Mapping: Join to product catalog for category hierarchy (Department → Category → Subcategory)
- Standardization: Normalize discount codes, categorize payment methods (card/cash/member_discount/comp)
- Enrichment: Append product margins, inventory status, seasonality flags, exhibition tie-ins
- Derived Fields: Calculate net_revenue (after discounts), margin_dollars, basket_id (group concurrent transactions)

**Quality Metrics:** 99.7% successful product catalog joins, <0.5% orphaned SKUs, 100% price validation.

**Downstream Consumers:** Powers revenue_breakdown, gift_shop_inventory_turnover, financial analytics.

**Business Logic:** Member discount transactions linked to membership records when member_id available (60% of transactions). Enables cross-sell analysis between admission and retail.

**Storage:** Snowflake staging schema with product dimension star schema design.

**Refresh:** Real-time streaming via Snowpipe from S3, sub-5-minute latency.""",
        owners=["Data Science"],
    )
)

exhibition_visits = Asset(
    key="pg://museum-db/exhibition-visits",
    properties=AssetProperties(
        name="Exhibition Visit Events",
        description="""**Purpose:** Structured gallery traffic dataset enabling exhibition popularity analysis and space utilization optimization.

**Upstream Dependency:** raw_exhibition_checkins (s3://museum-raw/exhibition-checkins)

**Transformation Logic:**
- Sessionization: Calculate dwell time from check-in/check-out pairs, apply 4-hour timeout for unclosed sessions
- Validation: Filter out staff badge IDs, remove test events, validate timestamp ordering
- Enrichment: Append exhibition metadata (curator, opening date, exhibition type, ticket requirement)
- Aggregation: Roll up to visit-level records with gallery sequence, total dwell time, galleries_visited_count
- Derived Fields: Calculate engagement_score (dwell time vs. exhibition average), peak_hour_visit_flag, weekend_visit_flag

**Quality Metrics:** 92% of sessions have valid check-out times (8% timeout), 100% gallery attribution accuracy.

**Downstream Consumers:** Critical input for daily_visitor_metrics, member_engagement_scores, security_incident_summary, education_program_attendance.

**Business Value:** Data drives exhibition curation decisions (average dwell time by exhibition type informs future planning). Peak traffic analysis supports staffing optimization.

**Storage:** PostgreSQL operational database, indexed on badge_id and timestamp for fast queries.

**Refresh:** Hourly batch processing with 1-hour lookback for late-arriving checkout events.""",
        owners=["Data Science", "nicholas"],
    )
)

climate_control_logs = Asset(
    key="pg://museum-db/climate-control",
    properties=AssetProperties(
        name="Climate Control System Logs",
        description="""**Purpose:** Conservation-critical environmental monitoring with automated alerting for preservation compliance.

**Upstream Dependency:** raw_environmental_sensors (s3://museum-raw/environmental-sensors)

**Transformation Logic:**
- Aggregation: Roll up 60-second readings to 15-minute averages (reduces volume by 15x while preserving trend detection)
- Validation: Detect and interpolate sensor failures (flat-line detection, out-of-range values)
- Threshold Monitoring: Apply conservation standards (temp: 68-72°F, humidity: 45-55%, light: <150 lux)
- Alert Generation: Trigger automated alerts for threshold violations lasting >10 minutes
- Enrichment: Append gallery artwork inventory value, environmental zone classification, HVAC zone mapping

**Quality Metrics:** 99.9% sensor uptime, <0.1% interpolated values, 5-minute alert latency.

**Downstream Consumers:** Feeds conservation_schedule (climate alerts trigger work order prioritization), operations_mart, artwork_condition_reports.

**Compliance:** Data provides regulatory documentation for insurance audits and loan agreements. Conservation standards based on AIC guidelines.

**Business Impact:** Proactive alerting prevents conservation crises. 2022 humidity spike detection saved estimated $2.3M in potential restoration costs.

**Storage:** PostgreSQL time-series extension (TimescaleDB) for efficient time-range queries, 7-year retention.

**Refresh:** Real-time processing from S3 stream, 5-minute micro-batches with immediate alert evaluation.""",
        owners=["Search & Rescue", "nicholas"],
    )
)

# ============================================================================
# SECOND TRANSFORMATION LAYER (Multi-upstream combinations)
# ============================================================================

daily_visitor_metrics = Asset(
    key="snowflake://analytics/daily-visitor-metrics",
    properties=AssetProperties(
        name="Daily Visitor Metrics",
        description="""**Purpose:** Core operational dashboard dataset combining admission and gallery traffic for daily performance monitoring.

**Upstream Dependencies:**
- cleaned_ticket_sales (snowflake://staging/cleaned-ticket-sales)
- exhibition_visits (pg://museum-db/exhibition-visits)

**Transformation Logic:**
- Aggregation: Daily rollup of visitor counts, revenue, and gallery traffic
- Join Logic: Link ticket transactions to exhibition visits via timestamp + visitor segmentation
- Calculations: attendance_count, admission_revenue, avg_revenue_per_visitor, gallery_visits_per_ticket, peak_hour_traffic
- Comparisons: Day-over-day, week-over-week, year-over-year change metrics
- Segmentation: Break down by visitor type (adult/child/senior/member), day of week, hour of day

**Key Metrics:**
- Total daily attendance (target: 800-1,200 visitors)
- Revenue per visitor (benchmark: $22-28)
- Gallery engagement rate (visits per ticket holder, target: 3.0+)
- Peak hour concentration (identifies crowding issues)

**Downstream Consumers:** Feeds integrated_visitor_profile, visitor_analytics_mart. Primary dataset for operations team daily standup.

**Business Value:** Early warning system for attendance trends. Drives dynamic pricing decisions and marketing campaign effectiveness measurement.

**Storage:** Snowflake analytics schema with materialized daily aggregations for dashboard performance.

**Refresh:** Hourly incremental processing, final daily metrics locked at midnight +1 hour.""",
        owners=["Data Science", "nicholas"],
    )
)

revenue_breakdown = Asset(
    key="snowflake://analytics/revenue-breakdown",
    properties=AssetProperties(
        name="Revenue Source Breakdown",
        description="""**Purpose:** Comprehensive revenue attribution analysis across all income streams for financial planning and forecasting.

**Upstream Dependencies:**
- cleaned_ticket_sales (snowflake://staging/cleaned-ticket-sales)
- cleaned_gift_shop (snowflake://staging/cleaned-gift-shop)

**Transformation Logic:**
- Revenue Categorization: Classify into admission_revenue, retail_revenue, member_discounts (negative revenue), special_events
- Attribution: Link retail purchases to admission tickets via timestamp proximity (30-minute window)
- Calculations: total_revenue, revenue_mix_percentages, cross_sell_rate (retail attach rate to admission)
- Trend Analysis: Moving averages (7-day, 30-day), seasonality adjustments, anomaly detection
- Derived Metrics: revenue_per_transaction, average_basket_size, discount_impact, payment_method_distribution

**Key Business Metrics:**
- Admission revenue: Typically 70-75% of daily earned revenue
- Retail revenue: Target 20-25% of earned revenue
- Cross-sell rate: Goal 45%+ of ticket holders make retail purchase
- Member discount impact: Track revenue foregone vs. member retention value

**Downstream Consumers:** Feeds revenue_forecasts, financial_health_score, financial_analytics_mart, executive_dashboard.

**Business Impact:** Informs pricing strategy, product mix decisions, membership discount optimization. Critical input for monthly board financial reporting.

**Storage:** Snowflake analytics schema with daily grain, 60-month retention for seasonal trend analysis.

**Refresh:** Hourly for real-time revenue monitoring, daily reconciliation with payment processor.""",
        owners=["nicholas", "crickpettish"],
    )
)

member_engagement_scores = Asset(
    key="snowflake://analytics/member-engagement",
    properties=AssetProperties(
        name="Member Engagement Scores",
        description="""**Purpose:** Predictive member health scoring enabling proactive retention campaigns and upgrade targeting.

**Upstream Dependencies:**
- cleaned_memberships (snowflake://staging/cleaned-memberships)
- exhibition_visits (pg://museum-db/exhibition-visits)

**Transformation Logic:**
- Member Activity Linking: Join membership records to gallery visits using member_id from badge assignments
- Behavioral Scoring: Calculate engagement_score (0-100) based on visit frequency, gallery diversity, program participation, guest brings
- Segmentation: Classify members as highly_engaged (score 75+), moderately_engaged (50-74), at_risk (<50)
- Trend Detection: Track engagement trajectory (improving/stable/declining) using 90-day rolling windows
- Predictive Modeling: Churn risk probability using logistic regression on engagement trends + tenure
- Derived Metrics: visits_per_month, avg_dwell_time, exhibition_preferences (art period/style affinities), guest_bring_rate

**Scoring Model:**
- Visit frequency: 40% weight (benchmark: 2+ visits/month for highly engaged)
- Gallery diversity: 20% weight (visiting across multiple exhibitions vs. single preference)
- Program participation: 15% weight (lectures, tours, members-only events)
- Dwell time: 15% weight (indicator of content appreciation)
- Guest brings: 10% weight (proxy for advocacy and word-of-mouth value)

**Key Segments:**
- Highly engaged (25% of members): Upgrade targets, ambassador program candidates
- Moderately engaged (50%): Stable base, program cross-sell opportunities
- At-risk (25%): Retention campaign targets, win-back offers

**Downstream Consumers:** Feeds visitor_retention_analysis, integrated_visitor_profile, visitor_analytics_mart. Primary input for CRM retention campaigns.

**Business Value:** Enables targeted retention spending. $50 retention campaign to at-risk members generates $175 average lifetime value recovery. Model accuracy: 78% precision for churn prediction.

**Storage:** Snowflake analytics schema, member-level grain with monthly snapshots for trend analysis.

**Refresh:** Daily score updates, full model retrain quarterly with latest behavioral data.""",
        owners=["Data Science"],
    )
)

visitor_demographics = Asset(
    key="pg://museum-db/visitor-demographics",
    properties=AssetProperties(
        name="Visitor Demographic Profiles",
        description="""**Purpose:** Unified demographic dataset combining transactional and membership data for audience understanding and marketing segmentation.

**Upstream Dependencies:**
- cleaned_ticket_sales (snowflake://staging/cleaned-ticket-sales)
- cleaned_memberships (snowflake://staging/cleaned-memberships)

**Transformation Logic:**
- Record Consolidation: Union ticket holder demographics (zip code, ticket type as age proxy) with member profiles (full demographic data)
- Deduplication: Identify member ticket purchases, avoid double-counting in audience metrics
- Geographic Enrichment: Append census data to zip codes (median income, education, population density)
- Segmentation: Create audience segments (local/tourist, age bands, income quartiles, visit frequency tiers)
- Derived Fields: Calculate distance_to_museum, market_penetration_rate (visitors per capita by zip), repeat_visitor_flag

**Data Completeness:**
- Geographic data: 85% of ticket sales (15% missing zip code)
- Age demographic: 95% (derived from ticket type + member records)
- Income proxy: 82% (via zip code enrichment)
- Education level: 60% (member self-reporting only)

**Key Demographics:**
- Age: 18-34 (28%), 35-54 (42%), 55+ (30%)
- Geography: Local <50mi (58%), regional 50-200mi (27%), tourist 200mi+ (15%)
- Income: <$50k (18%), $50-100k (32%), $100-150k (28%), $150k+ (22%)

**Downstream Consumers:** Feeds integrated_visitor_profile, visitor_analytics_mart, executive_dashboard. Primary dataset for marketing persona development and campaign targeting.

**Business Value:** Enables ZIP code level marketing ROI analysis. Identified underserved high-income neighborhoods, led to targeted campaign generating $450k incremental membership revenue.

**Storage:** PostgreSQL analytics database, visitor-level grain with privacy controls (PII restricted access).

**Refresh:** Daily incremental updates from both upstream sources, monthly geographic enrichment refresh.""",
        owners=["Data Science", "nicholas"],
    )
)

conservation_schedule = Asset(
    key="pg://museum-db/conservation-schedule",
    properties=AssetProperties(
        name="Conservation Work Schedule",
        description="""**Purpose:** Dynamic conservation prioritization system combining curatorial requests with environmental risk factors.

**Upstream Dependencies:**
- raw_conservation_work_orders (s3://museum-raw/conservation-work-orders)
- climate_control_logs (pg://museum-db/climate-control)

**Transformation Logic:**
- Risk Assessment: Integrate climate alerts (humidity spikes, temperature instability) with artwork condition reports
- Priority Scoring: Calculate urgency_score (0-100) based on curator priority + environmental risk + artwork value + exhibition schedule
- Resource Allocation: Assign conservators based on specialization (paintings, textiles, sculpture) and estimated hours
- Scheduling: Optimize sequence to minimize gallery closures, respect exhibition loan deadlines
- Alert Correlation: Auto-escalate work orders when climate events affect already vulnerable artworks

**Priority Scoring Model:**
- Curator urgency flag: 30% weight (urgent/high/medium/low)
- Environmental risk: 25% weight (recent climate alerts in artwork location)
- Artwork insurance value: 20% weight (high-value pieces prioritized for risk mitigation)
- Exhibition schedule: 15% weight (works in upcoming exhibitions get priority)
- Condition severity: 10% weight (stable vs. deteriorating)

**Key Outputs:**
- Weekly work schedule with conservator assignments
- Resource bottleneck identification (when demand exceeds capacity)
- Gallery closure calendar (conservation requiring artwork removal)
- Budget burn rate tracking (labor hours + materials)

**Downstream Consumers:** Feeds artwork_condition_reports, operations_mart, conservation_tracking_dashboard. Primary tool for conservation lab project management.

**Business Value:** Optimizes scarce conservation resources ($850k annual lab budget). Prevents emergency conservation (3-5x cost premium). Climate-driven auto-escalation prevented major damage in 2023 humidity event.

**Storage:** PostgreSQL operational database with daily snapshots for audit trail and schedule history.

**Refresh:** Daily priority recalculation at 6 AM ET, real-time updates when climate alerts trigger work order changes.""",
        owners=["crickpettish", "nicholas"],
    )
)

# ============================================================================
# THIRD TRANSFORMATION LAYER (Single downstream from previous layer)
# ============================================================================

visitor_retention_analysis = Asset(
    key="snowflake://analytics/visitor-retention",
    properties=AssetProperties(
        name="Visitor Retention Analysis",
        description="""**Purpose:** Advanced cohort-based retention modeling driving membership renewal campaigns and lifetime value optimization.

**Upstream Dependency:** member_engagement_scores (snowflake://analytics/member-engagement)

**Transformation Logic:**
- Cohort Construction: Group members by join month, creating monthly cohorts for longitudinal analysis
- Retention Calculation: Track renewal rates by cohort at 3, 6, 9, 12, 18, 24 month intervals
- Churn Prediction: Gradient boosting classifier trained on engagement scores + demographic features
- Lifetime Value Modeling: Project member LTV using retention curves + upgrade probability + ancillary spend
- Intervention Analysis: A/B test results from retention campaigns, calculate incremental retention lift

**Predictive Models:**
- Churn Model: XGBoost classifier, 78% precision, 72% recall, trained on 36 months historical data
- Feature Importance: Top predictors are engagement trend (35%), days since last visit (22%), tenure (18%), program participation (15%)
- Model Outputs: Churn probability score, recommended intervention timing, predicted LTV impact of retention

**Key Metrics:**
- 12-month retention rate: Current 76% (benchmark: 72% industry average)
- High-engagement member retention: 89%
- At-risk member retention: 42% (target: 55% with intervention campaigns)
- Retention campaign ROI: $4.20 revenue per $1 spent on targeted outreach

**Business Insights:**
- Members who visit 3+ times in first 90 days show 88% retention vs. 52% for 0-1 visits (drives onboarding strategy)
- Engagement cliff at 6-month mark (visit frequency drops 40%) triggers mid-cycle engagement campaign
- Upgrade to higher tiers shows 95% retention (vs. 76% base), informs upgrade incentive programs

**Downstream Consumers:** Feeds visitor_analytics_mart, executive_dashboard. Primary input for Salesforce CRM retention workflows and marketing automation.

**Business Value:** Retention campaign targeting using churn model delivers $280k incremental membership revenue annually. Early intervention (90-day risk window) shows 2.3x higher save rate than late-stage (30-day) campaigns.

**Storage:** Snowflake analytics schema, member-level predictions updated daily, cohort metrics calculated monthly.

**Refresh:** Daily churn score updates for active members, monthly cohort analysis refresh, quarterly model retrain.""",
        owners=["Data Science"],
    )
)

revenue_forecasts = Asset(
    key="snowflake://analytics/revenue-forecasts",
    properties=AssetProperties(
        name="Revenue Forecast Models",
        description="""**Purpose:** Statistical forecasting system providing quarterly revenue projections for budget planning and board reporting.

**Upstream Dependency:** revenue_breakdown (snowflake://analytics/revenue-breakdown)

**Transformation Logic:**
- Time Series Decomposition: Separate revenue into trend, seasonal, and irregular components using STL decomposition
- Seasonal Modeling: Apply seasonal ARIMA models to each revenue stream (admission, retail, membership, programs)
- External Factors: Incorporate special exhibitions schedule, holiday calendar, local events, weather forecasts
- Scenario Analysis: Generate optimistic/base/conservative forecasts with confidence intervals
- Ensemble Approach: Combine Prophet, ARIMA, and gradient boosting forecasts using weighted average

**Forecasting Models:**
- Prophet Model: Handles seasonality and holidays, 12% MAPE (mean absolute percentage error)
- ARIMA(2,1,2): Classical time series, 14% MAPE
- XGBoost: Incorporates external features (exhibitions, weather), 11% MAPE
- Ensemble: Weighted average (0.4 XGBoost + 0.35 Prophet + 0.25 ARIMA), 9.5% MAPE

**Forecast Outputs:**
- Next quarter (Q+1): 85% confidence interval ±8%
- Two quarters (Q+2): 80% confidence interval ±12%
- Three quarters (Q+3): 75% confidence interval ±15%
- Revenue by stream: Admission, retail, membership, programs, special events

**Key Drivers:**
- Special exhibitions: +25-40% attendance during blockbuster shows (e.g., Van Gogh, Tutankhamun)
- Seasonality: Summer peak (June-August: +35% vs. winter), December holiday spike (+20%)
- Economic indicators: Local unemployment rate (negative correlation), tourism stats (positive correlation)
- Marketing spend: 1.8x ROI on digital advertising, 2.1x on partnership programs

**Downstream Consumers:** Feeds financial_health_score, financial_analytics_mart, executive_dashboard. Primary input for annual budget planning and board financial projections.

**Business Value:** Accurate forecasting enables proactive budget adjustments. Q4 2023 forecast triggered early cost reductions, preventing projected $180k budget shortfall. Forecast accuracy improvement from 15% MAPE (manual) to 9.5% (automated) reduced forecast error by 37%.

**Storage:** Snowflake analytics schema with forecast snapshots retained for accuracy tracking and model improvement.

**Refresh:** Monthly forecast generation for upcoming 4 quarters, weekly forecast refresh when actual revenue deviates >10% from projection.""",
        owners=["nicholas", "Data Science"],
    )
)

artwork_condition_reports = Asset(
    key="pg://museum-db/artwork-condition-reports",
    properties=AssetProperties(
        name="Artwork Condition Assessment Reports",
        description="""**Purpose:** Comprehensive artwork preservation documentation linking conservation interventions with environmental monitoring for insurance and loan compliance.

**Upstream Dependency:** conservation_schedule (pg://museum-db/conservation-schedule)



[Additional details available in full documentation]""",
        owners=["crickpettish"],
    )
)

# ============================================================================
# ADDITIONAL TRANSFORMATION NODES
# ============================================================================

gift_shop_inventory_turnover = Asset(
    key="snowflake://analytics/gift-shop-inventory-turnover",
    properties=AssetProperties(
        name="Gift Shop Inventory Turnover Analysis",
        description="""**Purpose:** Retail inventory optimization analytics driving product mix, pricing, and purchasing decisions.

**Upstream Dependency:** cleaned_gift_shop (snowflake://staging/cleaned-gift-shop)

**Transformation Logic:**
- SKU-Level Metrics: Calculate inventory turnover ratio, days of supply, sell-through rate by product
- ABC Classification: Classify products by revenue contribution (A: top 20% revenue, B: next 30%, C: bottom 50%)
- Velocity Analysis: Identify fast-movers (turnover >8x/year), medium (4-8x), slow (<4x), dead stock (0 sales in 90 days)
- Margin Analysis: Calculate gross margin by product, identify high-margin opportunities
- Seasonality: Detect seasonal patterns (holiday cards, exhibition-themed merchandise spikes)

**Key Metrics:**
- Overall turnover: 6.2x annually (benchmark: 4-8x for museum retail)
- Dead stock: 8% of SKUs (target: <5%), representing $45k tied capital
- Fast-movers: 22% of SKUs generate 68% of revenue (classic Pareto distribution)
- Average days of supply: 58 days (target: 45-60 days)

**Optimization Recommendations:**
- Liquidation candidates: 85 SKUs with <2 sales in 6 months, $38k inventory value
- Reorder alerts: 23 fast-movers approaching stockout (days of supply <14)
- Margin opportunity: Increasing prices on low-elasticity items (exhibition catalogs, specialty books) could add $28k annual margin

**Downstream Consumers:** Feeds financial_analytics_mart. Primary tool for retail buyer monthly review meetings.

**Business Value:** Data-driven inventory management reduced dead stock from 12% to 8% (freed $42k working capital). Fast-mover reorder automation prevented 15 stockouts in peak season, protecting $22k revenue.

**Storage:** Snowflake analytics schema with daily SKU snapshots for trend detection, 24-month retention.

**Refresh:** Daily inventory calculations, weekly ABC reclassification, monthly seasonality updates.""",
        owners=["Data Science", "nicholas"],
    )
)

education_program_attendance = Asset(
    key="snowflake://analytics/education-program-attendance",
    properties=AssetProperties(
        name="Education Program Attendance Analytics",
        description="""**Purpose:** Educational program performance tracking and cross-sell analysis with admission revenue.

**Upstream Dependencies:**
- raw_education_programs (s3://museum-raw/education-programs)
- cleaned_ticket_sales (snowflake://staging/cleaned-ticket-sales)

**Transformation Logic:**
- Program Performance: Calculate registration rates, attendance rates, no-show percentages by program type
- Revenue Analysis: Track program fees, calculate cost-per-attendee including instructor labor and materials
- Cross-Sell Linkage: Match program participants to admission tickets using email/name matching (GDPR-compliant hashing)
- Conversion Tracking: Identify program attendees who purchase memberships within 90 days
- Capacity Utilization: Monitor program fill rates, identify over/under-capacity programs

**Key Metrics:**
- Overall program attendance: 850-1,200 participants monthly
- School tours: 78% of educational volume, break-even on direct costs, drive cafeteria/shop revenue
- Public workshops: 15% of volume, 45% gross margin, attract high-value audiences
- Lecture series: 7% of volume, 12% margin, strong membership conversion (22%)

**Cross-Sell Insights:**
- Program-to-admission: 58% of program participants visit museum within 6 months (vs. 12% general population)
- Program-to-membership: 15% convert to membership within 90 days (8x general population conversion)
- School tour ROI: Direct revenue break-even, but families return for paid visits averaging $145 additional revenue per school group

**Downstream Consumers:** Feeds integrated_visitor_profile, visitor_analytics_mart. Primary dataset for education department program planning.

**Business Value:** Analysis revealed lecture series attendees show 22% membership conversion, justifying increased investment in speaker fees. School tour follow-up marketing campaign generated $89k incremental family visit revenue.

**Storage:** Snowflake analytics schema with program-level and participant-level granularity.

**Refresh:** Weekly program performance updates, monthly cross-sell analysis refresh.""",
        owners=["Data Science"],
    )
)

artwork_insurance_valuations = Asset(
    key="pg://museum-db/artwork-insurance-valuations",
    properties=AssetProperties(
        name="Artwork Insurance Valuation Records",
        description="""**Purpose:** Dynamic insurance valuation tracking incorporating loan appraisals and conservation assessments for risk management.

**Upstream Dependencies:**
- raw_artwork_loans (s3://museum-raw/artwork-loans)
- raw_conservation_work_orders (s3://museum-raw/conservation-work-orders)

**Transformation Logic:**
- Valuation Updates: Incorporate loan agreement appraisals (third-party valuations) into insurance records
- Conservation Impact: Adjust valuations based on conservation work (restoration typically maintains value, deterioration reduces)
- Market Trends: Apply art market index adjustments by category (Old Masters, Impressionism, Contemporary, etc.)
- Revaluation Triggers: Flag artworks requiring professional appraisal (5-year cycle, or post-major conservation)
- Risk Assessment: Calculate total insured value by location (galleries, storage, on loan)

**Valuation Framework:**
- Loan Appraisals: Third-party valuations from borrowing institutions (most reliable, updated with each loan)
- Conservation Adjustments: +5-15% for successful restorations, -10-30% for unaddressed deterioration
- Market Indexing: Apply AMMA (Art Market Monitor Annual) category indices for general market movements
- Professional Reappraisal: Required every 5 years or post-conservation >$50k value

**Portfolio Statistics:**
- Total insured value: $850M collection (2,400 works)
- Average work value: $354k
- High-value works (>$10M): 12 pieces, $285M total, require enhanced coverage
- Works needing revaluation: 180 pieces (>5 years since last appraisal)

**Downstream Consumers:** Feeds operations_mart, conservation_tracking_dashboard. Critical input for annual insurance renewals and loan agreements.

**Business Impact:** Systematic valuation tracking identified $42M in under-insured works, preventing potential major coverage gaps. Conservation-driven revaluations improved loan negotiation leverage with partner institutions.

**Storage:** PostgreSQL with full valuation history audit trail, integrated with collection management system.

**Refresh:** Real-time updates when loan agreements finalized or conservation completed, monthly market index adjustments, annual full portfolio review.""",
        owners=["crickpettish", "nicholas"],
    )
)

security_incident_summary = Asset(
    key="snowflake://analytics/security-incident-summary",
    properties=AssetProperties(
        name="Security Incident Summary Reports",
        description="""**Purpose:** Security event intelligence correlating incidents with visitor traffic patterns for risk assessment and staffing optimization.

**Upstream Dependencies:**
- raw_security_logs (s3://museum-raw/security-logs)
- exhibition_visits (pg://museum-db/exhibition-visits)

**Business Value:** Traffic-correlated staffing optimization reduced overtime costs by $78k annually while maintaining security SLAs. Early warning system for unusual patterns prevented two potential theft attempts in 2023.

**Key Risk Metrics:**
- Overall incident rate: 2.8 per 1,000 visitors (industry benchmark: 2-4)
- High-traffic exhibition rate: 4.2 per 1,000 visitors (Mona Lisa effect: blockbusters attract higher-risk crowds)
- Peak hour incidents: 3x baseline rate (2-4 PM weekend crowding)
- Medical incidents: Spike in summer (heat, elderly visitors)



[Additional details available in full documentation]""",
        owners=["Search & Rescue"],
    )
)

donor_lifetime_value = Asset(
    key="redshift://warehouse/donor-lifetime-value",
    properties=AssetProperties(
        name="Donor Lifetime Value Models",
        description="""**Purpose:** Predictive donor analytics enabling major gift cultivation targeting and campaign ROI optimization.

**Upstream Dependencies:**
- raw_donor_contributions (s3://museum-raw/donor-contributions)
- cleaned_memberships (snowflake://staging/cleaned-memberships)

**Business Value:** LTV model identified 28 high-potential donors for $100M capital campaign cultivation, resulting in $12M commitments in first year. Model-driven cultivation strategy improved major gift conversion rate from 8% to 14% (75% lift). ROI: $8.50 raised per $1 spent on targeted cultivation.

**Key Insights:**
- Member-donors give 4.2x more than non-member donors over 5 years
- Event attendance correlation: Each gala attendance increases subsequent annual giving by avg $3,200
- Giving velocity: Donors who increase gifts 2+ consecutive years show 85% probability of major gift ($25k+) within 3 years
- Planned giving prospects: Donors age 70+ with $50k+ lifetime giving and no recent major gifts (42 prospects identified)



[Additional details available in full documentation]""",
        owners=["nicholas", "crickpettish"],
    )
)

# ============================================================================
# COMPLEX HUB NODES (Multi-upstream, multi-downstream)
# ============================================================================

integrated_visitor_profile = Asset(
    key="snowflake://analytics/integrated-visitor-profile",
    properties=AssetProperties(
        name="Integrated 360° Visitor Profile",
        description="""**Purpose:** Master visitor intelligence asset unifying demographic, behavioral, and transactional data across all museum touchpoints into single customer view.

**Upstream Dependencies (3 sources):**
- visitor_demographics (pg://museum-db/visitor-demographics) - Demographic attributes and geographic segmentation
- member_engagement_scores (snowflake://analytics/member-engagement) - Behavioral engagement scoring and visit patterns
- daily_visitor_metrics (snowflake://analytics/daily-visitor-metrics) - Transaction-level visit and spending behavior

**Business Value - Critical Hub Asset:**
This is the museum's "single source of truth" for visitor intelligence, consolidating previously siloed data streams. Enables:



[Additional details available in full documentation]""",
        owners=["Data Science", "nicholas"],
    )
)

financial_health_score = Asset(
    key="snowflake://analytics/financial-health-score",
    properties=AssetProperties(
        name="Museum Financial Health Score",
        description="""**Purpose:** Composite financial intelligence asset synthesizing revenue performance, donor capacity, and forecasting models into single institutional health metric for executive decision-making.

**Upstream Dependencies (3 sources):**
- revenue_breakdown (snowflake://analytics/revenue-breakdown) - Multi-channel revenue attribution and trends
- revenue_forecasts (snowflake://analytics/revenue-forecasts) - Forward-looking revenue projections with confidence intervals
- raw_donor_contributions (s3://museum-raw/donor-contributions) - Philanthropic pipeline and major gift tracking

**Business Value - Strategic Decision Support:**
This hub asset transforms disparate financial data into actionable intelligence:



[Additional details available in full documentation]""",
        owners=["nicholas", "crickpettish"],
    )
)

# ============================================================================
# DATA WAREHOUSE AGGREGATION LAYER (Multi-upstream marts)
# ============================================================================

visitor_analytics_mart = Asset(
    key="redshift://warehouse/visitor-analytics-mart",
    properties=AssetProperties(
        name="Visitor Analytics Data Mart",
        description="""**Purpose:** Enterprise visitor intelligence data warehouse powering all BI tools, dashboards, and self-service analytics for visitor-related insights.

**Upstream Dependencies (3 sources):**
- integrated_visitor_profile (snowflake://analytics/integrated-visitor-profile) - Master 360° visitor profiles with demographics, behavior, and propensity scores
- visitor_retention_analysis (snowflake://analytics/visitor-retention) - Cohort-based retention metrics, churn predictions, and lifetime value models
- daily_visitor_metrics (snowflake://analytics/daily-visitor-metrics) - Operational KPIs for daily attendance, revenue, and gallery traffic

**Business Value - Enterprise Data Warehouse:**
Consolidates visitor data from multiple upstream systems into single-source-of-truth warehouse enabling:



[Additional details available in full documentation]""",
        owners=["Data Science", "nicholas"],
    )
)

financial_analytics_mart = Asset(
    key="redshift://warehouse/financial-analytics-mart",
    properties=AssetProperties(
        name="Financial Analytics Data Mart",
        description="""**Purpose:** Enterprise financial intelligence warehouse consolidating revenue, forecasting, and health metrics for executive reporting and budget planning.

**Upstream Dependencies (3 sources):**
- financial_health_score (snowflake://analytics/financial-health-score) - Composite institutional health metric with component scores
- revenue_forecasts (snowflake://analytics/revenue-forecasts) - Statistical forecasting models for quarterly revenue projections
- revenue_breakdown (snowflake://analytics/revenue-breakdown) - Multi-channel revenue attribution and trend analysis

**Business Value - Strategic Financial Intelligence:**
This mart transformed financial reporting from reactive (2-week lag) to proactive (next-day):



[Additional details available in full documentation]""",
        owners=["nicholas", "crickpettish"],
    )
)

operations_mart = Asset(
    key="redshift://warehouse/operations-mart",
    properties=AssetProperties(
        name="Operations Management Data Mart",
        description="""**Purpose:** Operational intelligence warehouse consolidating facilities, conservation, and security data for day-to-day museum operations management.

**Upstream Dependencies (3 sources):**
- conservation_schedule (pg://museum-db/conservation-schedule) - Prioritized artwork maintenance schedule with resource allocation
- climate_control_logs (pg://museum-db/climate-control) - Environmental monitoring with automated threshold alerting
- raw_security_logs (s3://museum-raw/security-logs) - Facility security events and incident tracking

**Business Value - Operational Excellence:**
This mart enables proactive operations management versus reactive crisis response:



[Additional details available in full documentation]""",
        owners=["Search & Rescue", "crickpettish"],
    )
)

# ============================================================================
# REPORTING LAYER (Leaf nodes - multi-upstream, no downstream)
# ============================================================================

dashboard_health_validator = Asset(
    key="local://museum/dashboard-health-validator",
    properties=AssetProperties(
        name="Dashboard Health Validator",
        description="""**Purpose:** Validation check that monitors the health of dashboard generation by tracking historical task run patterns.

**Behavior:** Uses Prefect SDK to query the last task run of itself. If the previous run succeeded, it throws an error (demonstrating failure after success pattern). If the previous run failed or doesn't exist, it succeeds. This creates an alternating success/failure pattern useful for testing downstream error handling.

**Use Case:** Demonstrates SDK usage for task run introspection and conditional error handling based on historical execution state. Used to validate that downstream dashboards can gracefully handle upstream validation failures.

**Upstream Dependencies:** None (checks its own previous execution)

**Downstream Consumers:** Executive dashboard generation depends on this validation passing. When validation fails, dashboard generation is skipped or uses cached data.

**Pattern:** Stateful validation that enforces alternating run outcomes for robustness testing.

**Refresh:** Runs before each executive dashboard refresh.""",
        owners=["nicholas"],
    )
)

executive_dashboard = Asset(
    key="tableau://dashboards/executive-summary",
    properties=AssetProperties(
        name="Executive Summary Dashboard",
        description="""**Purpose:** Mission-critical C-suite dashboard providing board of directors with comprehensive institutional performance monitoring across visitor and financial metrics.

**Upstream Dependencies:** dashboard_health_validator (validation check), visitor_analytics_mart (visitor intelligence), financial_analytics_mart (financial health, revenue, forecasts)

**Complete Data Lineage:** This terminal asset consolidates 10+ upstream assets plus validation: Raw sources (ticket_sales, memberships, gift_shop, exhibition_checkins, donor_contributions) → Staging (cleaned data) → Analytics (demographics, engagement, revenue_breakdown, forecasts) → Hub (integrated_visitor_profile, financial_health_score) → Marts (visitor/financial warehouses) → **Executive Dashboard (Terminal Leaf)**

**Key Components:**
- Health Score: 0-100 gauge (current: 78 "Good") combining revenue performance, diversification, forecast confidence, donor pipeline, sustainability
- Visitor Analytics: Daily attendance trends, persona distribution, engagement metrics, geographic heat maps, retention cohorts
- Financial Performance: Revenue mix (admission/retail/membership/programs/donations), budget variance waterfall, forecast with confidence intervals
- Strategic Initiatives: $100M capital campaign progress, membership growth, operational efficiency KPIs
- Interactive: 7/30/90/365-day periods, drill-downs to Looker reports, one-click PDF board packages

**Business Value - $820k Annual ROI:**
- Board governance: Eliminated 40 hours/month manual reporting (CFO time savings $180k)
- Proactive management: Q3 2023 health score decline triggered cost reduction, prevented $180k shortfall  
- Strategic decisions: Persona analysis drove $450k facility expansion decision
- Single source of truth: Eliminated conflicting departmental metrics, improved executive alignment

**Audience:** 12 executives (CEO, CFO, CMO, Development Director, committee chairs), 28 board members read-only. Updates every 2 hours, 99.5% uptime SLA.

**Technical:** Tableau Server with direct Redshift connection, <3-second load time, mobile-optimized for iPad board presentations, SSO via Okta with MFA, audit logging.

**Governance:** All metric changes require CFO + CEO approval, quarterly reconciliation with audited financials (99.9% accuracy), SOX-compliant access controls.

**Terminal Leaf Node:** No downstream dependencies. Data flows TO dashboard for human decision-making only.""",
        owners=["nicholas"],
    )
)

operations_dashboard = Asset(
    key="tableau://dashboards/operations-dashboard",
    properties=AssetProperties(
        name="Operations Management Dashboard",
        description="""**Purpose:** Real-time operational command center for facilities, conservation, and security teams monitoring museum operations and regulatory compliance.

**Upstream Dependency:** operations_mart (facilities, conservation, security operational intelligence)

**Data Lineage:** Raw sources (conservation_work_orders, environmental_sensors, security_logs, exhibition_checkins) → Staging (climate_control_logs) → Analytics (conservation_schedule, security_incident_summary, condition_reports) → Warehouse (operations_mart) → **Operations Dashboard (Terminal Leaf)**

**Dashboard Sections:**
- Real-Time Climate: 45 sensors across 12 galleries + 3 vaults, color-coded status (green/yellow/red), 24-hour trends, active alerts, HVAC system status
- Conservation Projects: Kanban board (backlog/in-progress/review/complete), Gantt timeline, conservator utilization, budget burn rate ($850k annual), urgent items
- Security Operations: Live 50-event feed, incident heat map, officer deployment, visitor context correlation, escalation tracking
- Compliance Scorecard: Daily gallery compliance status, climate violations, security incidents, conservation backlog vs 45-day target

**Operational Workflows:**
- Climate alerts trigger automatic PagerDuty notifications to facilities staff mobile devices, dashboard guides troubleshooting
- Conservation lab daily standup reviews kanban, drag-and-drop project assignment
- Security monitors live feed during shifts, drill-down for incident investigation

**Business Value - $650k Annual ROI:**
- Conservation crisis prevention: $400k (2022 humidity spike early detection)
- Insurance savings: $85k (8% premium reduction through documentation quality)
- Labor efficiency: $90k (eliminated 15 hours/week manual data collection)
- Resource optimization: $75k (22% improved conservation throughput, reduced cycle time 8.2→6.4 weeks)

**Users:** Facilities team (8), conservation lab (6), security (12), operations director. Real-time refresh: climate 5-min, security 1-min, conservation 15-min.

**Integrations:** Monday.com bi-directional sync for work orders, PagerDuty for alerting, HVAC building automation system, Genetec security platform.

**Technical:** Tableau Server with live Redshift connection, <5-second refresh, mobile-responsive for in-gallery use, offline mode for critical climate thresholds.

**Terminal Leaf Node:** No downstream dependencies. Consumed by operations teams for real-time decision-making and response.""",
        owners=["Search & Rescue", "crickpettish"],
    )
)

visitor_trends_report = Asset(
    key="looker://reports/visitor-trends",
    properties=AssetProperties(
        name="Visitor Trends Analysis Report",
        description="""**Purpose:** Self-service visitor analytics platform empowering marketing and membership teams with deep-dive trend analysis without technical expertise.

**Upstream Dependency:** visitor_analytics_mart (comprehensive visitor behavior warehouse)

**Data Lineage:** Raw sources (ticket_sales, memberships, exhibition_checkins, education_programs) → Staging (cleaned data) → Analytics (demographics, engagement, daily_metrics, education_attendance) → Hub (integrated_visitor_profile, retention_analysis) → Mart (visitor_analytics_mart) → **Visitor Trends Report (Terminal Leaf)**

**Report Categories (Looker Explores):**
1. Attendance & Traffic: Daily trends, day-of-week/hour heatmaps, special exhibition impact, weather correlation, capacity utilization
2. Demographics & Segmentation: Geographic maps, age/income analysis, 8 visitor personas, local vs tourist, member vs non-member
3. Engagement & Behavior: Gallery popularity, dwell time, visit sequence flows, cross-visitation, program participation, NPS correlation
4. Membership Analytics: Acquisition trends, retention curves, churn analysis, engagement scoring, upgrade pathways, lifetime value
5. Revenue & Conversion: Admission trends, cross-sell rates, conversion funnels, discount impact, payment methods
6. Marketing Performance: Campaign attribution, channel effectiveness, ZIP code ROI, promo code analysis, partnership programs

**Self-Service Features:** 6 pre-built explores, 40+ filter dimensions, 15+ chart types including maps/funnels/cohorts, automated email/Slack delivery, 12 pre-built dashboards, CSV/Excel/PDF export.

**Business Value - $465k Annual ROI:**
- Marketing optimization: $320k (ZIP code targeting, persona-based campaigns 3.2x higher conversion)
- Membership retention: $69k (churn prevention + upgrades, 76%→81% retention via mid-cycle engagement)
- Staff productivity: $76k (78% Data Science support ticket reduction, 3 days→20 min time-to-insight)

**Users:** 42 staff (marketing 12, membership 8, programs 6, exec 4), 250+ weekly queries, 93% adoption rate, NPS score 72.

**Technical:** Looker (Google Cloud), LookML semantic layer, 95% queries <10 seconds, SSH tunnel to Redshift, query result caching, inherits mart hourly refresh.

**Terminal Leaf Node:** No downstream dependencies. Users consume insights for campaign planning, retention strategies, program development.""",
        owners=["Data Science"],
    )
)

financial_reports = Asset(
    key="looker://reports/financial-reports",
    properties=AssetProperties(
        name="Financial Management Reports",
        description="""**Purpose:** Comprehensive financial reporting for CFO, finance team, and budget managers with variance analysis, forecasting, and audit documentation.

**Upstream Dependency:** financial_analytics_mart (revenue, forecasting, health metrics warehouse)

**Data Lineage:** Raw (ticket_sales, gift_shop, memberships, donor_contributions, education_programs) → Staging (cleaned) → Analytics (revenue_breakdown, forecasts, donor_ltv, inventory_turnover) → Hub (financial_health_score) → Mart (financial_analytics_mart) → **Financial Reports (Terminal Leaf)**

**Report Categories:**
1. Revenue Performance: Actual vs budget by stream/department/month, YoY growth, daily tracking, revenue mix trends
2. Forecasting: Quarterly forecasts with confidence intervals, accuracy tracking (9.5% MAPE), scenario analysis (base/optimistic/pessimistic)
3. Financial Health: Health score trending with 5 components, 25 KPIs with targets, liquidity analysis, operating margins
4. Cost & Budget: Department consumption with burn rates, labor vs budgeted FTEs, program P&L, capital expenditure tracking
5. Donor & Development: Major gift pipeline, retention cohorts, $100M campaign tracking, LTV distribution
6. Audit & Compliance: Daily QuickBooks reconciliation (99.9% match), grant compliance (NEA/NEH), Form 990 prep, board packages, lender covenants

**Scheduled Deliverables:** Daily CFO flash (8 AM), weekly executive summary, monthly board finance package (15 reports), quarterly full board package, annual Form 990 worksheets.

**Business Value - $610k Annual ROI:** Audit efficiency $90k (35% external audit reduction, Form 990 prep 80→25 hours), board automation $180k (CFO time savings), proactive management $180k (variance detection prevented shortfalls), grant compliance $120k (renewal rate 75%→92%), cash optimization $40k.

**Users:** CFO, finance team (4), budget managers (8), development (6), board finance committee (5).

**Technical:** Looker LookML, 90% queries <5 seconds, VPN to Redshift, daily QuickBooks reconciliation, 7 AM refresh.

**Compliance:** SOX controls, Form 990 exports, state reporting, grant compliance, lender covenants.

**Terminal Leaf Node:** No downstream dependencies. Consumed for financial analysis, board reporting, audit, compliance.""",
        owners=["nicholas", "crickpettish"],
    )
)

conservation_tracking_dashboard = Asset(
    key="powerbi://dashboards/conservation-tracking",
    properties=AssetProperties(
        name="Conservation Project Tracking Dashboard",
        description="""**Purpose:** Conservation lab project management for artwork preservation, regulatory compliance, and resource planning.

**Upstream:** operations_mart (conservation work, climate, compliance), artwork_condition_reports (assessments, treatment docs)

**Data Lineage:** Raw (conservation_work_orders, environmental_sensors, artwork_loans) → Staging (climate_control_logs) → Analytics (conservation_schedule, insurance_valuations, condition_reports) → Warehouse (operations_mart) → **Conservation Dashboard (Terminal Leaf)**

**Key Features:**
- Projects: Kanban for 200+ annual projects, Gantt timeline, conservator utilization, $850k budget tracking
- Condition: 2,400 artworks (Excellent 35%, Good 42%, Fair 18%, Poor 5%), treatment priority matrix, conservation history with imagery
- Environment: Real-time climate (12 galleries, 3 vaults), 90-day trends, alert history, risk exposure, AIC standards
- Compliance: 28 active loan docs, 180 artworks needing revaluation, AAM accreditation readiness, AIC Code of Ethics
- Predictive: Climate risk scoring (ML), demand forecasting, treatment prioritization algorithms, budget projection (Monte Carlo)
- Planning: Conservator capacity vs workload, backlog analysis (target 45 days, current 58), seasonal exhibition planning

**Business Value - $820k Annual ROI:** Conservation efficiency $180k (throughput +22%, cycle time 8.2→6.4 weeks), risk prevention $400k (early deterioration detection, $2.3M crisis averted), insurance savings $85k (8% premium reduction), regulatory $80k (AAM reaccreditation, loan docs 78%→98%), resource optimization (eliminated conflicts, 40% fewer gallery closures).

**Users:** 8 conservation staff (100% adoption, 45 weekly sessions, NPS 81), operations director, curator, insurance coordinator.

**Integrations:** Monday.com bi-directional work sync, CollectionSpace collections system, real-time climate from operations_mart, DAM for photography, iPad-optimized with offline mode.

**Technical:** Power BI Premium, DirectQuery to Redshift/PostgreSQL, real-time climate (5-min), hourly projects, daily metadata.

**Compliance:** AIC standards, AAM accreditation (2023 exemplary), insurance audits, loan agreement docs, NEH grants, full treatment audit trail.

**Terminal Leaf Node:** No downstream. Consumed by conservation team for project management, regulatory docs, curatorial planning.""",
        owners=["crickpettish"],
    )
)

# ============================================================================
# HELPER FUNCTIONS FOR REALISTIC DATA GENERATION
# ============================================================================

def generate_transaction_data(source: str, count: int) -> Dict:
    """Generate realistic transaction data."""
    return {
        "source": source,
        "records": count,
        "timestamp": datetime.now().isoformat(),
        "revenue": random.randint(1000, 50000),
        "customers": random.randint(50, 500)
    }

def generate_sensor_data(location: str) -> Dict:
    """Generate environmental sensor readings."""
    return {
        "location": location,
        "temperature": round(random.uniform(68.0, 72.0), 1),
        "humidity": round(random.uniform(45.0, 55.0), 1),
        "light_level": random.randint(50, 150),
        "timestamp": datetime.now().isoformat()
    }

def calculate_metrics(data: Dict) -> Dict:
    """Calculate derived metrics."""
    return {
        **data,
        "processed": True,
        "quality_score": round(random.uniform(0.85, 0.99), 2),
        "completeness": round(random.uniform(0.90, 1.0), 2)
    }

# ============================================================================
# ISOLATED ASSET TASKS
# ============================================================================

@materialize(annual_budget_upload, tags=["isolated", "manual-upload", "reference"])
def upload_annual_budget() -> Dict:
    """Isolated asset - no upstream or downstream dependencies."""
    print("📊 Uploading annual budget document...")
    time.sleep(0.5)
    return {
        "fiscal_year": 2024,
        "total_budget": 12500000,
        "upload_timestamp": datetime.now().isoformat(),
        "status": "standalone_reference"
    }

@materialize(external_weather_data, tags=["isolated", "external-api", "reference"])
def fetch_weather_data() -> Dict:
    """Isolated asset - external API data archived for reference."""
    print("🌤️  Fetching external weather data...")
    time.sleep(0.5)
    return {
        "temperature": 72,
        "conditions": "partly_cloudy",
        "timestamp": datetime.now().isoformat(),
        "status": "archived_reference"
    }

# ============================================================================
# DATA INGESTION TASKS (Root nodes - no upstream)
# ============================================================================

@materialize(raw_ticket_sales, tags=["ingestion", "revenue", "visitor-data"])
def ingest_ticket_sales() -> Dict:
    """Root node - no upstream, feeds multiple downstream assets."""
    print("🎫 Ingesting ticket sales from POS system...")
    time.sleep(0.5)
    return generate_transaction_data("pos_system", random.randint(500, 1500))

@materialize(raw_membership_signups, tags=["ingestion", "membership"])
def ingest_membership_signups() -> Dict:
    """Root node - no upstream, feeds member analytics."""
    print("👥 Ingesting membership signups...")
    time.sleep(0.5)
    return generate_transaction_data("membership_portal", random.randint(20, 100))

@materialize(raw_gift_shop_transactions, tags=["ingestion", "revenue", "retail"])
def ingest_gift_shop_transactions() -> Dict:
    """Root node - no upstream, feeds retail analytics."""
    print("🛍️  Ingesting gift shop transactions...")
    time.sleep(0.5)
    return generate_transaction_data("retail_pos", random.randint(200, 800))

@materialize(raw_exhibition_checkins, tags=["ingestion", "visitor-data"])
def ingest_exhibition_checkins() -> Dict:
    """Root node - no upstream, feeds exhibition analytics."""
    print("🖼️  Ingesting exhibition check-ins from RFID scanners...")
    time.sleep(0.5)
    return {
        "source": "rfid_system",
        "checkins": random.randint(300, 1200),
        "exhibitions": ["Impressionism", "Modern Sculpture", "Renaissance Masters"],
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_environmental_sensors, tags=["ingestion", "facilities", "conservation"])
def ingest_environmental_sensors() -> Dict:
    """Root node - no upstream, feeds climate control."""
    print("🌡️  Ingesting environmental sensor readings...")
    time.sleep(0.5)
    return {
        "sensors": [
            generate_sensor_data("Gallery A"),
            generate_sensor_data("Gallery B"),
            generate_sensor_data("Storage Vault")
        ],
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_security_logs, tags=["ingestion", "security", "facilities"])
def ingest_security_logs() -> Dict:
    """Root node - no upstream, feeds security analytics."""
    print("🔒 Ingesting security system logs...")
    time.sleep(0.5)
    return {
        "events": random.randint(50, 200),
        "alerts": random.randint(0, 5),
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_conservation_work_orders, tags=["ingestion", "conservation"])
def ingest_conservation_work_orders() -> Dict:
    """Root node - no upstream, feeds conservation scheduling."""
    print("🎨 Ingesting conservation work orders...")
    time.sleep(0.5)
    return {
        "work_orders": random.randint(5, 20),
        "priority_urgent": random.randint(0, 3),
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_artwork_loans, tags=["ingestion", "collection-management"])
def ingest_artwork_loans() -> Dict:
    """Root node - no upstream, feeds loan tracking."""
    print("📦 Ingesting artwork loan records...")
    time.sleep(0.5)
    return {
        "active_loans": random.randint(10, 30),
        "pending_returns": random.randint(2, 8),
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_education_programs, tags=["ingestion", "education"])
def ingest_education_programs() -> Dict:
    """Root node - no upstream, feeds education analytics."""
    print("📚 Ingesting education program registrations...")
    time.sleep(0.5)
    return {
        "registrations": random.randint(50, 200),
        "programs": ["School Tours", "Art Workshops", "Lecture Series"],
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_donor_contributions, tags=["ingestion", "fundraising"])
def ingest_donor_contributions() -> Dict:
    """Root node - no upstream, feeds donor analytics."""
    print("💝 Ingesting donor contribution records...")
    time.sleep(0.5)
    return {
        "contributions": random.randint(10, 50),
        "total_amount": random.randint(50000, 500000),
        "timestamp": datetime.now().isoformat()
    }

# ============================================================================
# FIRST TRANSFORMATION LAYER (Single upstream each)
# ============================================================================

@materialize(cleaned_ticket_sales, tags=["transformation", "cleaning"], asset_deps=[raw_ticket_sales])
def clean_ticket_sales(raw_sales: Dict) -> Dict:
    """Single upstream - transforms raw ticket data."""
    print("🧹 Cleaning ticket sales data...")
    time.sleep(0.5)
    return calculate_metrics(raw_sales)

@materialize(cleaned_memberships, tags=["transformation", "cleaning"], asset_deps=[raw_membership_signups])
def clean_memberships(raw_memberships: Dict) -> Dict:
    """Single upstream - transforms raw membership data."""
    print("🧹 Cleaning membership records...")
    time.sleep(0.5)
    return calculate_metrics(raw_memberships)

@materialize(cleaned_gift_shop, tags=["transformation", "cleaning"], asset_deps=[raw_gift_shop_transactions])
def clean_gift_shop(raw_shop: Dict) -> Dict:
    """Single upstream - transforms raw retail data."""
    print("🧹 Cleaning gift shop transactions...")
    time.sleep(0.5)
    return calculate_metrics(raw_shop)

@materialize(exhibition_visits, tags=["transformation", "visitor-tracking"], asset_deps=[raw_exhibition_checkins])
def process_exhibition_visits(raw_checkins: Dict) -> Dict:
    """Single upstream - structures exhibition visit data."""
    print("📊 Processing exhibition visits...")
    time.sleep(0.5)
    return {
        **raw_checkins,
        "avg_dwell_time_minutes": random.randint(15, 45),
        "popular_exhibition": random.choice(raw_checkins["exhibitions"])
    }

@materialize(climate_control_logs, tags=["transformation", "facilities"], asset_deps=[raw_environmental_sensors])
def process_climate_control(raw_sensors: Dict) -> Dict:
    """Single upstream - aggregates environmental data."""
    print("🌡️  Processing climate control logs...")
    time.sleep(0.5)
    avg_temp = sum(s["temperature"] for s in raw_sensors["sensors"]) / len(raw_sensors["sensors"])
    return {
        "average_temperature": round(avg_temp, 1),
        "alerts_triggered": random.randint(0, 2),
        "sensors_count": len(raw_sensors["sensors"])
    }

# ============================================================================
# SECOND TRANSFORMATION LAYER (Multi-upstream)
# ============================================================================

@materialize(daily_visitor_metrics, tags=["analytics", "visitors"], asset_deps=[cleaned_ticket_sales, exhibition_visits])
def calculate_daily_visitor_metrics(tickets: Dict, visits: Dict) -> Dict:
    """Multi-upstream (2) - combines ticket and exhibition data."""
    print("📈 Calculating daily visitor metrics...")
    time.sleep(0.5)
    return {
        "total_visitors": tickets["customers"],
        "exhibition_visits": visits["checkins"],
        "revenue_per_visitor": round(tickets["revenue"] / tickets["customers"], 2),
        "engagement_rate": round(visits["checkins"] / tickets["customers"], 2)
    }

@materialize(revenue_breakdown, tags=["analytics", "finance"], asset_deps=[cleaned_ticket_sales, cleaned_gift_shop])
def calculate_revenue_breakdown(tickets: Dict, shop: Dict) -> Dict:
    """Multi-upstream (2) - combines revenue sources."""
    print("💰 Calculating revenue breakdown...")
    time.sleep(0.5)
    total = tickets["revenue"] + shop["revenue"]
    return {
        "admission_revenue": tickets["revenue"],
        "retail_revenue": shop["revenue"],
        "total_revenue": total,
        "retail_percentage": round((shop["revenue"] / total) * 100, 1)
    }

@materialize(member_engagement_scores, tags=["analytics", "membership"], asset_deps=[cleaned_memberships, exhibition_visits])
def calculate_member_engagement(memberships: Dict, visits: Dict) -> Dict:
    """Multi-upstream (2) - scores member activity."""
    print("⭐ Calculating member engagement scores...")
    time.sleep(0.5)
    return {
        "active_members": memberships["customers"],
        "avg_visits_per_member": round(visits["checkins"] / memberships["customers"], 1),
        "engagement_score": round(random.uniform(0.6, 0.9), 2)
    }

@materialize(visitor_demographics, tags=["analytics", "demographics"], asset_deps=[cleaned_ticket_sales, cleaned_memberships])
def analyze_visitor_demographics(tickets: Dict, memberships: Dict) -> Dict:
    """Multi-upstream (2) - combines demographic data."""
    print("👥 Analyzing visitor demographics...")
    time.sleep(0.5)
    return {
        "total_unique_visitors": tickets["customers"] + memberships["customers"],
        "member_ratio": round(memberships["customers"] / (tickets["customers"] + memberships["customers"]), 2),
        "age_distribution": {"18-34": 0.30, "35-54": 0.45, "55+": 0.25}
    }

@materialize(conservation_schedule, tags=["operations", "conservation"], asset_deps=[raw_conservation_work_orders, climate_control_logs])
def create_conservation_schedule(work_orders: Dict, climate: Dict) -> Dict:
    """Multi-upstream (2) - prioritizes conservation work."""
    print("📋 Creating conservation schedule...")
    time.sleep(0.5)
    return {
        "scheduled_work_orders": work_orders["work_orders"],
        "urgent_items": work_orders["priority_urgent"],
        "climate_alerts": climate["alerts_triggered"],
        "priority_affected_by_climate": random.randint(0, 3)
    }

# ============================================================================
# THIRD TRANSFORMATION LAYER (Single downstream from previous)
# ============================================================================

@materialize(visitor_retention_analysis, tags=["analytics", "retention"], asset_deps=[member_engagement_scores])
def analyze_visitor_retention(engagement: Dict) -> Dict:
    """Single upstream - derives retention metrics."""
    print("🔄 Analyzing visitor retention...")
    time.sleep(0.5)
    return {
        **engagement,
        "retention_rate": round(random.uniform(0.70, 0.85), 2),
        "churn_risk_members": random.randint(10, 50),
        "predicted_renewals": round(engagement["active_members"] * random.uniform(0.75, 0.90))
    }

@materialize(revenue_forecasts, tags=["analytics", "forecasting"], asset_deps=[revenue_breakdown])
def create_revenue_forecasts(revenue: Dict) -> Dict:
    """Single upstream - forecasts future revenue."""
    print("📊 Creating revenue forecasts...")
    time.sleep(0.5)
    return {
        "current_revenue": revenue["total_revenue"],
        "q1_forecast": round(revenue["total_revenue"] * random.uniform(1.05, 1.15)),
        "q2_forecast": round(revenue["total_revenue"] * random.uniform(1.10, 1.20)),
        "confidence_interval": 0.85
    }

@materialize(artwork_condition_reports, tags=["operations", "conservation"], asset_deps=[conservation_schedule])
def generate_artwork_condition_reports(schedule: Dict) -> Dict:
    """Single upstream - creates condition documentation."""
    print("📝 Generating artwork condition reports...")
    time.sleep(0.5)
    return {
        "reports_generated": schedule["scheduled_work_orders"],
        "condition_excellent": random.randint(50, 100),
        "condition_good": random.randint(20, 50),
        "condition_needs_work": random.randint(5, 20)
    }

# ============================================================================
# ADDITIONAL TRANSFORMATION NODES
# ============================================================================

@materialize(gift_shop_inventory_turnover, tags=["analytics", "retail"], asset_deps=[cleaned_gift_shop])
def analyze_inventory_turnover(shop: Dict) -> Dict:
    """Single upstream - analyzes retail inventory."""
    print("📦 Analyzing gift shop inventory turnover...")
    time.sleep(0.5)
    return {
        **shop,
        "turnover_rate": round(random.uniform(4.0, 8.0), 1),
        "fast_movers": random.randint(20, 40),
        "slow_movers": random.randint(5, 15)
    }

@materialize(education_program_attendance, tags=["analytics", "education"], asset_deps=[raw_education_programs, cleaned_ticket_sales])
def analyze_education_attendance(programs: Dict, tickets: Dict) -> Dict:
    """Multi-upstream (2) - analyzes education programs."""
    print("📚 Analyzing education program attendance...")
    time.sleep(0.5)
    return {
        "program_registrations": programs["registrations"],
        "cross_sell_rate": round(programs["registrations"] / tickets["customers"], 2),
        "revenue_per_program": random.randint(500, 2000)
    }

@materialize(artwork_insurance_valuations, tags=["operations", "collection"], asset_deps=[raw_artwork_loans, raw_conservation_work_orders])
def calculate_insurance_valuations(loans: Dict, conservation: Dict) -> Dict:
    """Multi-upstream (2) - updates insurance values."""
    print("💎 Calculating artwork insurance valuations...")
    time.sleep(0.5)
    return {
        "artworks_valued": loans["active_loans"] + conservation["work_orders"],
        "total_insured_value": random.randint(50000000, 100000000),
        "revaluation_needed": random.randint(5, 15)
    }

@materialize(security_incident_summary, tags=["analytics", "security"], asset_deps=[raw_security_logs, exhibition_visits])
def summarize_security_incidents(security: Dict, visits: Dict) -> Dict:
    """Multi-upstream (2) - correlates security with traffic."""
    print("🔒 Summarizing security incidents...")
    time.sleep(0.5)
    return {
        "total_events": security["events"],
        "incidents_per_1000_visitors": round((security["alerts"] / visits["checkins"]) * 1000, 2),
        "risk_level": "low" if security["alerts"] < 3 else "medium"
    }

@materialize(donor_lifetime_value, tags=["analytics", "fundraising"], asset_deps=[raw_donor_contributions, cleaned_memberships])
def calculate_donor_lifetime_value(donors: Dict, members: Dict) -> Dict:
    """Multi-upstream (2) - predicts donor value."""
    print("💝 Calculating donor lifetime value...")
    time.sleep(0.5)
    return {
        "total_donors": donors["contributions"],
        "avg_contribution": round(donors["total_amount"] / donors["contributions"]),
        "predicted_ltv": round(donors["total_amount"] * random.uniform(3.0, 5.0)),
        "high_value_donors": random.randint(5, 15)
    }

# ============================================================================
# COMPLEX HUB NODES (Multi-upstream, multi-downstream)
# ============================================================================

@materialize(integrated_visitor_profile, tags=["analytics", "integration", "hub"],
             asset_deps=[visitor_demographics, member_engagement_scores, daily_visitor_metrics])
def create_integrated_visitor_profile(demographics: Dict, engagement: Dict, metrics: Dict) -> Dict:
    """Multi-upstream (3), multi-downstream - central visitor data hub."""
    print("🎯 Creating integrated 360° visitor profile...")
    time.sleep(0.5)
    return {
        "total_visitors": demographics["total_unique_visitors"],
        "engagement_score": engagement["engagement_score"],
        "revenue_per_visitor": metrics["revenue_per_visitor"],
        "member_percentage": demographics["member_ratio"],
        "visit_frequency": engagement["avg_visits_per_member"],
        "profile_completeness": 0.95
    }

@materialize(financial_health_score, tags=["analytics", "finance", "hub"],
             asset_deps=[revenue_breakdown, revenue_forecasts, raw_donor_contributions])
def calculate_financial_health_score(revenue: Dict, forecasts: Dict, donors: Dict) -> Dict:
    """Multi-upstream (3), multi-downstream - financial intelligence hub."""
    print("💰 Calculating museum financial health score...")
    time.sleep(0.5)
    score = round(random.uniform(0.75, 0.95), 2)
    return {
        "health_score": score,
        "current_revenue": revenue["total_revenue"],
        "projected_growth": round(((forecasts["q2_forecast"] - revenue["total_revenue"]) / revenue["total_revenue"]) * 100, 1),
        "donor_contribution_pct": round((donors["total_amount"] / (revenue["total_revenue"] + donors["total_amount"])) * 100, 1),
        "rating": "excellent" if score > 0.85 else "good"
    }

# ============================================================================
# DATA WAREHOUSE AGGREGATION LAYER (Multi-upstream marts)
# ============================================================================

@materialize(visitor_analytics_mart, tags=["warehouse", "mart", "visitors"],
             asset_deps=[integrated_visitor_profile, visitor_retention_analysis, daily_visitor_metrics])
def build_visitor_analytics_mart(profile: Dict, retention: Dict, metrics: Dict) -> Dict:
    """Multi-upstream (3) data mart - comprehensive visitor warehouse."""
    print("🏛️  Building visitor analytics data mart...")
    time.sleep(0.5)
    return {
        "mart_name": "visitor_analytics",
        "total_records": profile["total_visitors"],
        "metrics_tracked": ["engagement", "retention", "revenue", "demographics"],
        "retention_rate": retention["retention_rate"],
        "avg_revenue_per_visitor": metrics["revenue_per_visitor"],
        "data_quality": 0.98
    }

@materialize(financial_analytics_mart, tags=["warehouse", "mart", "finance"],
             asset_deps=[financial_health_score, revenue_forecasts, revenue_breakdown])
def build_financial_analytics_mart(health: Dict, forecasts: Dict, revenue: Dict) -> Dict:
    """Multi-upstream (3) data mart - comprehensive financial warehouse."""
    print("🏛️  Building financial analytics data mart...")
    time.sleep(0.5)
    return {
        "mart_name": "financial_analytics",
        "health_score": health["health_score"],
        "current_revenue": revenue["total_revenue"],
        "forecast_accuracy": forecasts["confidence_interval"],
        "revenue_streams": ["admission", "retail", "membership", "donors"],
        "data_quality": 0.97
    }

@materialize(operations_mart, tags=["warehouse", "mart", "operations"],
             asset_deps=[conservation_schedule, climate_control_logs, raw_security_logs])
def build_operations_mart(conservation: Dict, climate: Dict, security: Dict) -> Dict:
    """Multi-upstream (3) data mart - comprehensive operations warehouse."""
    print("🏛️  Building operations management data mart...")
    time.sleep(0.5)
    return {
        "mart_name": "operations_management",
        "active_work_orders": conservation["scheduled_work_orders"],
        "climate_alerts": climate["alerts_triggered"],
        "security_events": security["events"],
        "operational_health": "excellent",
        "data_quality": 0.96
    }

# ============================================================================
# REPORTING LAYER (Leaf nodes - multi-upstream, no downstream)
# ============================================================================

@materialize(dashboard_health_validator, tags=["validation", "health-check", "stateful"])
def validate_dashboard_health() -> Dict:
    """
    Validates dashboard health by checking the last task run state.

    Behavior:
    - Queries Prefect API for the last run of this task
    - If last run succeeded: throws an error (tests error handling)
    - If last run failed or doesn't exist: succeeds
    - Creates alternating success/failure pattern for robustness testing
    """
    import asyncio

    print("🔍 Validating dashboard health using Prefect SDK...")
    time.sleep(0.5)

    async def check_last_run():
        async with get_client() as client:
            # Get the current task run context to find task runs of this task
            # Filter by name to only get runs of this specific task
            task_runs = await client.read_task_runs(
                task_run_filter=TaskRunFilter(
                    name=TaskRunFilterName(like_="validate_dashboard_health%")
                ),
                sort=TaskRunSort.END_TIME_DESC,
                limit=10  # Get last 10 runs to find the previous one
            )

            # END_TIME_DESC with NULLS LAST: completed runs come first, current
            # running task (null end_time) sorts to the end of the list.
            # So index 0 is the most recently completed previous run.
            if len(task_runs) >= 1:
                last_run = task_runs[0]
                last_state = last_run.state

                print(f"  📊 Found previous task run: {last_run.id}")
                print(f"  📈 Previous state: {last_state.type if last_state else 'Unknown'}")

                if last_state and last_state.is_completed():
                    # Last run succeeded, so we fail this time
                    print("  ❌ Last run SUCCEEDED - throwing error to test error handling!")
                    raise ValueError(
                        "Dashboard validation failed: Previous run succeeded, "
                        "triggering alternating failure pattern for robustness testing. "
                        "This demonstrates conditional error handling based on historical state."
                    )
                else:
                    # Last run failed or was in another state, so we succeed
                    print("  ✅ Last run FAILED or was incomplete - validation PASSES!")
                    return {
                        "validation_status": "passed",
                        "previous_run_id": str(last_run.id),
                        "previous_state": last_state.type.value if last_state else "unknown",
                        "pattern": "alternating_after_failure",
                        "timestamp": datetime.now().isoformat()
                    }
            else:
                # First run ever, so we succeed
                print("  🆕 No previous run found - first execution, validation PASSES!")
                return {
                    "validation_status": "passed",
                    "previous_run_id": None,
                    "previous_state": "none",
                    "pattern": "first_run",
                    "timestamp": datetime.now().isoformat()
                }

    # Run the async function
    return asyncio.run(check_last_run())

@materialize(executive_dashboard, tags=["reporting", "dashboard", "leaf"],
             asset_deps=[dashboard_health_validator, visitor_analytics_mart, financial_analytics_mart])
def create_executive_dashboard(validation: Dict, visitor_mart: Dict, financial_mart: Dict) -> Dict:
    """Multi-upstream (3), no downstream - terminal leaf node."""
    print("📊 Creating executive summary dashboard...")
    print(f"  ✅ Dashboard health validation: {validation['validation_status']}")
    time.sleep(0.5)
    return {
        "dashboard": "executive_summary",
        "validation_status": validation["validation_status"],
        "visitor_records": visitor_mart["total_records"],
        "financial_health": financial_mart["health_score"],
        "kpis_displayed": 15,
        "last_updated": datetime.now().isoformat(),
        "status": "terminal_leaf_node"
    }

@materialize(operations_dashboard, tags=["reporting", "dashboard", "leaf"],
             asset_deps=[operations_mart])
def create_operations_dashboard(ops_mart: Dict) -> Dict:
    """Single upstream, no downstream - terminal leaf node."""
    print("🔧 Creating operations management dashboard...")
    time.sleep(0.5)
    return {
        "dashboard": "operations_management",
        "work_orders": ops_mart["active_work_orders"],
        "alerts": ops_mart["climate_alerts"],
        "status": "terminal_leaf_node"
    }

@materialize(visitor_trends_report, tags=["reporting", "report", "leaf"],
             asset_deps=[visitor_analytics_mart])
def create_visitor_trends_report(visitor_mart: Dict) -> Dict:
    """Single upstream, no downstream - terminal leaf node."""
    print("📈 Creating visitor trends analysis report...")
    time.sleep(0.5)
    return {
        "report": "visitor_trends",
        "retention_rate": visitor_mart["retention_rate"],
        "pages": 25,
        "status": "terminal_leaf_node"
    }

@materialize(financial_reports, tags=["reporting", "report", "leaf"],
             asset_deps=[financial_analytics_mart])
def create_financial_reports(financial_mart: Dict) -> Dict:
    """Single upstream, no downstream - terminal leaf node."""
    print("💵 Creating financial management reports...")
    time.sleep(0.5)
    return {
        "report": "financial_management",
        "health_score": financial_mart["health_score"],
        "revenue": financial_mart["current_revenue"],
        "status": "terminal_leaf_node"
    }

@materialize(conservation_tracking_dashboard, tags=["reporting", "dashboard", "leaf"],
             asset_deps=[operations_mart, artwork_condition_reports])
def create_conservation_dashboard(ops_mart: Dict, conditions: Dict) -> Dict:
    """Multi-upstream (2), no downstream - terminal leaf node."""
    print("🎨 Creating conservation project tracking dashboard...")
    time.sleep(0.5)
    return {
        "dashboard": "conservation_tracking",
        "active_projects": ops_mart["active_work_orders"],
        "condition_reports": conditions["reports_generated"],
        "status": "terminal_leaf_node"
    }

# ============================================================================
# FLOW ORCHESTRATION - Flow of Flows Architecture
# ============================================================================

@flow(name="Isolated Assets Flow", log_prints=True)
def isolated_assets_flow():
    """Subflow for isolated assets with no dependencies."""
    print("\n🔹 ISOLATED ASSETS FLOW")
    print("=" * 80)

    budget = upload_annual_budget()
    weather = fetch_weather_data()

    print(f"✅ Isolated assets: Budget ({budget['fiscal_year']}), Weather archived")
    return {"budget": budget, "weather": weather}


@flow(name="Data Ingestion Flow", log_prints=True, task_runner=DaskTaskRunner())
def data_ingestion_flow():
    """Subflow for raw data ingestion - all root nodes."""
    print("\n🔹 DATA INGESTION FLOW")
    print("=" * 80)

    tickets = ingest_ticket_sales()
    memberships = ingest_membership_signups()
    shop = ingest_gift_shop_transactions()
    exhibitions = ingest_exhibition_checkins()
    sensors = ingest_environmental_sensors()
    security = ingest_security_logs()
    conservation = ingest_conservation_work_orders()
    loans = ingest_artwork_loans()
    education = ingest_education_programs()
    donors = ingest_donor_contributions()

    total_records = (tickets["records"] + memberships["records"] +
                    shop["records"] + exhibitions["checkins"])
    print(f"✅ Ingested {total_records} total records from all sources")

    return {
        "tickets": tickets,
        "memberships": memberships,
        "shop": shop,
        "exhibitions": exhibitions,
        "sensors": sensors,
        "security": security,
        "conservation": conservation,
        "loans": loans,
        "education": education,
        "donors": donors
    }


@flow(name="Visitor Analytics Flow", log_prints=True, task_runner=DaskTaskRunner())
def visitor_analytics_flow(ingestion_data: Dict):
    """Subflow for visitor-related analytics transformations."""
    print("\n🔹 VISITOR ANALYTICS FLOW")
    print("=" * 80)

    # First transformation layer
    cleaned_tickets = clean_ticket_sales(ingestion_data["tickets"])
    cleaned_members = clean_memberships(ingestion_data["memberships"])
    exhibit_visits = process_exhibition_visits(ingestion_data["exhibitions"])

    # Second transformation layer
    daily_metrics = calculate_daily_visitor_metrics(cleaned_tickets, exhibit_visits)
    engagement = calculate_member_engagement(cleaned_members, exhibit_visits)
    demographics = analyze_visitor_demographics(cleaned_tickets, cleaned_members)

    # Third transformation layer
    retention = analyze_visitor_retention(engagement)

    # Education analytics
    edu_attendance = analyze_education_attendance(ingestion_data["education"], cleaned_tickets)

    # Hub node
    visitor_profile = create_integrated_visitor_profile(demographics, engagement, daily_metrics)

    print(f"✅ Visitor analytics: {visitor_profile['total_visitors']} visitors profiled")

    return {
        "cleaned_tickets": cleaned_tickets,
        "cleaned_members": cleaned_members,
        "exhibit_visits": exhibit_visits,
        "daily_metrics": daily_metrics,
        "engagement": engagement,
        "demographics": demographics,
        "retention": retention,
        "visitor_profile": visitor_profile,
        "edu_attendance": edu_attendance
    }


@flow(name="Financial Analytics Flow", log_prints=True, task_runner=DaskTaskRunner())
def financial_analytics_flow(ingestion_data: Dict, visitor_data: Dict):
    """Subflow for financial analytics transformations."""
    print("\n🔹 FINANCIAL ANALYTICS FLOW")
    print("=" * 80)

    # First transformation layer
    cleaned_shop_data = clean_gift_shop(ingestion_data["shop"])

    # Second transformation layer
    revenue = calculate_revenue_breakdown(visitor_data["cleaned_tickets"], cleaned_shop_data)

    # Third transformation layer
    forecasts = create_revenue_forecasts(revenue)

    # Additional nodes
    inventory = analyze_inventory_turnover(cleaned_shop_data)
    donor_ltv = calculate_donor_lifetime_value(ingestion_data["donors"], visitor_data["cleaned_members"])

    # Hub node
    financial_health = calculate_financial_health_score(revenue, forecasts, ingestion_data["donors"])

    print(f"✅ Financial analytics: ${financial_health['current_revenue']:,} revenue, {financial_health['rating']} health")

    return {
        "cleaned_shop": cleaned_shop_data,
        "revenue": revenue,
        "forecasts": forecasts,
        "inventory": inventory,
        "donor_ltv": donor_ltv,
        "financial_health": financial_health
    }


@flow(name="Operations Management Flow", log_prints=True, task_runner=DaskTaskRunner())
def operations_management_flow(ingestion_data: Dict, visitor_data: Dict):
    """Subflow for facilities and conservation operations."""
    print("\n🔹 OPERATIONS MANAGEMENT FLOW")
    print("=" * 80)

    # First transformation layer
    climate = process_climate_control(ingestion_data["sensors"])

    # Second transformation layer
    conservation_sched = create_conservation_schedule(ingestion_data["conservation"], climate)

    # Third transformation layer
    condition_reports = generate_artwork_condition_reports(conservation_sched)

    # Additional nodes
    insurance = calculate_insurance_valuations(ingestion_data["loans"], ingestion_data["conservation"])
    security_summary = summarize_security_incidents(ingestion_data["security"], visitor_data["exhibit_visits"])

    print(f"✅ Operations: {conservation_sched['scheduled_work_orders']} work orders, {climate['alerts_triggered']} alerts")

    return {
        "climate": climate,
        "conservation_schedule": conservation_sched,
        "condition_reports": condition_reports,
        "insurance": insurance,
        "security_summary": security_summary
    }


@flow(name="Data Warehouse Flow", log_prints=True, task_runner=DaskTaskRunner())
def data_warehouse_flow(visitor_data: Dict, financial_data: Dict, operations_data: Dict, ingestion_data: Dict):
    """Subflow for building data warehouse marts."""
    print("\n🔹 DATA WAREHOUSE FLOW")
    print("=" * 80)

    # Build marts
    visitor_mart = build_visitor_analytics_mart(
        visitor_data["visitor_profile"],
        visitor_data["retention"],
        visitor_data["daily_metrics"]
    )

    financial_mart = build_financial_analytics_mart(
        financial_data["financial_health"],
        financial_data["forecasts"],
        financial_data["revenue"]
    )

    ops_mart = build_operations_mart(
        operations_data["conservation_schedule"],
        operations_data["climate"],
        ingestion_data["security"]
    )

    print(f"✅ Data warehouse: 3 marts built with {visitor_mart['data_quality']:.0%} avg quality")

    return {
        "visitor_mart": visitor_mart,
        "financial_mart": financial_mart,
        "operations_mart": ops_mart
    }


@flow(name="Reporting Flow", log_prints=True, task_runner=DaskTaskRunner())
def reporting_flow(warehouse_data: Dict, operations_data: Dict):
    """Subflow for final reporting and dashboards - all leaf nodes."""
    print("\n🔹 REPORTING FLOW (LEAF NODES)")
    print("=" * 80)

    # Validate dashboard health before creating reports
    validation = validate_dashboard_health()

    # Create all final reports
    exec_dash = create_executive_dashboard(
        validation,
        warehouse_data["visitor_mart"],
        warehouse_data["financial_mart"]
    )

    ops_dash = create_operations_dashboard(warehouse_data["operations_mart"])

    visitor_report = create_visitor_trends_report(warehouse_data["visitor_mart"])

    financial_report = create_financial_reports(warehouse_data["financial_mart"])

    conservation_dash = create_conservation_dashboard(
        warehouse_data["operations_mart"],
        operations_data["condition_reports"]
    )

    print(f"✅ Reporting: 5 dashboards/reports generated (all terminal leaf nodes)")

    return {
        "executive_dashboard": exec_dash,
        "operations_dashboard": ops_dash,
        "visitor_trends": visitor_report,
        "financial_reports": financial_report,
        "conservation_dashboard": conservation_dash
    }


@flow(name="Museum Operations - Primary Flow", log_prints=True, task_runner=DaskTaskRunner())
def museum_operations_flow():
    """
    Master orchestration flow for museum operations analytics.

    Demonstrates comprehensive asset dependency patterns:
    - 40+ assets across ingestion, transformation, warehouse, and reporting layers
    - Isolated assets (no dependencies)
    - Root nodes (no upstream)
    - Leaf nodes (no downstream)
    - Multi-upstream/downstream combinations
    - Hub nodes (multi-multi patterns)

    Asset Categories:
    - s3:// - Raw data storage
    - snowflake:// - Cloud data warehouse staging and analytics
    - pg:// - PostgreSQL operational database
    - redshift:// - Enterprise data warehouse marts
    - tableau://, looker://, powerbi:// - BI visualization platforms
    - local://, external:// - Isolated reference data

    Architecture: Flow of flows pattern
    1. Isolated Assets Flow (standalone references)
    2. Data Ingestion Flow (raw data collection)
    3. Visitor Analytics Flow (visitor pipeline)
    4. Financial Analytics Flow (revenue pipeline)
    5. Operations Management Flow (facilities pipeline)
    6. Data Warehouse Flow (mart aggregation)
    7. Reporting Flow (terminal dashboards)
    """
    print("=" * 80)
    print("🏛️  MUSEUM OPERATIONS - MASTER ORCHESTRATION FLOW")
    print("=" * 80)
    print("📊 Asset Pipeline: ~40 assets across 7 subflows")
    print("🔀 Dependency Patterns: Isolated, root, leaf, multi-upstream, multi-downstream, hub nodes")
    print("=" * 80)

    start_time = datetime.now()

    # Execute flow of flows
    print("\n🚀 Starting museum operations pipeline...\n")

    # Phase 1: Isolated assets (can run independently)
    isolated_data = isolated_assets_flow()

    # Phase 2: Data ingestion (all root nodes, no upstream dependencies)
    ingestion_data = data_ingestion_flow()

    # Phase 3: Analytics flows (parallel execution where possible)
    visitor_data = visitor_analytics_flow(ingestion_data)
    financial_data = financial_analytics_flow(ingestion_data, visitor_data)
    operations_data = operations_management_flow(ingestion_data, visitor_data)

    # Phase 4: Data warehouse (aggregation layer)
    warehouse_data = data_warehouse_flow(visitor_data, financial_data, operations_data, ingestion_data)

    # Phase 5: Reporting (all leaf nodes, terminal consumption)
    reports = reporting_flow(warehouse_data, operations_data)

    # Calculate summary statistics
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print("\n" + "=" * 80)
    print("🎯 MUSEUM OPERATIONS SUMMARY")
    print("=" * 80)
    print(f"⏱️  Pipeline Duration: {duration:.1f} seconds")
    print(f"🏛️  Visitor Analytics: {visitor_data['visitor_profile']['total_visitors']} visitors profiled")
    print(f"💰 Financial Health: {financial_data['financial_health']['rating'].upper()} (score: {financial_data['financial_health']['health_score']})")
    print(f"🎨 Conservation: {operations_data['conservation_schedule']['scheduled_work_orders']} work orders scheduled")
    print(f"📊 Data Quality: {warehouse_data['visitor_mart']['data_quality']:.0%} average across marts")
    print(f"📈 Dashboards Created: {len(reports)} terminal reporting assets")
    print("\n✅ Asset Dependency Patterns Demonstrated:")
    print("   - Isolated assets: annual_budget_upload, external_weather_data")
    print("   - Root nodes: 10 raw ingestion sources")
    print("   - Single transformations: 5 cleaning/processing tasks")
    print("   - Multi-upstream: 8 analytics aggregations")
    print("   - Hub nodes: integrated_visitor_profile, financial_health_score")
    print("   - Data marts: 3 warehouse aggregations")
    print("   - Leaf nodes: 5 terminal dashboards/reports")
    print("=" * 80)

    return {
        "duration_seconds": duration,
        "total_visitors": visitor_data['visitor_profile']['total_visitors'],
        "total_revenue": financial_data['financial_health']['current_revenue'],
        "financial_health_score": financial_data['financial_health']['health_score'],
        "data_quality": warehouse_data['visitor_mart']['data_quality'],
        "dashboards_created": len(reports),
        "pipeline_status": "success"
    }


if __name__ == "__main__":
    # Run the master orchestration flow
    result = museum_operations_flow()
    print(f"\n🎊 Museum operations pipeline completed successfully!")
    print(f"📊 Final metrics: {result}")
