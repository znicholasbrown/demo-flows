"""
Museum Acquisitions Flow - Cross-Workspace Asset Dependency Demonstration

This flow demonstrates cross-workspace asset dependencies with museum_operations_flow:

Cross-workspace upstream assets (materialized by museum_operations_flow, read here):
  1. artwork_condition_reports   - conservation capacity check before committing to new acquisitions
  2. financial_health_score      - institutional financial health gates acquisition budget capacity

Cross-workspace shared assets (materialized here, read by museum_operations_flow):
  1. newly_catalogued_artworks   - new acquisitions need conservation scheduling in ops
  2. acquisition_provenance_database - provenance records required for ops insurance valuations

Workspace: national-museum-acquisitions
Theme: Art acquisition lifecycle - market research through collection integration
Architecture: Flow of flows pattern mirroring museum_operations_flow
"""
import time
import random
from datetime import datetime
from typing import Dict

from prefect import flow
from prefect.assets import Asset, AssetProperties, materialize
from prefect_dask import DaskTaskRunner


# ============================================================================
# CROSS-WORKSPACE REFERENCE ASSETS
# Materialized by museum_operations_flow - declared here as upstream deps only
# Same asset keys as defined in museum_operations_flow.py
# ============================================================================

artwork_condition_reports = Asset(
    key="pg://museum-db/artwork-condition-reports",
    properties=AssetProperties(
        name="Artwork Condition Assessment Reports",
        description="""Cross-workspace upstream asset materialized by museum_operations_flow.

The acquisitions team references conservation condition reports to assess feasibility
before committing to acquire artworks that may require significant restoration. Current
lab backlog and conservator specializations directly constrain which acquisitions are
operationally viable in a given period.""",
        owners=["crickpettish"],
    )
)

financial_health_score = Asset(
    key="snowflake://analytics/financial-health-score",
    properties=AssetProperties(
        name="Museum Financial Health Score",
        description="""Cross-workspace upstream asset materialized by museum_operations_flow.

The acquisitions committee uses institutional financial health score to calibrate
acquisition budget capacity. Board policy requires health score >70 for major
acquisitions (>$500k); health score <60 triggers an automatic acquisition freeze
to protect operating reserves and endowment targets.""",
        owners=["nicholas", "crickpettish"],
    )
)

# ============================================================================
# CROSS-WORKSPACE SHARED ASSETS
# Materialized HERE, declared as upstream deps in museum_operations_flow
# ============================================================================

newly_catalogued_artworks = Asset(
    key="pg://museum-db/newly-catalogued-artworks",
    properties=AssetProperties(
        name="Newly Catalogued Artworks",
        description="""**Purpose:** Registry of formally acquired and catalogued artworks that have
completed the acquisitions pipeline and been added to the permanent collection.

**Materialized by:** museum_acquisitions_flow (national-museum-acquisitions workspace)
**Consumed by:** museum_operations_flow (museum-admin workspace) as upstream dep for conservation_schedule

**Cross-Workspace Role:** When new works join the collection, operations must schedule
initial condition assessments, assign climate monitoring zones, establish conservation
priority baselines, and register works in insurance tracking systems. This asset is the
handoff point between the acquisitions and operations data pipelines.

**Schema:** Artwork records with accession number, title, artist, acquisition date,
acquisition method (purchase/gift/bequest/government-transfer), preliminary condition
rating, dimensions, medium, assigned storage location, and exhibition eligibility flag.

**Volume:** 15-40 new acquisitions annually.

**Refresh:** Updated when approved acquisitions complete formal cataloguing (typically
within 30 days of physical receipt).""",
        owners=["crickpettish", "nicholas"],
    )
)

acquisition_provenance_database = Asset(
    key="snowflake://acquisitions/provenance-database",
    properties=AssetProperties(
        name="Acquisition Provenance Database",
        description="""**Purpose:** Comprehensive provenance and authentication records for all acquired
artworks, establishing clear ownership history, export compliance, and ethical acquisition
standards per AAM guidelines and the 1970 UNESCO Convention.

**Materialized by:** museum_acquisitions_flow (national-museum-acquisitions workspace)
**Consumed by:** museum_operations_flow (museum-admin workspace) as upstream dep for artwork_insurance_valuations

**Cross-Workspace Role:** Insurance valuations for acquired works require complete provenance
chains to establish legal ownership (insurable interest), historical transaction values
(market comparables for insurance benchmarking), and export license compliance (required
for international insurance coverage). The ops flow's insurance valuation model cannot
properly price newly acquired works without this provenance record.

**Schema:** Per-artwork provenance chains with prior owners, ownership periods, exhibition
history, auction/sale records, export licenses, authentication certificates, scholarly
literature citations, and repatriation clearance documentation.

**Volume:** Full provenance records for 2,400+ collection works. 15-40 new artwork records
added per acquisition cohort. Cross-checked against WIPO Art Loss Register for all acquisitions.

**Refresh:** Continuous updates as new provenance research emerges; bulk updates when an
acquisition cohort clears final cataloguing.""",
        owners=["crickpettish"],
    )
)

# ============================================================================
# ISOLATED ASSET (No upstream, no downstream)
# ============================================================================

acquisition_policy_manual = Asset(
    key="local://acquisitions/policy-manual",
    properties=AssetProperties(
        name="Collections Acquisition Policy Manual",
        description="""**Purpose:** Board-approved governing document defining acquisition scope, ethics
standards, and approval authority thresholds.

**Source:** Board of Trustees Collections Committee, reviewed and ratified annually.
Uploaded by Chief Curator and Director of Collections.

**Contents:** Collection scope statement (geographic focus, time periods, media types),
acquisition ethics guidelines (1970 UNESCO Convention, due diligence requirements),
financial authority matrix ($0-50k: Chief Curator; $50-500k: Director + CFO; $500k+:
Board Committee vote), deaccession policy, gift acceptance criteria, repatriation
commitments.

**Format:** PDF in SharePoint, versioned for audit trail. Not processed by the analytics
pipeline - reference document only.

**Refresh:** Annual review at January board meeting. Interim amendments require majority board vote.""",
        owners=["crickpettish"],
    )
)

# ============================================================================
# DATA INGESTION LAYER (Root nodes - no upstream)
# ============================================================================

raw_auction_results = Asset(
    key="s3://acquisitions-raw/auction-results",
    properties=AssetProperties(
        name="Raw Auction House Results",
        description="""**Purpose:** Market price discovery data from major auction houses for competitive
bidding strategy and collection valuation benchmarking.

**Source:** Christie's, Sotheby's, Bonhams, and Phillips auction results via API
integrations. Covers fine art, works on paper, sculpture, and decorative arts aligned
with collection scope.

**Schema:** Lot-level records with sale date, auction house, lot number, artist, title,
date of work, medium, dimensions, estimate range (low/high), hammer price, buyer's
premium, total realized, currency, country of sale, condition report reference, and
provenance snippet.

**Volume:** 8,000-15,000 relevant lots captured annually from 200+ international sales.
Filtered to collection scope (~20% of all auction activity).

**Downstream Consumers:** Feeds art_market_prices → art_market_comparables →
acquisition_candidates, acquisition_budget_capacity.

**Business Value:** Real-time market intelligence prevents overpaying at auction. Average
acquisition savings: 8-12% below estimated fair market value. Comparable data also used
to review donor appraisals for gift acceptance.

**Refresh:** Post-sale batch ingestion within 48 hours, daily during major international
sale weeks (NY/London/HK seasons).""",
        owners=["crickpettish", "nicholas"],
    )
)

raw_dealer_submissions = Asset(
    key="s3://acquisitions-raw/dealer-submissions",
    properties=AssetProperties(
        name="Raw Dealer and Gallery Submissions",
        description="""**Purpose:** Private market inventory submissions from galleries and art dealers
offering works for direct acquisition outside of auction.

**Source:** Dealer relationship portal (Submittable form) and direct email submissions
processed by acquisitions staff. Covers 15 primary gallery relationships plus ad-hoc
submissions.

**Schema:** Submission records with dealer, submission date, artwork details (artist,
title, date, medium, dimensions, condition), asking price, provenance summary,
availability window, exclusivity period, hi-res images, and exhibition history.

**Volume:** 200-400 submissions annually. 12-15% advance to formal consideration.

**Downstream Consumers:** Feeds art_market_comparables for private market price
benchmarking alongside auction data.

**Business Context:** Private market acquisitions avoid buyer's premium (15-25%).
40% of purchases in the last 5 years were sourced privately through dealer relationships.

**Refresh:** Daily batch processing of portal submissions, weekly staff review updates.""",
        owners=["crickpettish"],
    )
)

raw_gift_intake_forms = Asset(
    key="s3://acquisitions-raw/gift-intake-forms",
    properties=AssetProperties(
        name="Raw Artwork Gift and Bequest Intake Forms",
        description="""**Purpose:** Prospective artwork donation and bequest proposals from collectors,
estates, and institutional donors.

**Source:** Gift intake form on museum website and development officer-initiated
proposals from major donor cultivation.

**Schema:** Intake records with donor identity, contact info, artwork details, proposed
gift type (outright gift, fractional gift, bequest, promised gift, bargain sale),
appraisal documentation (if available), donor display/restriction requests, and
development officer relationship notes.

**Volume:** 80-150 intake forms annually. 20-30% advance to curatorial review; 8-12%
result in accepted gifts.

**Downstream Consumers:** Feeds vetted_gift_proposals → acquisition_candidates.

**Business Context:** Gifts and bequests represent the highest-value acquisition channel
by appraised value (avg $450k vs. $285k for purchases). 2023 highlight: $8.2M bequest
from estate of private collector.

**Refresh:** Real-time form ingestion, weekly curatorial review batch processing.""",
        owners=["nicholas", "crickpettish"],
    )
)

raw_provenance_research_files = Asset(
    key="s3://acquisitions-raw/provenance-research",
    properties=AssetProperties(
        name="Raw Provenance Research Files",
        description="""**Purpose:** Archival research documentation establishing ownership history for
acquisition candidates, supporting due diligence and ethical acquisition compliance.

**Source:** Internal provenance research team outputs and commissioned external researchers.
Sources include ERR Project, Commission for Looted Art, Art Loss Register, Interpol
stolen art database, archival photo libraries, auction records, and scholarly literature.

**Schema:** Research files with artwork identifier, researcher, ownership timeline entries
(owner, dates, evidence source, confidence level), gap periods (unknown ownership windows),
export documentation references, repatriation risk assessment (low/medium/high/unclear),
and supporting documents inventory.

**Volume:** 50-80 active provenance research files. All acquisitions require research
completion before committee review.

**Standards:** Follows AAM Standards Regarding the Unlawful Appropriation of Objects
During the Nazi Era (2001) and AAMD Object Registry protocols.

**Downstream Consumers:** Feeds provenance_clearance_records → ethics_review_status →
acquisition_committee_dossiers.

**Refresh:** Continuous updates; final status locked when research completes (typically
4-12 weeks per artwork).""",
        owners=["crickpettish"],
    )
)

raw_repatriation_claims_db = Asset(
    key="s3://acquisitions-raw/repatriation-claims",
    properties=AssetProperties(
        name="Raw Repatriation Claims Database",
        description="""**Purpose:** Active and historical repatriation claims and ownership disputes
affecting acquisition candidates and the existing collection.

**Source:** AAMD Object Registry, UNESCO cultural property claims, bilateral government
cultural property agreements, and direct institutional claims received by legal department.

**Schema:** Claim records with claimed artwork identifier, claimant identity
(government/institution/individual), claim type (WWII looting, colonial-era removal,
cultural patrimony, theft), claim date, legal jurisdiction, evidence summary, and
resolution status (active/resolved/rejected/negotiating).

**Volume:** 15-25 active claims globally affecting museum acquisition scope. 3-5 new
claims filed annually.

**Downstream Consumers:** Critical filter in provenance_clearance_records and
ethics_review_status — any artwork with an active claim is automatically blocked from
acquisition.

**Legal Context:** Bilateral agreements (e.g., with Italy, Greece, Cyprus) require
proactive vetting against this database before any acquisition in scope.

**Refresh:** Weekly sync with AAMD registry; immediate intake for direct claims to
legal department.""",
        owners=["crickpettish"],
    )
)

# ============================================================================
# FIRST TRANSFORMATION LAYER
# ============================================================================

art_market_prices = Asset(
    key="snowflake://acquisitions-staging/art-market-prices",
    properties=AssetProperties(
        name="Cleaned Art Market Price Data",
        description="""**Purpose:** Production-ready auction results dataset with standardized currencies,
de-duped lots, and outlier handling for reliable market analysis.

**Upstream Dependency:** raw_auction_results (s3://acquisitions-raw/auction-results)

**Transformation Logic:**
- Currency normalization: Convert all hammer prices to USD using sale-date ECB exchange rates
- Deduplication: Remove duplicate lots from simultaneous reporting by multiple sources
- Outlier handling: Flag lots with realized price >300% of high estimate or <20% of low
  estimate for manual review
- Enrichment: Append artist_market_index (proprietary growth index by artist) and
  category classification (Impressionism, Modern, Contemporary, etc.)
- Derived fields: buyer's_premium_rate, total_cost_to_buyer, buy_rate (sold vs. passed)

**Quality Metrics:** 99.1% lot matching to canonical artist records, 98.5% complete price
data, <0.5% currency conversion errors.

**Downstream Consumers:** Powers art_market_comparables and acquisition_budget_capacity.

**Storage:** Snowflake staging schema, 10-year historical retention, partitioned by sale_date.

**Refresh:** Batch within 48 hours of each sale; real-time during major international
sale weeks.""",
        owners=["crickpettish", "nicholas"],
    )
)

vetted_gift_proposals = Asset(
    key="snowflake://acquisitions-staging/vetted-gift-proposals",
    properties=AssetProperties(
        name="Vetted Gift and Bequest Proposals",
        description="""**Purpose:** Curatorially-reviewed donation proposals that have passed initial
quality and collection-fit screening, ready for full due diligence evaluation.

**Upstream Dependency:** raw_gift_intake_forms (s3://acquisitions-raw/gift-intake-forms)

**Screening Logic:**
- Collection fit: Filter against acquisition policy scope (period, medium, geography)
- Condition threshold: Remove self-assessed "poor" condition works without conservation
  budget allocation
- Legal review: Flag works with unusual restriction requests for legal counsel
- Completeness check: Require minimum documentation (artist, date, medium, dimensions,
  basic provenance)
- Duplicate detection: Match against existing collection to prevent redundant acquisitions

**Screening Rate:** 22% of raw intake forms advance to vetted status.

**Downstream Consumers:** Feeds acquisition_candidates scoring pipeline.

**Business Context:** Development team notified of major gift prospects (>$500k estimated
value) for donor cultivation alignment.

**Storage:** Snowflake staging with full intake history preserved for donor relationship tracking.

**Refresh:** Weekly batch screening; immediate for time-sensitive estate opportunities.""",
        owners=["crickpettish"],
    )
)

provenance_clearance_records = Asset(
    key="pg://acquisitions-db/provenance-clearance",
    properties=AssetProperties(
        name="Provenance Clearance Records",
        description="""**Purpose:** Synthesized provenance research outcomes combining archival research
with active claims data to produce ethical acquisition clearance determinations.

**Upstream Dependencies:**
- raw_provenance_research_files (s3://acquisitions-raw/provenance-research)
- raw_repatriation_claims_db (s3://acquisitions-raw/repatriation-claims)

**Transformation Logic:**
- Research integration: Consolidate multiple research source outputs into unified
  ownership timeline
- Gap analysis: Identify and classify ownership gaps (pre-1932 vs 1933-1945 vs
  post-1945) per AAMD standards
- Claims cross-reference: Automatically block any artwork appearing in active claims
- Risk scoring: Generate provenance_risk_score (0-100) based on gap duration, gap
  period (Nazi-era gaps weighted 3x), and evidence quality
- Clearance determination: Auto-clear (0-20), manual review (21-50), senior review
  (51-75), blocked (76-100)

**Key Thresholds:** Nazi-era ownership gap of any duration → minimum "senior review".
Unresolved active claim → automatic block.

**Downstream Consumers:** Feeds ethics_review_status, acquisition_committee_dossiers,
and acquisition_provenance_database.

**Storage:** PostgreSQL with complete audit trail; immutable records once clearance issued.

**Refresh:** Rolling updates as provenance research completes; claims status synced weekly.""",
        owners=["crickpettish"],
    )
)

# ============================================================================
# SECOND TRANSFORMATION LAYER
# ============================================================================

art_market_comparables = Asset(
    key="snowflake://acquisitions-analytics/market-comparables",
    properties=AssetProperties(
        name="Art Market Comparable Sales Analysis",
        description="""**Purpose:** Defensible market valuation framework using recent comparable sales
to establish fair market value ranges for acquisition pricing and donor appraisal review.

**Upstream Dependencies:**
- art_market_prices (snowflake://acquisitions-staging/art-market-prices)
- raw_dealer_submissions (s3://acquisitions-raw/dealer-submissions)

**Transformation Logic:**
- Comparable selection: Match acquisition candidates to comparable works using
  multi-factor similarity (artist, period, medium, size, condition, provenance quality)
- Regression modeling: Apply hedonic regression controlling for size, medium, auction
  house prestige, and market cycle timing
- Private market adjustment: Apply 8-12% discount to auction comparables for private
  transaction benchmarks
- Market trend adjustment: Apply artist-specific trailing 3-year market index

**Comparable Quality Scoring:**
- Identical artist + period + medium: 90-100 (strong)
- Same artist + different characteristics: 70-89 (good)
- Related artist/school: 50-69 (weak)

**Downstream Consumers:** Feeds acquisition_candidates (valuation component),
acquisition_budget_capacity (cost feasibility), acquisition_committee_dossiers.

**Business Value:** Comparable analysis prevented $340k overbid at 2023 London sale.
Gift appraisal review caught one inflated donor appraisal ($180k museum value vs.
$420k donor appraisal).

**Refresh:** Weekly for active pipeline; on-demand for time-sensitive auction opportunities.""",
        owners=["crickpettish", "nicholas"],
    )
)

ethics_review_status = Asset(
    key="pg://acquisitions-db/ethics-review",
    properties=AssetProperties(
        name="Ethics and Compliance Review Status",
        description="""**Purpose:** Consolidated ethics review tracking combining provenance clearance,
repatriation risk, and cultural patrimony compliance into a single acquisition eligibility
determination.

**Upstream Dependency:** provenance_clearance_records (pg://acquisitions-db/provenance-clearance)

**Review Logic:**
- Provenance risk threshold: Works scoring >50 require Ethics Committee review
- Cultural patrimony check: Cross-reference against UNESCO 1970 Convention source country
  origin and bilateral agreements
- International sanctions: Screen sellers/donors against OFAC and international sanctions
- Anti-money laundering: Apply art market AML regulations; source of funds verification
  for purchases >€10k
- Final determination: Cleared / Cleared-with-conditions / Pending-review / Blocked

**Ethics Committee:** 5-member standing committee (Director, Chief Curator, Legal Counsel,
CFO, External Ethics Advisor). Quarterly meetings, ad-hoc for time-sensitive cases.

**Downstream Consumers:** Required input for acquisition_candidates and
acquisition_committee_dossiers. No artwork advances to committee without ethics clearance.

**Storage:** PostgreSQL with complete review audit trail; immutable determination records.

**Refresh:** Weekly status updates; immediate for time-sensitive acquisitions.""",
        owners=["crickpettish"],
    )
)

acquisition_candidates = Asset(
    key="snowflake://acquisitions-analytics/acquisition-candidates",
    properties=AssetProperties(
        name="Scored Acquisition Candidate Pipeline",
        description="""**Purpose:** Comprehensive ranked pipeline of all works under active consideration,
scoring each candidate across curatorial fit, market value, provenance quality, and
strategic priority.

**Upstream Dependencies:**
- art_market_comparables (snowflake://acquisitions-analytics/market-comparables)
- vetted_gift_proposals (snowflake://acquisitions-staging/vetted-gift-proposals)
- ethics_review_status (pg://acquisitions-db/ethics-review)

**Scoring Model:**
- Curatorial fit (30%): Collection gap analysis, scholarly significance
- Financial value (25%): Market value relative to budget, gift deduction value
- Provenance quality (20%): Clearance determination, documentation completeness
- Acquisition urgency (15%): Availability window, competition risk
- Strategic priority (10%): Exhibition tie-in potential, director/curator designation

**Pipeline Stages:**
  Identified → Screened → Under Research → Ethics Cleared → Committee Ready →
  Approved → Acquired

**Volume:** 150-200 candidates in pipeline at any time. 20-30 committee-ready annually.

**Downstream Consumers:** Feeds conservation_feasibility_assessment,
acquisition_committee_dossiers, acquisition_budget_capacity.

**Business Value:** Centralized pipeline eliminated duplicate department pursuits. Pipeline
visibility improved acquisition success rate from 62% to 78% of targeted works.

**Refresh:** Daily stage transitions, weekly full scoring recalculation.""",
        owners=["crickpettish", "nicholas"],
    )
)

# ============================================================================
# THIRD TRANSFORMATION LAYER
# Includes cross-workspace upstream deps from museum_operations_flow
# ============================================================================

conservation_feasibility_assessment = Asset(
    key="snowflake://acquisitions-analytics/conservation-feasibility",
    properties=AssetProperties(
        name="Conservation Feasibility Assessment",
        description="""**Purpose:** Acquisition viability analysis combining the operations flow's
conservation condition data with candidate assessments to determine whether the museum
has capacity to properly care for prospective acquisitions.

**Upstream Dependencies:**
- acquisition_candidates (snowflake://acquisitions-analytics/acquisition-candidates)
- artwork_condition_reports (pg://museum-db/artwork-condition-reports)
  [CROSS-WORKSPACE upstream from museum_operations_flow]

**Cross-Workspace Integration:** References the operations flow's artwork_condition_reports
to assess current conservation lab capacity and backlog. A high-condition-need acquisition
during peak conservation periods may be deferred until capacity is available. The lab's
current conservator specializations (paintings, textiles, sculpture) also determine whether
the museum can handle the medium of an acquisition candidate in-house.

**Assessment Logic:**
- Conservation backlog check: Current lab backlog vs. 45-day target
- Condition classification: Assess candidate condition from dealer/donor documentation
- Treatment cost estimation: $150-400/hr specialist time based on medium and condition
- Specialty matching: Lab conservator specializations vs. acquisition candidate medium
- Gallery climate compatibility: Verify candidate climate requirements match available
  gallery environments

**Output:** Feasibility score (0-100) and recommended timing (immediate / defer-3mo /
defer-6mo / requires-external-conservation-first).

**Business Value:** Prevented three acquisitions requiring specialty conservation beyond
lab capacity (would have required $180k external contractor cost not in acquisition budget).

**Refresh:** Weekly for committee-ready candidates; on-demand when conservation capacity
changes significantly.""",
        owners=["crickpettish", "nicholas"],
    )
)

acquisition_budget_capacity = Asset(
    key="snowflake://acquisitions-analytics/budget-capacity",
    properties=AssetProperties(
        name="Acquisition Budget Capacity Analysis",
        description="""**Purpose:** Dynamic acquisition spending capacity model reconciling available
acquisition fund balances with pipeline pricing and institutional financial health constraints.

**Upstream Dependencies:**
- art_market_comparables (snowflake://acquisitions-analytics/market-comparables)
- financial_health_score (snowflake://analytics/financial-health-score)
  [CROSS-WORKSPACE upstream from museum_operations_flow]

**Cross-Workspace Integration:** References the operations flow's financial_health_score
to apply institutional financial health constraints on acquisition spending. Board policy:
health score >80 → full capacity; 70-80 → 75% capacity; 60-70 → 50% capacity;
<60 → emergency freeze. This ensures acquisitions don't destabilize operating reserves.

**Budget Model:**
- Acquisition fund balance: Available by fund type (unrestricted/restricted/deaccession
  proceeds/government grants)
- Committed spend: Works with binding agreements or accepted gift valuations
- Health score gating: Apply health thresholds to available capacity
- Grant alignment: Match government acquisition grant requirements with eligible pipeline

**Annual Acquisition Budget:**
- Purchase funds: $2.8M (unrestricted $1.2M + restricted by category $1.6M)
- Deaccession proceeds: Variable ($0-1.5M annually, restricted to new acquisitions)
- Government grants: $400-800k (IMLS, NEA, state arts agencies)
- Emergency reserve: 15% held back for time-sensitive opportunities

**Business Value:** Health score gating prevented $850k acquisition commitment during
Q2 2023 budget stress, protecting operating reserves.

**Refresh:** Daily for committed spend tracking; weekly for capacity recalculation;
real-time when financial_health_score crosses a threshold.""",
        owners=["nicholas", "crickpettish"],
    )
)

# ============================================================================
# HUB NODE (Multi-upstream, multi-downstream)
# ============================================================================

acquisition_committee_dossiers = Asset(
    key="pg://acquisitions-db/committee-dossiers",
    properties=AssetProperties(
        name="Acquisition Committee Review Dossiers",
        description="""**Purpose:** Comprehensive acquisition review packages compiled for curatorial
committee and board approval, consolidating all due diligence streams into standardized
decision documentation.

**Upstream Dependencies (4 sources):**
- acquisition_candidates (snowflake://acquisitions-analytics/acquisition-candidates)
- ethics_review_status (pg://acquisitions-db/ethics-review)
- conservation_feasibility_assessment (snowflake://acquisitions-analytics/conservation-feasibility)
- acquisition_budget_capacity (snowflake://acquisitions-analytics/budget-capacity)

**Dossier Contents:**
1. Executive summary (1-page recommendation with vote recommendation)
2. Curatorial argument (collection fit, scholarly significance, peer institution comparison)
3. Provenance documentation (ownership history, clearance determination, research gaps)
4. Market valuation (comparable analysis, pricing rationale, negotiation room)
5. Conservation assessment (condition report, treatment plan, timeline, estimated cost)
6. Financial section (fund source, budget impact, gift restrictions, tax considerations)
7. Legal memo (title warranties, export compliance, insurance during transit)
8. Photography (hi-res images, condition photography, installation mockups)

**Approval Levels:**
- <$50k: Chief Curator sign-off (expedited)
- $50k-$500k: Curatorial Committee vote (monthly or email vote)
- >$500k: Full Board Collections Committee vote (quarterly or special meeting)

**Downstream Consumers:** Feeds approved_acquisitions, acquisitions_pipeline_dashboard,
collection_development_report.

**Business Value:** Standardized dossiers reduced committee preparation time from 3 weeks
to 8 days. Board approval rate for fully-dossied works: 84% vs. 61% for informal presentations.

**Refresh:** Compiled on-demand as works reach committee-ready status; finalized 5
business days before committee meeting.""",
        owners=["crickpettish", "nicholas"],
    )
)

# ============================================================================
# FOURTH LAYER - APPROVALS
# ============================================================================

approved_acquisitions = Asset(
    key="pg://acquisitions-db/approved-acquisitions",
    properties=AssetProperties(
        name="Board-Approved Acquisitions",
        description="""**Purpose:** Authoritative registry of artworks with formal board/committee
approval to acquire, driving purchase execution, gift acceptance, and collection integration.

**Upstream Dependency:** acquisition_committee_dossiers (pg://acquisitions-db/committee-dossiers)

**Approval Record Schema:** Artwork identifier, approval date, approval level
(curator/committee/board), vote outcome (unanimous/majority/conditional), acquisition method
(purchase/gift/bequest/government-transfer), approved budget, approved fund source,
conditions attached (display obligations, naming rights, appraisal requirements), and
assigned acquisitions coordinator.

**Post-Approval Workflow:**
- Purchase: Legal executes purchase agreement, wire transfer, shipping logistics
- Gift: Gift acceptance letter, IRS Form 8283 coordination, deed of gift signed
- Bequest: Estate counsel coordination, probate clearance, transfer documentation
- Government Transfer: Federal collections transfer protocols, deaccession paperwork

**Downstream Consumers:** Feeds newly_catalogued_artworks (after transfer and cataloguing)
and acquisition_provenance_database (provenance chain finalized at acquisition).

**Volume:** 20-30 approvals annually. Average time from approval to physical receipt:
45 days (purchases), 90 days (gifts), 6-18 months (bequests).

**Refresh:** Real-time updates when committee/board votes are recorded.""",
        owners=["crickpettish"],
    )
)

# ============================================================================
# REPORTING LAYER (Leaf nodes - no downstream)
# ============================================================================

acquisitions_pipeline_dashboard = Asset(
    key="tableau://acquisitions/pipeline-dashboard",
    properties=AssetProperties(
        name="Acquisitions Pipeline Dashboard",
        description="""**Purpose:** Real-time acquisitions committee dashboard providing Chief Curator,
Director, and Board Collections Committee with pipeline visibility, budget tracking, and
approval workflow management.

**Upstream Dependencies:** acquisition_committee_dossiers, acquisition_budget_capacity,
acquisition_candidates

**Dashboard Sections:**
- Pipeline Kanban: Visual stages from Identified → Acquired, works at each stage
- Budget Tracker: Available vs. committed spend by fund type, fiscal year pacing
- Upcoming Reviews: Next 3 months of committee meetings with works scheduled for review
- Ethics Status: Provenance research completion rates, pending clearances, blocked works
- Approved in Progress: Works with board approval tracking execution milestones
- Market Conditions: Auction season calendar, category market trends

**Business Value:** Eliminated quarterly "where are we on acquisitions?" board meetings.
Committee preparation time reduced from 3 weeks to 8 days.

**Users:** Chief Curator, Director of Collections, CFO, Development Director, 5 Board
Collections Committee members.

**Technical:** Tableau Server, daily refresh, PDF export for board packages.

**Terminal Leaf Node:** No downstream dependencies.""",
        owners=["crickpettish"],
    )
)

collection_development_report = Asset(
    key="looker://acquisitions/collection-development",
    properties=AssetProperties(
        name="Collection Development Strategic Report",
        description="""**Purpose:** Annual and quarterly strategic collection development analysis measuring
acquisition program effectiveness against collection strategy goals and peer institution benchmarks.

**Upstream Dependencies:** approved_acquisitions, acquisition_candidates, art_market_comparables

**Report Categories:**
1. Collection Gap Analysis: Coverage map vs. institutional strategy, underrepresented
   periods/media/geographies, priority targets for next 3-year plan
2. Acquisition Performance: Success rates by source channel (auction/private/gift),
   average time-to-acquisition, budget utilization vs. plan
3. Market Intelligence: Category price trends affecting collection strategy, emerging
   artist markets aligned to scope, competitive landscape
4. Gift Program Health: Donor pipeline, conversion rates, gift-in-kind vs. purchase mix
5. Provenance & Ethics: Portfolio-wide provenance research status, documentation
   completeness improvement year-over-year
6. ROI Analysis: Collection value accretion from acquisitions, insurance value impact,
   exhibition revenue enabled by new acquisitions

**Audience:** Director, Board Collections Committee, Chief Curator, CFO, Development Director.

**Refresh:** Quarterly strategic report + annual comprehensive review.

**Terminal Leaf Node:** No downstream dependencies.""",
        owners=["nicholas", "crickpettish"],
    )
)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_mock_ops_data(asset_name: str) -> Dict:
    """Generate mock data representing what would come from museum_operations_flow."""
    if asset_name == "artwork_condition_reports":
        return {
            "reports_generated": random.randint(40, 80),
            "condition_excellent": random.randint(50, 100),
            "condition_good": random.randint(20, 50),
            "condition_needs_work": random.randint(5, 20),
            "lab_backlog_days": random.randint(35, 65),
            "source": "cross_workspace_museum_operations_flow"
        }
    elif asset_name == "financial_health_score":
        score = round(random.uniform(0.72, 0.92), 2)
        return {
            "health_score": score,
            "rating": "excellent" if score > 0.85 else "good",
            "current_revenue": random.randint(800000, 1200000),
            "projected_growth": round(random.uniform(2.0, 8.0), 1),
            "source": "cross_workspace_museum_operations_flow"
        }
    return {}


# ============================================================================
# ISOLATED ASSET TASK
# ============================================================================

@materialize(acquisition_policy_manual, tags=["isolated", "governance", "reference"])
def upload_acquisition_policy() -> Dict:
    """Isolated asset - no upstream or downstream dependencies."""
    print("📋 Uploading acquisition policy manual...")
    time.sleep(0.3)
    return {
        "version": "2024-v3",
        "board_approved": "2024-01-15",
        "scope": "national_collection_focus",
        "status": "standalone_reference"
    }


# ============================================================================
# DATA INGESTION TASKS (Root nodes)
# ============================================================================

@materialize(raw_auction_results, tags=["ingestion", "market-data", "auction"])
def ingest_auction_results() -> Dict:
    """Root node - ingests auction house price data."""
    print("🔨 Ingesting auction house results...")
    time.sleep(0.5)
    return {
        "lots_captured": random.randint(500, 1500),
        "auction_houses": ["Christie's", "Sotheby's", "Bonhams", "Phillips"],
        "total_sale_value": random.randint(50000000, 200000000),
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_dealer_submissions, tags=["ingestion", "market-data", "private-market"])
def ingest_dealer_submissions() -> Dict:
    """Root node - ingests private dealer and gallery submissions."""
    print("🖼️  Ingesting dealer and gallery submissions...")
    time.sleep(0.4)
    return {
        "submissions": random.randint(15, 40),
        "dealers": random.randint(8, 15),
        "avg_asking_price": random.randint(80000, 500000),
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_gift_intake_forms, tags=["ingestion", "gifts", "donors"])
def ingest_gift_intake_forms() -> Dict:
    """Root node - ingests artwork gift and bequest proposals."""
    print("🎁 Ingesting artwork gift intake forms...")
    time.sleep(0.4)
    return {
        "intake_forms": random.randint(5, 20),
        "gift_types": ["outright_gift", "bequest", "promised_gift", "fractional_gift"],
        "avg_appraised_value": random.randint(100000, 2000000),
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_provenance_research_files, tags=["ingestion", "provenance", "due-diligence"])
def ingest_provenance_research() -> Dict:
    """Root node - ingests provenance research documentation."""
    print("🔍 Ingesting provenance research files...")
    time.sleep(0.4)
    return {
        "active_research_files": random.randint(20, 60),
        "completed_this_period": random.randint(3, 12),
        "research_sources": ["ERR_Project", "Art_Loss_Register", "Interpol_DB", "Archives"],
        "timestamp": datetime.now().isoformat()
    }

@materialize(raw_repatriation_claims_db, tags=["ingestion", "compliance", "repatriation"])
def ingest_repatriation_claims() -> Dict:
    """Root node - syncs external repatriation claims database."""
    print("⚖️  Ingesting repatriation claims database...")
    time.sleep(0.3)
    return {
        "active_claims": random.randint(8, 20),
        "new_claims_this_period": random.randint(0, 3),
        "claim_types": ["nazi_era", "colonial_removal", "cultural_patrimony"],
        "timestamp": datetime.now().isoformat()
    }


# ============================================================================
# FIRST TRANSFORMATION LAYER TASKS
# ============================================================================

@materialize(art_market_prices, tags=["transformation", "market-data"],
             asset_deps=[raw_auction_results])
def clean_auction_results(auction_data: Dict) -> Dict:
    """Single upstream - cleans and normalizes auction price data."""
    print("🧹 Cleaning art market price data...")
    time.sleep(0.4)
    return {
        **auction_data,
        "usd_normalized": True,
        "quality_score": round(random.uniform(0.97, 0.99), 3),
        "outliers_flagged": random.randint(5, 25),
        "avg_realized_price": random.randint(15000, 500000),
    }

@materialize(vetted_gift_proposals, tags=["transformation", "gifts"],
             asset_deps=[raw_gift_intake_forms])
def vet_gift_proposals(raw_gifts: Dict) -> Dict:
    """Single upstream - screens gift proposals for collection fit."""
    print("✅ Vetting artwork gift proposals...")
    time.sleep(0.4)
    vetted = max(1, int(raw_gifts["intake_forms"] * 0.22))
    return {
        "vetted_proposals": vetted,
        "screened_out": raw_gifts["intake_forms"] - vetted,
        "avg_estimated_value": raw_gifts["avg_appraised_value"],
        "timestamp": datetime.now().isoformat()
    }

@materialize(provenance_clearance_records, tags=["transformation", "provenance", "compliance"],
             asset_deps=[raw_provenance_research_files, raw_repatriation_claims_db])
def process_provenance_clearance(research: Dict, claims: Dict) -> Dict:
    """Multi-upstream (2) - synthesizes provenance research with repatriation claims."""
    print("🔍 Processing provenance clearance records...")
    time.sleep(0.5)
    cleared = max(1, int(research["completed_this_period"] * 0.70))
    blocked = min(2, claims["new_claims_this_period"])
    return {
        "cleared_this_period": cleared,
        "pending_review": max(0, research["completed_this_period"] - cleared - blocked),
        "blocked_by_claims": blocked,
        "active_research_files": research["active_research_files"],
        "timestamp": datetime.now().isoformat()
    }


# ============================================================================
# SECOND TRANSFORMATION LAYER TASKS
# ============================================================================

@materialize(art_market_comparables, tags=["analytics", "market-data", "valuation"],
             asset_deps=[art_market_prices, raw_dealer_submissions])
def build_market_comparables(market_prices: Dict, dealer_data: Dict) -> Dict:
    """Multi-upstream (2) - builds market comparable valuations."""
    print("📊 Building art market comparable analysis...")
    time.sleep(0.5)
    avg_market = round((market_prices["avg_realized_price"] + dealer_data["avg_asking_price"]) / 2)
    return {
        "comparables_analyzed": market_prices["lots_captured"] + dealer_data["submissions"],
        "avg_market_value": avg_market,
        "private_market_discount": 0.88,
        "confidence_score": round(random.uniform(0.75, 0.95), 2),
    }

@materialize(ethics_review_status, tags=["compliance", "ethics"],
             asset_deps=[provenance_clearance_records])
def assess_ethics_compliance(clearance: Dict) -> Dict:
    """Single upstream - produces ethics review determinations."""
    print("⚖️  Assessing ethics and compliance status...")
    time.sleep(0.4)
    return {
        "cleared_works": clearance["cleared_this_period"],
        "pending_ethics_review": clearance["pending_review"],
        "blocked_works": clearance["blocked_by_claims"],
        "compliance_rate": round(
            clearance["cleared_this_period"] / max(1, clearance["active_research_files"]), 2
        )
    }

@materialize(acquisition_candidates, tags=["analytics", "pipeline"],
             asset_deps=[art_market_comparables, vetted_gift_proposals, ethics_review_status])
def score_acquisition_candidates(market: Dict, gifts: Dict, ethics: Dict) -> Dict:
    """Multi-upstream (3) - scores and ranks full acquisition pipeline."""
    print("🏆 Scoring acquisition candidates...")
    time.sleep(0.5)
    total_candidates = gifts["vetted_proposals"] + random.randint(5, 15)
    committee_ready = max(1, int(ethics["cleared_works"] * 0.6))
    return {
        "total_pipeline": total_candidates,
        "committee_ready": committee_ready,
        "avg_candidate_score": round(random.uniform(0.60, 0.85), 2),
        "avg_market_value": market["avg_market_value"],
        "top_category": random.choice(["Impressionism", "Modern", "Contemporary", "Works_on_Paper"])
    }


# ============================================================================
# THIRD LAYER - Uses cross-workspace upstream deps from museum_operations_flow
# ============================================================================

@materialize(conservation_feasibility_assessment,
             tags=["analytics", "conservation", "cross-workspace"],
             asset_deps=[acquisition_candidates, artwork_condition_reports])
def assess_conservation_feasibility(candidates: Dict, condition_reports: Dict) -> Dict:
    """Multi-upstream (2) - second dep is cross-workspace from museum_operations_flow."""
    print("🔬 Assessing conservation feasibility for acquisition candidates...")
    print("  [Cross-workspace] Reading artwork_condition_reports from museum_operations_flow")
    time.sleep(0.5)
    feasible = max(1, int(candidates["committee_ready"] * 0.85))
    return {
        "candidates_assessed": candidates["committee_ready"],
        "conservation_feasible": feasible,
        "deferred_for_capacity": candidates["committee_ready"] - feasible,
        "lab_backlog_days": condition_reports.get("lab_backlog_days", random.randint(35, 65)),
        "estimated_treatment_cost": random.randint(15000, 80000)
    }

@materialize(acquisition_budget_capacity,
             tags=["analytics", "finance", "cross-workspace"],
             asset_deps=[art_market_comparables, financial_health_score])
def calculate_budget_capacity(market: Dict, health: Dict) -> Dict:
    """Multi-upstream (2) - second dep is cross-workspace from museum_operations_flow."""
    print("💰 Calculating acquisition budget capacity...")
    print("  [Cross-workspace] Reading financial_health_score from museum_operations_flow")
    time.sleep(0.5)
    base_capacity = 2800000
    health_modifier = health.get("health_score", 0.80)
    gated_capacity = round(base_capacity * min(1.0, health_modifier / 0.80))
    committed = random.randint(400000, 1200000)
    return {
        "total_acquisition_budget": base_capacity,
        "health_score_gating": round(health_modifier, 2),
        "available_capacity": gated_capacity,
        "committed_spend": committed,
        "remaining_capacity": max(0, gated_capacity - committed)
    }


# ============================================================================
# HUB NODE TASK
# ============================================================================

@materialize(acquisition_committee_dossiers,
             tags=["analytics", "committee", "hub"],
             asset_deps=[acquisition_candidates, ethics_review_status,
                         conservation_feasibility_assessment, acquisition_budget_capacity])
def compile_committee_dossiers(candidates: Dict, ethics: Dict,
                                feasibility: Dict, budget: Dict) -> Dict:
    """Multi-upstream (4) hub - compiles committee review packages."""
    print("📁 Compiling acquisition committee dossiers...")
    time.sleep(0.5)
    dossiers = min(candidates["committee_ready"], feasibility["conservation_feasible"])
    return {
        "dossiers_compiled": dossiers,
        "ready_for_committee": dossiers,
        "ethics_cleared": ethics["cleared_works"],
        "budget_available": budget["remaining_capacity"],
        "avg_acquisition_value": candidates["avg_market_value"]
    }


# ============================================================================
# FOURTH LAYER - APPROVED ACQUISITIONS
# ============================================================================

@materialize(approved_acquisitions, tags=["pipeline", "approvals"],
             asset_deps=[acquisition_committee_dossiers])
def record_approved_acquisitions(dossiers: Dict) -> Dict:
    """Single upstream - records committee/board approved acquisitions."""
    print("✅ Recording board-approved acquisitions...")
    time.sleep(0.4)
    approved = max(1, int(dossiers["dossiers_compiled"] * random.uniform(0.75, 0.90)))
    return {
        "approved_count": approved,
        "declined": dossiers["dossiers_compiled"] - approved,
        "total_approved_value": approved * dossiers["avg_acquisition_value"],
        "purchase_count": max(1, int(approved * 0.60)),
        "gift_count": max(0, approved - max(1, int(approved * 0.60))),
        "timestamp": datetime.now().isoformat()
    }


# ============================================================================
# COLLECTION INTEGRATION - Cross-workspace assets materialized here,
# declared as upstream deps in museum_operations_flow
# ============================================================================

@materialize(newly_catalogued_artworks,
             tags=["collection", "integration", "cross-workspace"],
             asset_deps=[approved_acquisitions])
def catalogue_new_acquisitions(approved: Dict) -> Dict:
    """
    Cross-workspace materialization: consumed by museum_operations_flow.

    Operations flow reads this as upstream for conservation_schedule — newly
    acquired works need initial condition assessments, climate zone assignments,
    and conservation priority baselines before they can be managed operationally.
    """
    print("📚 Cataloguing newly acquired artworks...")
    print("  [Cross-workspace] newly_catalogued_artworks → museum_operations_flow::conservation_schedule")
    time.sleep(0.5)
    catalogued = max(1, int(approved["approved_count"] * 0.85))
    return {
        "newly_catalogued": catalogued,
        "accession_numbers": [f"2024.{i:03d}" for i in range(1, catalogued + 1)],
        "pending_cataloguing": approved["approved_count"] - catalogued,
        "new_purchases": approved["purchase_count"],
        "new_gifts": approved["gift_count"],
        "timestamp": datetime.now().isoformat()
    }

@materialize(acquisition_provenance_database,
             tags=["collection", "provenance", "cross-workspace"],
             asset_deps=[approved_acquisitions, provenance_clearance_records])
def update_provenance_database(approved: Dict, provenance: Dict) -> Dict:
    """
    Cross-workspace materialization: consumed by museum_operations_flow.

    Operations flow reads this as upstream for artwork_insurance_valuations —
    complete provenance chains are required to establish insurable value (legal
    ownership/insurable interest) and historical transaction values for insurance
    benchmarking. Newly acquired works cannot be properly insured without it.
    """
    print("🔒 Updating acquisition provenance database...")
    print("  [Cross-workspace] acquisition_provenance_database → museum_operations_flow::artwork_insurance_valuations")
    time.sleep(0.5)
    return {
        "provenance_records_total": random.randint(2400, 2500),
        "new_records_added": approved["approved_count"],
        "fully_documented_pct": round(random.uniform(0.85, 0.95), 2),
        "cleared_of_claims": provenance["cleared_this_period"],
        "aamd_compliant_pct": round(random.uniform(0.92, 0.98), 2),
        "timestamp": datetime.now().isoformat()
    }


# ============================================================================
# REPORTING LAYER TASKS (Leaf nodes - no downstream)
# ============================================================================

@materialize(acquisitions_pipeline_dashboard,
             tags=["reporting", "dashboard", "leaf"],
             asset_deps=[acquisition_committee_dossiers, acquisition_budget_capacity,
                         acquisition_candidates])
def create_acquisitions_dashboard(dossiers: Dict, budget: Dict, candidates: Dict) -> Dict:
    """Multi-upstream (3), no downstream - terminal leaf node."""
    print("📊 Creating acquisitions pipeline dashboard...")
    time.sleep(0.4)
    return {
        "dashboard": "acquisitions_pipeline",
        "pipeline_count": candidates["total_pipeline"],
        "committee_ready": dossiers["ready_for_committee"],
        "budget_available": budget["remaining_capacity"],
        "status": "terminal_leaf_node"
    }

@materialize(collection_development_report,
             tags=["reporting", "report", "leaf"],
             asset_deps=[approved_acquisitions, acquisition_candidates, art_market_comparables])
def create_collection_development_report(approved: Dict, candidates: Dict,
                                          market: Dict) -> Dict:
    """Multi-upstream (3), no downstream - terminal leaf node."""
    print("📈 Creating collection development strategic report...")
    time.sleep(0.4)
    return {
        "report": "collection_development",
        "acquisitions_this_period": approved["approved_count"],
        "pipeline_health": candidates["avg_candidate_score"],
        "market_value_avg": market["avg_market_value"],
        "status": "terminal_leaf_node"
    }


# ============================================================================
# FLOW ORCHESTRATION - Flow of Flows Architecture
# ============================================================================

@flow(name="Isolated Acquisitions Assets Flow", log_prints=True)
def isolated_acquisitions_assets_flow():
    """Subflow for isolated governance reference assets."""
    print("\n🔹 ISOLATED ACQUISITIONS ASSETS FLOW")
    print("=" * 80)
    policy = upload_acquisition_policy()
    print(f"✅ Policy manual v{policy['version']} uploaded")
    return {"policy": policy}


@flow(name="Acquisitions Data Ingestion Flow", log_prints=True, task_runner=DaskTaskRunner())
def acquisitions_data_ingestion_flow():
    """Subflow for all raw data ingestion - root nodes."""
    print("\n🔹 ACQUISITIONS DATA INGESTION FLOW")
    print("=" * 80)

    auction = ingest_auction_results()
    dealers = ingest_dealer_submissions()
    gifts = ingest_gift_intake_forms()
    provenance = ingest_provenance_research()
    claims = ingest_repatriation_claims()

    print(f"✅ Ingested: {auction['lots_captured']} auction lots, "
          f"{dealers['submissions']} dealer submissions, "
          f"{gifts['intake_forms']} gift proposals, "
          f"{provenance['active_research_files']} provenance files")

    return {
        "auction": auction, "dealers": dealers, "gifts": gifts,
        "provenance": provenance, "claims": claims
    }


@flow(name="Art Market Analysis Flow", log_prints=True, task_runner=DaskTaskRunner())
def art_market_analysis_flow(ingestion_data: Dict):
    """Subflow for art market data transformation and analysis."""
    print("\n🔹 ART MARKET ANALYSIS FLOW")
    print("=" * 80)

    market_prices = clean_auction_results(ingestion_data["auction"])
    comparables = build_market_comparables(market_prices, ingestion_data["dealers"])

    print(f"✅ Market analysis: {comparables['comparables_analyzed']} comparables, "
          f"avg value ${comparables['avg_market_value']:,}")

    return {"market_prices": market_prices, "comparables": comparables}


@flow(name="Due Diligence Flow", log_prints=True, task_runner=DaskTaskRunner())
def due_diligence_flow(ingestion_data: Dict):
    """Subflow for provenance research, ethics review, and gift vetting."""
    print("\n🔹 DUE DILIGENCE FLOW")
    print("=" * 80)

    vetted_gifts = vet_gift_proposals(ingestion_data["gifts"])
    provenance_records = process_provenance_clearance(
        ingestion_data["provenance"], ingestion_data["claims"]
    )
    ethics_status = assess_ethics_compliance(provenance_records)

    print(f"✅ Due diligence: {vetted_gifts['vetted_proposals']} gifts vetted, "
          f"{ethics_status['cleared_works']} works ethics-cleared, "
          f"{ethics_status['blocked_works']} blocked")

    return {
        "vetted_gifts": vetted_gifts,
        "provenance_records": provenance_records,
        "ethics_status": ethics_status
    }


@flow(name="Acquisitions Committee Flow", log_prints=True, task_runner=DaskTaskRunner())
def acquisitions_committee_flow(market_data: Dict, due_diligence_data: Dict):
    """
    Subflow for committee preparation.

    Reads cross-workspace upstream assets from museum_operations_flow:
    - artwork_condition_reports  → conservation feasibility
    - financial_health_score     → acquisition budget capacity
    """
    print("\n🔹 ACQUISITIONS COMMITTEE FLOW")
    print("=" * 80)
    print("  [Cross-workspace] Reading upstream assets from museum_operations_flow")

    # Mock cross-workspace data (in production these come from the ops workspace)
    ops_condition_reports = generate_mock_ops_data("artwork_condition_reports")
    ops_financial_health = generate_mock_ops_data("financial_health_score")

    print(f"  → artwork_condition_reports: {ops_condition_reports['lab_backlog_days']} day backlog")
    print(f"  → financial_health_score: {ops_financial_health['health_score']} ({ops_financial_health['rating']})")

    candidates = score_acquisition_candidates(
        market_data["comparables"],
        due_diligence_data["vetted_gifts"],
        due_diligence_data["ethics_status"]
    )
    feasibility = assess_conservation_feasibility(candidates, ops_condition_reports)
    budget = calculate_budget_capacity(market_data["comparables"], ops_financial_health)
    dossiers = compile_committee_dossiers(
        candidates, due_diligence_data["ethics_status"], feasibility, budget
    )

    print(f"✅ Committee prep: {dossiers['dossiers_compiled']} dossiers compiled, "
          f"${budget['remaining_capacity']:,} budget available")

    return {
        "candidates": candidates, "feasibility": feasibility,
        "budget": budget, "dossiers": dossiers
    }


@flow(name="Collection Integration Flow", log_prints=True)
def collection_integration_flow(committee_data: Dict, due_diligence_data: Dict):
    """
    Subflow for formal acquisition approval and collection cataloguing.

    Materializes cross-workspace assets consumed by museum_operations_flow:
    - newly_catalogued_artworks       → ops conservation_schedule
    - acquisition_provenance_database → ops artwork_insurance_valuations
    """
    print("\n🔹 COLLECTION INTEGRATION FLOW")
    print("=" * 80)
    print("  [Cross-workspace] Materializing assets consumed by museum_operations_flow")

    approved = record_approved_acquisitions(committee_data["dossiers"])
    catalogued = catalogue_new_acquisitions(approved)
    provenance_db = update_provenance_database(
        approved, due_diligence_data["provenance_records"]
    )

    print(f"✅ Collection integration: {catalogued['newly_catalogued']} works catalogued, "
          f"{provenance_db['new_records_added']} provenance records added")
    print(f"  → newly_catalogued_artworks ready for museum_operations_flow conservation_schedule")
    print(f"  → acquisition_provenance_database ready for museum_operations_flow artwork_insurance_valuations")

    return {"approved": approved, "catalogued": catalogued, "provenance_db": provenance_db}


@flow(name="Acquisitions Reporting Flow", log_prints=True, task_runner=DaskTaskRunner())
def acquisitions_reporting_flow(committee_data: Dict, collection_data: Dict):
    """Subflow for acquisitions dashboards and strategic reports."""
    print("\n🔹 ACQUISITIONS REPORTING FLOW")
    print("=" * 80)

    dashboard = create_acquisitions_dashboard(
        committee_data["dossiers"],
        committee_data["budget"],
        committee_data["candidates"]
    )
    report = create_collection_development_report(
        collection_data["approved"],
        committee_data["candidates"],
        committee_data["dossiers"]  # reuse dossiers dict - has avg_market_value
    )

    print(f"✅ Reporting: dashboard updated, collection development report generated")
    return {"dashboard": dashboard, "report": report}


@flow(name="Museum Acquisitions Flow", log_prints=True)
def museum_acquisitions_flow():
    """
    Primary orchestration flow for national museum acquisitions.

    Demonstrates cross-workspace asset dependencies with museum_operations_flow:
    - Reads artwork_condition_reports + financial_health_score from ops workspace
    - Materializes newly_catalogued_artworks + acquisition_provenance_database for ops workspace
    """
    print("\n" + "=" * 80)
    print("🏛️  MUSEUM ACQUISITIONS FLOW")
    print("   Workspace: national-museum-acquisitions")
    print("   Cross-workspace deps: museum-admin (museum_operations_flow)")
    print("=" * 80)

    isolated_acquisitions_assets_flow()

    ingestion_data = acquisitions_data_ingestion_flow()

    market_data = art_market_analysis_flow(ingestion_data)
    due_diligence_data = due_diligence_flow(ingestion_data)

    committee_data = acquisitions_committee_flow(market_data, due_diligence_data)
    collection_data = collection_integration_flow(committee_data, due_diligence_data)

    acquisitions_reporting_flow(committee_data, collection_data)

    print("\n" + "=" * 80)
    print("✅ MUSEUM ACQUISITIONS FLOW COMPLETE")
    print(f"   {collection_data['catalogued']['newly_catalogued']} new works catalogued → ops conservation_schedule")
    print(f"   {collection_data['provenance_db']['new_records_added']} provenance records → ops artwork_insurance_valuations")
    print("=" * 80)


if __name__ == "__main__":
    museum_acquisitions_flow()
