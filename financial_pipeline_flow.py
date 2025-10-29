"""
Financial Data Pipeline Flow - A Production-Grade Financial Data Engineering Demo

This flow demonstrates:
1. Five different assets with diverse URI prefixes (s3, gcs, azure, snowflake, local)
2. Three distinct artifacts showcasing data quality, risk analytics, and reconciliation
3. Nested subflow architecture for market data ingestion
4. Comprehensive logging throughout the pipeline
5. AI-generated executive summaries using Marvin
"""
import time
import random
from datetime import datetime, timedelta
from typing import Literal
import os

from prefect import flow, task
from prefect.assets import Asset, AssetProperties, materialize
from prefect.artifacts import create_markdown_artifact
import marvin
from pydantic_ai.models.anthropic import AnthropicModel


divider = "=" * 10

# Define Assets for the financial data pipeline
market_data_records = Asset(
    key="s3://financial-pipeline/market-data",
    properties=AssetProperties(
        name="Market Data Records",
        description="""# Market Data Records Asset

## Overview
This asset contains **real-time and historical market data** ingested from multiple global exchanges including NYSE, NASDAQ, LSE, and other major trading venues. The data is stored in a highly optimized columnar format in Amazon S3 for efficient querying and long-term retention.

## Data Contents
The market data records include:
- **Quote Data**: Real-time bid/ask prices with microsecond timestamps
- **Trade Data**: Executed trades with volume, price, and venue information
- **OHLCV Bars**: Time-series aggregations at 1min, 5min, 15min, 1hour, and daily intervals
- **Order Book Snapshots**: Level 2 market depth data for liquidity analysis
- **Corporate Actions**: Dividends, splits, and other material events affecting securities

## Technical Specifications
- **Storage Format**: Parquet with Snappy compression
- **Partitioning Strategy**: Partitioned by date (YYYY-MM-DD) and exchange for optimal query performance
- **Update Frequency**: Real-time streaming updates with 10-500ms latency
- **Retention Policy**: Hot storage for 90 days, cold storage for 7 years per regulatory requirements
- **Data Volume**: Approximately 50-100GB per trading day across all exchanges

## Data Quality & Validation
All ingested data passes through comprehensive validation pipelines that check for:
- Schema compliance and data type validation
- Timestamp sequencing and gap detection
- Duplicate record identification and removal
- Price reasonability checks (outlier detection)
- Cross-exchange reference data validation

## Access Patterns
This asset is primarily consumed by:
- **Trading Systems**: For order execution and market making algorithms
- **Risk Management**: For real-time exposure monitoring and VaR calculations
- **Quantitative Research**: For backtesting strategies and alpha discovery
- **Compliance**: For best execution analysis and trade surveillance
- **Data Science**: For machine learning model training and feature engineering

## Governance & Security
- **Classification**: Confidential - Proprietary Trading Data
- **Access Control**: Role-based access with audit logging
- **Encryption**: Server-side encryption (SSE-S3) with KMS key management
- **Compliance**: SOC 2 Type II, SOX, and SEC Rule 17a-4 compliant storage
""",
        owners=["data-engineering", "trading-desk"],
    )
)

risk_metrics = Asset(
    key="gcs://trading-analytics/risk-metrics",
    properties=AssetProperties(
        name="Risk Metrics",
        description="""# Risk Metrics Asset

## Overview
This asset contains **comprehensive risk analytics** computed across all trading portfolios, providing real-time visibility into market risk exposure, concentration risk, and regulatory capital requirements. The metrics are calculated using industry-standard models and stored in Google Cloud Storage for high-performance analytics.

## Key Metrics Computed
### Market Risk Metrics
- **Value at Risk (VaR)**: Historical and parametric VaR at 95%, 99%, and 99.9% confidence levels
- **Expected Shortfall (ES/CVaR)**: Conditional Value at Risk for tail risk assessment
- **Stress Testing**: Scenario-based stress tests simulating market crashes, rate shocks, and crisis events
- **Greeks**: Delta, Gamma, Vega, Theta, and Rho for options and derivatives portfolios

### Portfolio Risk Metrics
- **Beta & Correlation**: Market correlation and systematic risk exposure
- **Sharpe Ratio**: Risk-adjusted return analysis
- **Maximum Drawdown**: Historical peak-to-trough decline analysis
- **Volatility**: Realized and implied volatility across asset classes
- **Tracking Error**: Active risk vs. benchmark for all strategies

### Concentration Risk
- **Position Limits**: Single-name, sector, and country concentration analysis
- **Liquidity Risk**: Days-to-liquidate calculations based on average daily volume
- **Counterparty Exposure**: Aggregated exposure by counterparty with credit limits

## Calculation Methodology
Risk metrics are computed using:
- **Monte Carlo Simulation**: 10,000+ scenarios for complex derivatives
- **Historical Simulation**: Rolling 252-day lookback for empirical distributions
- **Parametric Models**: GARCH, EWMA, and other volatility forecasting models
- **Copula Methods**: For capturing tail dependencies and correlation breakdown

## Data Lineage & Dependencies
Risk metrics depend on:
- Market data from multiple exchanges (upstream dependency)
- Portfolio positions and transactions (upstream dependency)
- Volatility surfaces and correlation matrices (reference data)
- Yield curves and rate scenarios (market data feeds)
""",
        owners=["risk-management", "quant-team"],
    )
)

audit_trails = Asset(
    key="azure://compliance-vault/audit-trails",
    properties=AssetProperties(
        name="Audit Trails",
        description="""# Audit Trails Asset

## Overview
This asset maintains **comprehensive audit trails** for all trading, operational, and system activities to meet stringent regulatory requirements and support internal investigations. Stored in Azure Blob Storage with immutable, write-once-read-many (WORM) properties to ensure tamper-proof record keeping.

## Audit Trail Categories
### Trading Activity Audit Trails
- **Order Lifecycle**: Complete history of order creation, modification, cancellation, and execution
- **Trade Amendments**: All post-trade modifications with justification and approver information
- **Execution Quality**: Best execution analysis with venue selection rationale
- **Market Making Activity**: Quote updates, spreads, and liquidity provision records

### Compliance & Regulatory Audit Trails
- **Position Limit Monitoring**: Daily position limit checks with breach notifications
- **Concentration Rules**: Compliance with single-issuer, sector, and geographic limits
- **Trade Surveillance**: Pattern detection for insider trading, market manipulation, and wash trading
- **Reg SHO Compliance**: Short sale locate records and marking validation
- **MiFID II/MiFIR**: Clock synchronization records and transaction reporting audit trails

### Operational Audit Trails
- **Access Logs**: User authentication, authorization, and privileged access records
- **Data Modifications**: All changes to reference data, static data, and configuration
- **Reconciliation History**: Break identification, resolution, and sign-off records
- **Exception Handling**: Trade breaks, settlement failures, and operational incidents

### Anti-Money Laundering (AML) & KYC
- **Customer Due Diligence**: KYC verification records and periodic reviews
- **Transaction Monitoring**: Suspicious activity detection and SAR filing records
- **Sanctions Screening**: OFAC and PEP screening results with match disposition

## Regulatory Compliance Requirements
This asset satisfies requirements from:
- **SEC Rule 17a-4**: Electronic record retention for broker-dealers
- **FINRA Rule 4511**: Books and records requirements
- **Dodd-Frank Act**: Swap data recordkeeping and reporting
- **GDPR**: Right to be forgotten with compliant redaction procedures
- **SOX Section 404**: Internal controls over financial reporting
""",
        owners=["compliance", "legal"],
    )
)

portfolio_snapshots = Asset(
    key="snowflake://data-warehouse/portfolio-snapshots",
    properties=AssetProperties(
        name="Portfolio Snapshots",
        description="""# Portfolio Snapshots Asset

## Overview
This asset contains **point-in-time snapshots** of all portfolio positions, cash balances, and valuations across the entire organization. Stored in Snowflake Data Cloud, these snapshots provide a complete, consistent view of the firm's financial position at specific moments in time, enabling accurate performance attribution, risk analysis, and regulatory reporting.

## Snapshot Contents
### Position Data
- **Equity Holdings**: Long and short positions with cost basis, market value, and unrealized P&L
- **Fixed Income**: Bonds with accrued interest, duration, convexity, and credit ratings
- **Derivatives**: Options, futures, swaps with notional amounts, Greeks, and margin requirements
- **Foreign Exchange**: FX spot and forward positions with currency exposures
- **Alternative Investments**: Private equity, hedge funds, and real estate holdings

### Valuation Details
- **Mark-to-Market Pricing**: End-of-day pricing from multiple market data vendors
- **Accruals**: Interest accruals, dividend receivables, and fee accruals
- **Unrealized P&L**: Mark-to-market gains/losses by position and portfolio
- **Realized P&L**: Closed position P&L for the period
- **Total Return**: Time-weighted and money-weighted returns

### Cash & Margin
- **Cash Positions**: Multi-currency cash balances across prime brokers and custodians
- **Margin Requirements**: Initial margin, variation margin, and excess liquidity
- **Collateral**: Posted and received collateral by counterparty
- **Pending Settlements**: Trade date vs. settlement date cash impacts

### Account Hierarchy
Snapshots capture the complete account structure:
- **Master Fund Level**: Consolidated positions across all strategies
- **Sub-Fund Level**: Individual strategy or product-level positions
- **Account Level**: Positions by prime broker, custodian, or legal entity
- **Desk Level**: Attribution by trading desk or portfolio manager

## Snapshot Frequency & Timing
- **Daily Snapshots**: Captured at market close (4:00 PM ET) every business day
- **Intraday Snapshots**: Hourly snapshots during market hours (9:30 AM - 4:00 PM ET)
- **Month-End Snapshots**: Enhanced snapshots with additional audit and reconciliation data
- **Year-End Snapshots**: Special regulatory and tax reporting snapshots
""",
        owners=["portfolio-management", "accounting"],
    )
)

reconciliation_reports = Asset(
    key="local://financial-reports/reconciliation-reports",
    properties=AssetProperties(
        name="Reconciliation Reports",
        description="""# Reconciliation Reports Asset

## Overview

## Reconciliation Scope
### Cash Reconciliation
- **Bank Accounts**: Reconcile cash balances across all bank accounts and currencies
- **Prime Broker Cash**: Compare cash positions to prime broker statements
- **Settlement Cash**: Reconcile pending settlements and DVP/RVP transactions
- **FX Translation**: Validate foreign exchange translations and rates used

### Position Reconciliation
- **Securities Holdings**: Compare long and short positions to custodian records
- **Corporate Actions**: Verify dividends, splits, mergers, and other corporate actions
- **Loan Positions**: Reconcile stock borrow/loan positions to securities lending platforms
- **Derivatives Positions**: Compare OTC derivatives to trade repositories and CCPs

### Transaction Reconciliation
- **Trade Confirmation**: Match internal trade records to broker confirmations
- **Settlement Status**: Reconcile settlement instructions and confirmations
- **Fee Validation**: Verify commission charges, platform fees, and other costs
- **Interest & Accruals**: Reconcile interest charges and credits to account statements

### P&L Reconciliation
- **Daily P&L**: Compare internal P&L calculations to prime broker P&L statements
- **Pricing Differences**: Identify and explain valuation discrepancies
- **Fee Amortization**: Reconcile management fees and performance allocations
- **Expense Attribution**: Validate operational expense allocation across funds

### Break Severity Levels
- **Critical (>$1M)**: Escalated immediately to CFO and operations leadership
- **High ($100K-$1M)**: Daily management review with 3-day resolution SLA
- **Medium ($10K-$100K)**: Weekly review with 7-day resolution target
- **Low (<$10K)**: Monthly review with 30-day resolution target

## Operational Workflow
Daily reconciliation process:
1. **Data Collection** (6:00 AM): Download statements from prime brokers and custodians
2. **Automated Matching** (6:30 AM): Run reconciliation engines to identify breaks
3. **Report Generation** (7:00 AM): Generate detailed reconciliation reports
4. **Review & Investigation** (8:00 AM - 12:00 PM): Operations team investigates breaks
5. **Escalation** (As Needed): High-severity breaks escalated to management
6. **Resolution & Adjustment** (Throughout Day): Post adjustments to resolve breaks
7. **Sign-Off** (5:00 PM): Manager review and approval of reconciliation results
""",
        owners=["operations", "accounting"],
    )
)


def get_anthropic_model():
    """Get Anthropic model if API key is available."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key:
        return AnthropicModel(model_name="claude-3-5-sonnet-latest")
    else:
        return None


anthropic_model = get_anthropic_model()

# Marvin agent for financial analysis
financial_analyst = marvin.Agent(
    name="Financial Analyst",
    instructions="You are a senior financial analyst with expertise in data engineering, risk management, and regulatory compliance. Generate professional, concise summaries of financial data pipeline operations, focusing on data quality, risk metrics, and operational efficiency. Use financial terminology appropriately.",
    model=anthropic_model if anthropic_model else None
)


@materialize(market_data_records, tags=["ingestion", "market-data", "real-time"])
def fetch_market_data(exchange: str, symbols: list[str]) -> dict:
    """Fetch market data from exchange APIs."""
    print(f"📊 [INGESTION] Connecting to {exchange} exchange...")
    time.sleep(1.5)

    print(f"📈 [INGESTION] Fetching real-time quotes for {len(symbols)} symbols...")
    time.sleep(1)

    # Simulate market data fetch
    market_data = {
        "exchange": exchange,
        "symbols": symbols,
        "records_fetched": len(symbols) * random.randint(50, 200),
        "timestamp": datetime.now().isoformat(),
        "latency_ms": random.randint(10, 50),
        "data_quality_score": round(random.uniform(0.95, 0.99), 3)
    }

    print(f"✅ [INGESTION] Fetched {market_data['records_fetched']} records from {exchange}")
    print(f"⏱️  [INGESTION] Average latency: {market_data['latency_ms']}ms")
    print(f"🎯 [INGESTION] Data quality score: {market_data['data_quality_score']}")

    return market_data


@task(tags=["validation", "data-quality", "monitoring"])
def create_data_quality_report(market_data: dict) -> dict:
    """Create data quality artifact showing validation results."""
    print(f"🔍 [VALIDATION] Running data quality checks on {market_data['exchange']} data...")
    time.sleep(1)

    # Simulate validation checks
    validation_results = {
        "total_records": market_data["records_fetched"],
        "valid_records": int(market_data["records_fetched"] * market_data["data_quality_score"]),
        "null_values": random.randint(0, 10),
        "duplicate_records": random.randint(0, 5),
        "schema_violations": random.randint(0, 3),
        "timestamp_gaps": random.randint(0, 2)
    }

    validation_results["invalid_records"] = (
        validation_results["total_records"] - validation_results["valid_records"]
    )

    print(f"✅ [VALIDATION] Valid records: {validation_results['valid_records']}/{validation_results['total_records']}")
    print(f"⚠️  [VALIDATION] Issues found: {validation_results['invalid_records']} invalid records")

    # Create data quality artifact with markdown table
    quality_markdown = f"""## 📊 Market Data Quality Report
### {market_data['exchange']} Exchange
**Report Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

#### Data Quality Score: {market_data['data_quality_score']*100:.1f}%

| Metric | Count | Status |
|--------|-------|--------|
| Total Records | {validation_results["total_records"]} | ✅ |
| Valid Records | {validation_results["valid_records"]} | ✅ |
| Invalid Records | {validation_results["invalid_records"]} | {"⚠️" if validation_results["invalid_records"] > 10 else "✅"} |
| Null Values | {validation_results["null_values"]} | {"⚠️" if validation_results["null_values"] > 5 else "✅"} |
| Duplicates | {validation_results["duplicate_records"]} | {"⚠️" if validation_results["duplicate_records"] > 3 else "✅"} |
| Schema Violations | {validation_results["schema_violations"]} | {"⚠️" if validation_results["schema_violations"] > 2 else "✅"} |
| Timestamp Gaps | {validation_results["timestamp_gaps"]} | {"⚠️" if validation_results["timestamp_gaps"] > 1 else "✅"} |
"""

    create_markdown_artifact(
        key=f"data-quality-{market_data['exchange'].lower()}",
        markdown=quality_markdown,
        description=f"Data quality report for {market_data['exchange']} market data"
    )

    market_data["validation"] = validation_results
    print(f"📋 [VALIDATION] Data quality report artifact created")

    return market_data


@flow(name="Market Data Ingestion", log_prints=True)
def market_data_ingestion_subflow(exchange: str, symbols: list[str]) -> dict:
    """Subflow for ingesting and validating market data from exchanges."""
    print(f"\n💹 [SUBFLOW] Starting market data ingestion pipeline for {exchange}...")
    print(f"📌 [SUBFLOW] Symbols: {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")

    # Fetch market data (creates asset)
    market_data = fetch_market_data(exchange, symbols)

    # Validate and create quality report (creates artifact)
    validated_data = create_data_quality_report(market_data)

    print(f"✅ [SUBFLOW] Market data ingestion complete for {exchange}")
    return validated_data


@materialize(risk_metrics, tags=["risk", "analytics", "var"])
def calculate_risk_metrics(market_data: dict) -> dict:
    """Calculate risk metrics including VaR and Greeks."""
    print(f"\n🎲 [RISK] Calculating risk metrics for portfolio...")
    time.sleep(1.5)

    print(f"📊 [RISK] Computing Value at Risk (VaR) at 95% confidence...")
    time.sleep(1)

    risk_data = {
        "portfolio_value": random.randint(50_000_000, 150_000_000),
        "var_95": random.randint(500_000, 2_000_000),
        "var_99": random.randint(800_000, 3_000_000),
        "expected_shortfall": random.randint(1_000_000, 4_000_000),
        "beta": round(random.uniform(0.8, 1.2), 3),
        "sharpe_ratio": round(random.uniform(1.2, 2.5), 3),
        "max_drawdown": round(random.uniform(0.05, 0.15), 3),
        "volatility": round(random.uniform(0.12, 0.25), 3),
        "timestamp": datetime.now().isoformat()
    }

    print(f"💰 [RISK] Portfolio Value: ${risk_data['portfolio_value']:,}")
    print(f"⚠️  [RISK] VaR (95%): ${risk_data['var_95']:,}")
    print(f"📈 [RISK] Sharpe Ratio: {risk_data['sharpe_ratio']}")
    print(f"📉 [RISK] Volatility: {risk_data['volatility']*100:.1f}%")

    # Create risk analytics artifact with markdown table
    risk_markdown = f"""## 🎲 Risk Analytics Dashboard
**Analysis Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Portfolio Overview
- **Total Value:** ${risk_data['portfolio_value']:,}
- **Beta:** {risk_data['beta']} (Market Correlation)
- **Sharpe Ratio:** {risk_data['sharpe_ratio']} (Risk-Adjusted Return)

| Risk Metric | Value | Status |
|-------------|-------|--------|
| Value at Risk (95%) | ${risk_data['var_95']:,} | {"✅ Within Limits" if risk_data['var_95'] < 1_500_000 else "⚠️ Elevated"} |
| Value at Risk (99%) | ${risk_data['var_99']:,} | {"✅ Within Limits" if risk_data['var_99'] < 2_500_000 else "⚠️ Elevated"} |
| Expected Shortfall | ${risk_data['expected_shortfall']:,} | {"✅ Acceptable" if risk_data['expected_shortfall'] < 3_000_000 else "⚠️ High"} |
| Maximum Drawdown | {risk_data['max_drawdown']*100:.1f}% | {"✅ Acceptable" if risk_data['max_drawdown'] < 0.12 else "⚠️ High"} |
| Portfolio Volatility | {risk_data['volatility']*100:.1f}% | {"✅ Normal" if risk_data['volatility'] < 0.20 else "⚠️ High"} |
"""

    create_markdown_artifact(
        key="risk-analytics-dashboard",
        markdown=risk_markdown,
        description="Real-time risk metrics and exposure analysis"
    )

    print(f"📋 [RISK] Risk analytics artifact created")

    return risk_data


@materialize(audit_trails, tags=["compliance", "audit", "regulatory"])
def run_compliance_checks(market_data: dict, risk_data: dict) -> dict:
    """Run compliance checks and generate audit trails."""
    print(f"\n⚖️  [COMPLIANCE] Initiating regulatory compliance checks...")
    time.sleep(1.5)

    print(f"📋 [COMPLIANCE] Checking position limits and concentration rules...")
    time.sleep(1)

    print(f"🔍 [COMPLIANCE] Validating trade reporting requirements...")
    time.sleep(1)

    compliance_data = {
        "checks_performed": random.randint(15, 25),
        "checks_passed": random.randint(14, 24),
        "position_limit_breaches": random.randint(0, 2),
        "concentration_warnings": random.randint(0, 3),
        "reporting_delays": random.randint(0, 1),
        "aml_flags": random.randint(0, 1),
        "timestamp": datetime.now().isoformat()
    }

    compliance_data["checks_failed"] = (
        compliance_data["checks_performed"] - compliance_data["checks_passed"]
    )

    compliance_status = "PASS" if compliance_data["checks_failed"] == 0 else "WARNING"

    print(f"✅ [COMPLIANCE] Checks passed: {compliance_data['checks_passed']}/{compliance_data['checks_performed']}")
    print(f"⚠️  [COMPLIANCE] Position limit breaches: {compliance_data['position_limit_breaches']}")
    print(f"🎯 [COMPLIANCE] Overall status: {compliance_status}")

    return compliance_data


@materialize(portfolio_snapshots, tags=["portfolio", "positions", "valuation"])
def snapshot_portfolio_positions() -> dict:
    """Capture point-in-time snapshot of all portfolio positions."""
    print(f"\n📸 [PORTFOLIO] Capturing portfolio snapshot...")
    time.sleep(1.5)

    print(f"💼 [PORTFOLIO] Aggregating positions across all accounts...")
    time.sleep(1)

    print(f"💵 [PORTFOLIO] Calculating mark-to-market valuations...")
    time.sleep(1)

    snapshot_data = {
        "total_accounts": random.randint(50, 150),
        "total_positions": random.randint(500, 2000),
        "equity_positions": random.randint(300, 1200),
        "fixed_income_positions": random.randint(100, 500),
        "derivatives_positions": random.randint(50, 300),
        "cash_balance": random.randint(5_000_000, 20_000_000),
        "total_nav": random.randint(80_000_000, 200_000_000),
        "snapshot_timestamp": datetime.now().isoformat()
    }

    print(f"📊 [PORTFOLIO] Total positions: {snapshot_data['total_positions']:,}")
    print(f"💰 [PORTFOLIO] Total NAV: ${snapshot_data['total_nav']:,}")
    print(f"💵 [PORTFOLIO] Cash balance: ${snapshot_data['cash_balance']:,}")
    print(f"✅ [PORTFOLIO] Snapshot captured successfully")

    return snapshot_data


@materialize(reconciliation_reports, tags=["reconciliation", "operations", "accounting"])
def reconcile_transactions(portfolio_data: dict) -> dict:
    """Reconcile transactions between internal books and custodian records."""
    print(f"\n🔄 [RECONCILIATION] Starting daily reconciliation process...")
    time.sleep(1.5)

    print(f"📊 [RECONCILIATION] Comparing internal records with custodian data...")
    time.sleep(1)

    print(f"🔍 [RECONCILIATION] Identifying breaks and discrepancies...")
    time.sleep(1)

    recon_data = {
        "transactions_reconciled": random.randint(1000, 5000),
        "matched_transactions": random.randint(980, 4950),
        "breaks_identified": random.randint(5, 50),
        "cash_breaks": random.randint(0, 10),
        "position_breaks": random.randint(2, 20),
        "resolved_breaks": random.randint(3, 30),
        "pending_investigation": random.randint(1, 15),
        "total_break_amount": random.randint(10_000, 500_000),
        "timestamp": datetime.now().isoformat()
    }

    recon_data["unresolved_breaks"] = (
        recon_data["breaks_identified"] - recon_data["resolved_breaks"]
    )

    match_rate = (recon_data["matched_transactions"] / recon_data["transactions_reconciled"]) * 100

    print(f"✅ [RECONCILIATION] Matched: {recon_data['matched_transactions']:,}/{recon_data['transactions_reconciled']:,}")
    print(f"⚠️  [RECONCILIATION] Breaks identified: {recon_data['breaks_identified']}")
    print(f"🔧 [RECONCILIATION] Resolved: {recon_data['resolved_breaks']}")
    print(f"📊 [RECONCILIATION] Match rate: {match_rate:.2f}%")

    # Create reconciliation artifact with markdown table
    recon_markdown = f"""## 🔄 Daily Reconciliation Summary
**Reconciliation Date:** {datetime.now().strftime('%Y-%m-%d')}
**Processing Time:** {datetime.now().strftime('%H:%M:%S')}

### Match Rate: {match_rate:.2f}%

| Category | Count/Amount | Status |
|----------|--------------|--------|
| Total Transactions | {recon_data['transactions_reconciled']:,} | ✅ |
| Matched Transactions | {recon_data['matched_transactions']:,} | ✅ |
| Total Breaks | {recon_data['breaks_identified']} | {"⚠️" if recon_data['breaks_identified'] > 20 else "✅"} |
| Cash Breaks | {recon_data['cash_breaks']} | {"⚠️" if recon_data['cash_breaks'] > 5 else "✅"} |
| Position Breaks | {recon_data['position_breaks']} | {"⚠️" if recon_data['position_breaks'] > 10 else "✅"} |
| Resolved Breaks | {recon_data['resolved_breaks']} | ✅ |
| Pending Investigation | {recon_data['pending_investigation']} | {"⚠️" if recon_data['pending_investigation'] > 10 else "✅"} |
| Total Break Amount | ${recon_data['total_break_amount']:,} | {"⚠️" if recon_data['total_break_amount'] > 200_000 else "✅"} |
"""

    create_markdown_artifact(
        key="daily-reconciliation-summary",
        markdown=recon_markdown,
        description="Daily reconciliation summary with break analysis"
    )

    print(f"📋 [RECONCILIATION] Reconciliation artifact created")

    return recon_data


@task(tags=["reporting", "executive", "ai-generated"])
def generate_executive_summary(
    market_data: dict,
    risk_data: dict,
    compliance_data: dict,
    portfolio_data: dict,
    recon_data: dict
) -> str:
    """Generate AI-powered executive summary of the pipeline run."""
    print(f"\n📝 [REPORTING] Generating executive summary with AI analyst...")
    time.sleep(1)

    if anthropic_model:
        try:
            with marvin.Thread(id="financial-pipeline-summary") as thread:
                prompt = f"""Generate a professional executive summary for today's financial data pipeline execution.

Key Metrics:
- Market Data: {market_data['records_fetched']} records from {market_data['exchange']}
- Data Quality: {market_data['data_quality_score']*100:.1f}%
- Portfolio Value: ${risk_data['portfolio_value']:,}
- VaR (95%): ${risk_data['var_95']:,}
- Sharpe Ratio: {risk_data['sharpe_ratio']}
- Compliance Checks: {compliance_data['checks_passed']}/{compliance_data['checks_performed']} passed
- Reconciliation: {recon_data['matched_transactions']:,}/{recon_data['transactions_reconciled']:,} matched
- Breaks: {recon_data['breaks_identified']} identified, {recon_data['resolved_breaks']} resolved

Write a concise 3-paragraph executive summary highlighting:
1. Pipeline execution overview and data quality
2. Risk metrics and portfolio performance
3. Operational efficiency and key issues to address
"""
                summary = financial_analyst.run(prompt)
            print("✅ [REPORTING] AI-generated executive summary complete")
            return summary
        except Exception as e:
            print(f"⚠️  [REPORTING] AI analyst unavailable, using template summary: {e}")

    # Fallback summary
    return f"""
## Executive Summary - Financial Data Pipeline

### Pipeline Execution Overview
Today's financial data pipeline successfully processed {market_data['records_fetched']} market data records
from {market_data['exchange']} with a data quality score of {market_data['data_quality_score']*100:.1f}%.
The pipeline executed all critical components including market data ingestion, risk analytics,
compliance validation, portfolio snapshotting, and transaction reconciliation.

### Risk and Portfolio Performance
Current portfolio value stands at ${risk_data['portfolio_value']:,} with a Value at Risk (95% confidence)
of ${risk_data['var_95']:,}. The portfolio demonstrates a Sharpe ratio of {risk_data['sharpe_ratio']},
indicating strong risk-adjusted returns. Portfolio volatility is {risk_data['volatility']*100:.1f}%,
within acceptable ranges for the current market environment.

### Operational Efficiency
Compliance checks achieved a {(compliance_data['checks_passed']/compliance_data['checks_performed']*100):.1f}%
pass rate with {compliance_data['checks_failed']} items requiring attention. Daily reconciliation processed
{recon_data['transactions_reconciled']:,} transactions with {recon_data['breaks_identified']} breaks identified,
of which {recon_data['resolved_breaks']} have been resolved. Key focus areas include resolving the remaining
{recon_data['unresolved_breaks']} reconciliation breaks and monitoring compliance warnings.
"""


@flow(name="Financial Data Pipeline", log_prints=True)
def financial_data_pipeline_flow(
    exchanges: list[str] = None,
    symbols_per_exchange: int = 20
):
    """
    Production-Grade Financial Data Pipeline Flow

    This flow orchestrates a complete financial data engineering pipeline including:
    - Market data ingestion and validation (subflow)
    - Risk metrics calculation with VaR and Greeks
    - Regulatory compliance checks and audit trails
    - Portfolio position snapshotting
    - Transaction reconciliation with break analysis
    - AI-generated executive reporting

    Architecture:
    - Main flow: financial_data_pipeline_flow
      - Subflow: market_data_ingestion_subflow
        - Task: fetch_market_data (creates s3 asset)
        - Task: create_data_quality_report (creates artifact)
      - Task: calculate_risk_metrics (creates gcs asset + artifact)
      - Task: run_compliance_checks (creates azure asset)
      - Task: snapshot_portfolio_positions (creates snowflake asset)
      - Task: reconcile_transactions (creates local asset + artifact)
      - Task: generate_executive_summary (creates summary)

    Args:
        exchanges: List of exchanges to ingest data from
        symbols_per_exchange: Number of symbols to fetch per exchange
    """
    print(divider)
    print("💹 FINANCIAL DATA PIPELINE - PRODUCTION RUN")
    print(divider)
    print(f"⏰ Pipeline Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌍 Execution Environment: Production")
    print()

    if exchanges is None:
        exchanges = ["NYSE", "NASDAQ"]

    # Generate sample symbols
    symbol_prefixes = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM", "BAC", "GS"]

    print("📊 PIPELINE CONFIGURATION:")
    print(f"   💹 Exchanges: {', '.join(exchanges)}")
    print(f"   📈 Symbols per exchange: {symbols_per_exchange}")
    print()

    # Phase 1: Market Data Ingestion (Subflow)
    print(divider)
    print("PHASE 1: MARKET DATA INGESTION")
    print(divider)

    all_market_data = []
    for exchange in exchanges:
        symbols = random.sample(symbol_prefixes, min(len(symbol_prefixes), symbols_per_exchange))
        market_data = market_data_ingestion_subflow(exchange, symbols)
        all_market_data.append(market_data)
        time.sleep(0.5)

    # Use the most recent market data for downstream tasks
    primary_market_data = all_market_data[0]

    # Phase 2: Risk Analytics
    print("\n" + divider)
    print("PHASE 2: RISK ANALYTICS")
    print(divider)
    risk_data = calculate_risk_metrics(primary_market_data)

    # Phase 3: Compliance Checks
    print("\n" + divider)
    print("PHASE 3: REGULATORY COMPLIANCE")
    print(divider)
    compliance_data = run_compliance_checks(primary_market_data, risk_data)

    # Phase 4: Portfolio Snapshot
    print("\n" + divider)
    print("PHASE 4: PORTFOLIO SNAPSHOT")
    print(divider)
    portfolio_data = snapshot_portfolio_positions()

    # Phase 5: Transaction Reconciliation
    print("\n" + divider)
    print("PHASE 5: TRANSACTION RECONCILIATION")
    print(divider)
    recon_data = reconcile_transactions(portfolio_data)

    # Phase 6: Executive Reporting
    print("\n" + divider)
    print("PHASE 6: EXECUTIVE REPORTING")
    print(divider)
    executive_summary = generate_executive_summary(
        primary_market_data,
        risk_data,
        compliance_data,
        portfolio_data,
        recon_data
    )

    # Create final executive summary artifact
    print(f"\n📊 [REPORTING] Creating executive summary artifact...")

    summary_markdown = f"""# 📊 Daily Pipeline Executive Summary
**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Pipeline Execution Time:** {datetime.now().strftime('%H:%M:%S')}

---

{executive_summary}

---

*Generated by Financial Data Pipeline v2.0*
"""

    create_markdown_artifact(
        key="executive-summary-daily",
        markdown=summary_markdown,
        description="Daily executive summary of financial data pipeline"
    )

    print(f"✅ [REPORTING] Executive summary artifact created")

    # Final Summary
    total_records = sum(data['records_fetched'] for data in all_market_data)
    avg_quality = sum(data['data_quality_score'] for data in all_market_data) / len(all_market_data)

    print("\n" + divider)
    print("🎯 PIPELINE EXECUTION SUMMARY")
    print(divider)
    print(f"⏰ Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("📊 DATA PROCESSING:")
    print(f"   💹 Exchanges Processed: {len(exchanges)}")
    print(f"   📈 Total Market Records: {total_records:,}")
    print(f"   🎯 Average Data Quality: {avg_quality*100:.1f}%")
    print()
    print("💰 PORTFOLIO & RISK:")
    print(f"   💼 Portfolio Value: ${risk_data['portfolio_value']:,}")
    print(f"   ⚠️  Value at Risk (95%): ${risk_data['var_95']:,}")
    print(f"   📊 Sharpe Ratio: {risk_data['sharpe_ratio']}")
    print(f"   📈 Total Positions: {portfolio_data['total_positions']:,}")
    print()
    print("⚖️  COMPLIANCE & OPERATIONS:")
    print(f"   ✅ Compliance Pass Rate: {(compliance_data['checks_passed']/compliance_data['checks_performed']*100):.1f}%")
    print(f"   🔄 Reconciliation Match Rate: {(recon_data['matched_transactions']/recon_data['transactions_reconciled']*100):.2f}%")
    print(f"   ⚠️  Breaks to Resolve: {recon_data['unresolved_breaks']}")
    print()
    print("📋 ARTIFACTS GENERATED:")
    print(f"   ✅ Data Quality Reports: {len(all_market_data)}")
    print(f"   ✅ Risk Analytics Dashboard: 1")
    print(f"   ✅ Reconciliation Summary: 1")
    print(f"   ✅ Executive Summary: 1")
    print()
    print("💾 ASSETS MATERIALIZED:")
    print(f"   ✅ s3://financial-pipeline/market-data")
    print(f"   ✅ gcs://trading-analytics/risk-metrics")
    print(f"   ✅ azure://compliance-vault/audit-trails")
    print(f"   ✅ snowflake://data-warehouse/portfolio-snapshots")
    print(f"   ✅ local://financial-reports/reconciliation-reports")
    print(divider)
    print()

    return {
        "exchanges_processed": len(exchanges),
        "total_records": total_records,
        "data_quality": round(avg_quality, 3),
        "portfolio_value": risk_data['portfolio_value'],
        "var_95": risk_data['var_95'],
        "compliance_pass_rate": round((compliance_data['checks_passed']/compliance_data['checks_performed'])*100, 2),
        "recon_match_rate": round((recon_data['matched_transactions']/recon_data['transactions_reconciled'])*100, 2),
        "unresolved_breaks": recon_data['unresolved_breaks'],
        "artifacts_created": 2 + len(all_market_data),  # Quality reports + risk + recon + executive
        "assets_materialized": 5
    }


if __name__ == "__main__":
    # Execute the financial data pipeline
    result = financial_data_pipeline_flow(
        exchanges=["NYSE"],
        symbols_per_exchange=1
    )
    print("\n✅ Financial Data Pipeline execution completed successfully!")
    print(f"📊 Final Results: {result}")
