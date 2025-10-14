"""
Real-time Model Usage Dashboard Flow

Demonstrates the experimental artifacts SDK by simulating real-time token usage
tracking for different LLMs with a live-updating dashboard artifact.
"""
import time
import random
from datetime import datetime
from typing import Dict, List
import os

from prefect import flow, task
from prefect._experimental.artifacts.composable import Artifact
from prefect._experimental.artifacts.components import Markdown, Table, Progress, Link
import marvin
from pydantic_ai.models.anthropic import AnthropicModel


def get_anthropic_model():
    """Get Anthropic model if API key is available."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key:
        return AnthropicModel(model_name="claude-3-5-sonnet-latest")
    else:
        return None

anthropic_model = get_anthropic_model()

# Marvin agents for content generation
insight_agent = marvin.Agent(
    name="LLM Performance Analyst",
    instructions="You are an expert AI researcher and performance analyst. Generate insightful commentary about LLM usage patterns, performance trends, and cost optimization strategies. Keep insights concise but valuable.",
    model=anthropic_model if anthropic_model else None
)

news_agent = marvin.Agent(
    name="AI News Generator",
    instructions="You are an AI industry news writer. Generate realistic but fictional AI industry news updates, product announcements, and research breakthroughs. Keep items short and interesting.",
    model=anthropic_model if anthropic_model else None
)


def create_agent_status_component(current_second: int = 0, total_seconds: int = 60) -> Markdown:
    """Create a shared agent status component that can be used across artifacts."""
    # Determine agent status based on simulation progress
    progress_pct = (current_second / total_seconds) * 100 if total_seconds > 0 else 0

    if current_second == 0:
        status_emoji = "🔄"
        status_text = "Initializing"
        marvin_status = "🟡 Standby"
        news_status = "🟡 Standby"
        analysis_status = "🟡 Standby"
    elif current_second < 10:
        status_emoji = "🚀"
        status_text = "Starting Up"
        marvin_status = "🟢 Active" if anthropic_model else "🔴 Offline"
        news_status = "🟢 Active"
        analysis_status = "🟡 Queued"
    elif current_second < total_seconds - 10:
        status_emoji = "⚡"
        status_text = "Fully Operational"
        marvin_status = "🟢 Active" if anthropic_model else "🔴 Offline"
        news_status = "🟢 Generating"
        analysis_status = "🟡 Queued"
    elif current_second < total_seconds:
        status_emoji = "🏁"
        status_text = "Finishing"
        marvin_status = "🟢 Active" if anthropic_model else "🔴 Offline"
        news_status = "🟠 Finalizing"
        analysis_status = "🟡 Queued"
    else:
        status_emoji = "✅"
        status_text = "Complete"
        marvin_status = "🟡 Standby"
        news_status = "✅ Complete"
        analysis_status = "🟢 Generating"

    agent_status_markdown = f"""## {status_emoji} Agent Status - {status_text}

| Agent | Status | Function |
|-------|--------|----------|
| 🤖 Marvin AI | {marvin_status} | Content generation & analysis |
| 📰 News Generator | {news_status} | Industry news updates |
| 🧠 Analysis Engine | {analysis_status} | Performance insights |

*Progress: {progress_pct:.1f}% • Elapsed: {current_second}s*
    """

    return Markdown(markdown=agent_status_markdown)


@task
def initialize_models() -> Dict[str, Dict]:
    """Initialize model configurations and initial usage data."""
    print("🔧 Setting up model configurations...")

    models = {
        "gpt-4": {
            "name": "GPT-4",
            "provider": "OpenAI",
            "cost_per_token": 0.00003,
            "tokens_used": 0,
            "requests": 0,
            "avg_response_time": 0.0,
            "status": "Active"
        },
        "claude-3-opus": {
            "name": "Claude 3 Opus",
            "provider": "Anthropic",
            "cost_per_token": 0.000015,
            "tokens_used": 0,
            "requests": 0,
            "avg_response_time": 0.0,
            "status": "Active"
        },
        "gemini-pro": {
            "name": "Gemini Pro",
            "provider": "Google",
            "cost_per_token": 0.0000005,
            "tokens_used": 0,
            "requests": 0,
            "avg_response_time": 0.0,
            "status": "Active"
        },
        "llama-2-70b": {
            "name": "Llama 2 70B",
            "provider": "Meta",
            "cost_per_token": 0.000001,
            "tokens_used": 0,
            "requests": 0,
            "avg_response_time": 0.0,
            "status": "Active"
        }
    }

    for model_key, model_info in models.items():
        print(f"  ✅ {model_info['name']} ({model_info['provider']}) - ${model_info['cost_per_token']:.6f}/token")

    print(f"🎯 Model initialization complete - {len(models)} models ready for monitoring")
    return models


@task
def generate_static_analysis_report(model_data: Dict[str, Dict]) -> str:
    """Generate a one-time static analysis report using AI insights."""
    print("🧠 Generating AI-powered analysis report...")

    # Create artifact for static analysis
    analysis_artifact = Artifact(
        key="llm-analysis-report",
        description="AI-Generated LLM Performance Analysis and Insights"
    )

    # Generate insights using Marvin if available
    if anthropic_model:
        try:
            with marvin.Thread(id="llm-analysis") as thread:
                insights = insight_agent.run(
                    f"Analyze these LLM configurations and generate insights about cost efficiency, "
                    f"performance characteristics, and usage recommendations: {model_data}"
                )
            print("✅ AI-generated insights created")
        except Exception as e:
            print(f"⚠️  Marvin unavailable, using fallback insights: {e}")
            insights = "AI analysis unavailable - using fallback static insights about LLM performance patterns."
    else:
        insights = """
## Performance Analysis

### Cost Efficiency Rankings:
1. **Gemini Pro**: Extremely cost-effective at $0.0000005/token
2. **Llama 2 70B**: Open-source alternative at $0.000001/token
3. **Claude 3 Opus**: Premium option at $0.000015/token
4. **GPT-4**: High-cost leader at $0.00003/token

### Use Case Recommendations:
- **High-volume processing**: Gemini Pro or Llama 2 70B
- **Complex reasoning**: Claude 3 Opus or GPT-4
- **Cost-sensitive workflows**: Gemini Pro for most tasks
- **Enterprise applications**: Claude 3 Opus for balanced performance/cost
        """

    # Create the static analysis content
    analysis_content = Markdown(markdown=f"""# 🔍 LLM Performance Analysis Report

*Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

## 📊 Executive Summary
This comprehensive analysis evaluates the performance characteristics, cost efficiency, and optimal use cases for the monitored Large Language Models.

## 🤖 AI-Generated Insights
{insights}

## 📈 Technical Specifications

| Model | Provider | Cost/Token | Typical Use Cases |
|-------|----------|------------|------------------|
| GPT-4 | OpenAI | $0.00003 | Complex reasoning, coding, research |
| Claude 3 Opus | Anthropic | $0.000015 | Analysis, writing, balanced performance |
| Gemini Pro | Google | $0.0000005 | High-volume tasks, cost optimization |
| Llama 2 70B | Meta | $0.000001 | Open-source alternative, custom deployments |

## 🎯 Strategic Recommendations

### For Cost Optimization:
- Implement model routing based on task complexity
- Use Gemini Pro for simple tasks, GPT-4 for complex reasoning
- Monitor token usage patterns to identify optimization opportunities

### For Performance Maximization:
- Claude 3 Opus offers best balance of capability and cost
- GPT-4 for highest-quality outputs when cost is secondary
- Consider hybrid approaches using multiple models

---
*This analysis remains static throughout the monitoring session*
    """)

    analysis_artifact.append(analysis_content)
    print("📋 Static analysis report artifact created")
    return "analysis-complete"


@task
def generate_dynamic_news_feed(duration_seconds: int) -> str:
    """Generate a frequently-updating AI news feed artifact."""
    print("📰 Starting AI news feed generation...")

    # Create artifact for dynamic news feed
    news_artifact = Artifact(
        key="ai-industry-news",
        description="Real-time AI Industry News and Updates"
    )

    # Initialize with header
    news_header = Markdown(markdown=f"""# 📰 AI Industry News Feed

*Live updates during LLM monitoring session*

---
    """)
    news_artifact.append(news_header)

    # Agent status component (shared with dashboard)
    news_agent_status = create_agent_status_component(0, duration_seconds)
    news_artifact.append(news_agent_status)

    # Initialize news items list
    news_items = []

    # Update news feed every 5 seconds
    update_interval = 5
    updates_count = duration_seconds // update_interval

    for update_num in range(updates_count):
        elapsed = (update_num + 1) * update_interval
        print(f"📰 Generating news update {update_num + 1}/{updates_count} at {elapsed}s...")

        # Generate news item using Marvin if available
        if anthropic_model:
            try:
                with marvin.Thread(id=f"news-update-{update_num}") as thread:
                    news_item = news_agent.run(
                        f"Generate a short, realistic but fictional AI industry news item. "
                        f"Topics could include: new model releases, research breakthroughs, "
                        f"company announcements, performance benchmarks, or industry trends. "
                        f"Keep it to 1-2 sentences and make it timestamped."
                    )
                print(f"✅ AI-generated news item #{update_num + 1}")
            except Exception as e:
                print(f"⚠️  Marvin unavailable for news #{update_num + 1}, using fallback")
                fallback_news = [
                    "OpenAI announces 15% performance improvement in GPT-4 Turbo through architectural optimizations",
                    "Anthropic releases Claude 3.5 Sonnet with enhanced coding capabilities and 25% faster inference",
                    "Google introduces Gemini Ultra 2.0 with breakthrough multimodal reasoning at reduced costs",
                    "Meta open-sources Llama 3 with improved efficiency and new fine-tuning frameworks",
                    "Microsoft Azure AI reports 40% cost reduction for enterprise LLM deployments",
                    "New research shows transformer alternatives achieving 3x speed improvements",
                    "Industry consortium announces standardized LLM benchmarking protocols"
                ]
                news_item = f"**{datetime.now().strftime('%H:%M:%S')}** - {random.choice(fallback_news)}"
        else:
            # Fallback news items
            fallback_news = [
                "OpenAI announces 15% performance improvement in GPT-4 Turbo",
                "Anthropic releases Claude 3.5 Sonnet with enhanced coding capabilities",
                "Google introduces Gemini Ultra 2.0 with breakthrough multimodal reasoning",
                "Meta open-sources Llama 3 with improved efficiency frameworks",
                "Microsoft Azure AI reports 40% cost reduction for enterprise deployments",
                "New research shows transformer alternatives achieving 3x speed improvements",
                "Industry consortium announces standardized LLM benchmarking protocols"
            ]
            news_item = f"**{datetime.now().strftime('%H:%M:%S')}** - {random.choice(fallback_news)}"

        # Add timestamp and format
        timestamped_item = f"- **{datetime.now().strftime('%H:%M:%S')}** | {news_item}"
        news_items.append(timestamped_item)

        # Update agent status for news feed
        elapsed_seconds = (update_num + 1) * update_interval
        updated_news_agent_status = create_agent_status_component(elapsed_seconds, duration_seconds)
        news_agent_status.update(updated_news_agent_status.markdown)

        # Update the artifact with all news items
        news_content = f"""# 📰 AI Industry News Feed

*Live updates during LLM monitoring session - Update #{update_num + 1}*

## 🔥 Latest Headlines

{chr(10).join(news_items)}

---
*News feed updates every {update_interval} seconds during monitoring*
        """

        # Update the header with new content
        news_header.update(news_content)

        # Wait for next update (except on last iteration)
        if update_num < updates_count - 1:
            time.sleep(update_interval)

    print(f"📰 News feed complete - generated {len(news_items)} updates")
    return f"news-feed-{len(news_items)}-updates"


@task
def simulate_model_usage(model_data: Dict[str, Dict], duration_seconds: int = 30) -> Dict[str, Dict]:
    """Simulate realistic model usage over time."""
    print(f"🚀 Starting {duration_seconds}-second usage simulation...")
    print("📊 Creating real-time dashboard artifact...")

    # Create artifact dashboard
    dashboard = Artifact(
        key="llm-usage-dashboard",
        description="Real-time LLM Token Usage and Performance Dashboard"
    )

    # Initialize dashboard components
    header = Markdown(markdown=f"# 🤖 LLM Usage Dashboard\n*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
    dashboard.append(header)

    status_section = Markdown(markdown="## 📊 System Status\n**Status:** Initializing monitoring...")
    dashboard.append(status_section)

    # Add documentation link
    docs_link = Link(
        link="https://docs.prefect.io/latest/concepts/artifacts/",
        link_text="📖 Prefect Artifacts Documentation"
    )
    dashboard.append(docs_link)

    # Agent status component (shared with news feed)
    agent_status = create_agent_status_component(0, duration_seconds)
    dashboard.append(agent_status)

    # Progress tracking
    # progress_bar = Progress(progress="0.0")
    # dashboard.append(progress_bar)

    # Usage table
    usage_table = Table(table={
        "Model": [],
        "Provider": [],
        "Tokens Used": [],
        "Requests": [],
        "Avg Response (ms)": [],
        "Est. Cost ($)": [],
        "Status": []
    })
    dashboard.append(usage_table)

    print("⏳ Initializing dashboard components...")
    time.sleep(2)  # Initial setup delay
    print("✅ Dashboard ready - starting real-time monitoring loop...")

    # Track activity for detailed logging
    last_report_second = 0
    total_activity_count = 0

    # Simulation loop
    for i in range(duration_seconds):
        # Update progress
        progress = (i + 1) / duration_seconds * 100
        # progress_bar.update(str(progress))

        # Update timestamp
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        header.update(f"# 🤖 LLM Usage Dashboard\n*Last updated: {current_time}*")

        # Update agent status every few seconds
        if i % 3 == 0:
            updated_agent_status = create_agent_status_component(i + 1, duration_seconds)
            agent_status.update(updated_agent_status.markdown)

        # Track activity this second
        second_activity = 0

        # Simulate usage for each model
        for model_key, model_info in model_data.items():
            if random.random() < 0.7:  # 70% chance of activity per second
                # Simulate a request
                tokens = random.randint(100, 2000)
                response_time = random.uniform(200, 1500)  # ms

                model_info["tokens_used"] += tokens
                model_info["requests"] += 1
                second_activity += 1
                total_activity_count += 1

                # Update average response time
                old_avg = model_info["avg_response_time"]
                new_avg = (old_avg * (model_info["requests"] - 1) + response_time) / model_info["requests"]
                model_info["avg_response_time"] = new_avg

        # Log activity every 5 seconds
        if i > 0 and i % 5 == 0:
            elapsed = i + 1
            remaining = duration_seconds - elapsed
            total_requests = sum(model["requests"] for model in model_data.values())
            total_tokens = sum(model["tokens_used"] for model in model_data.values())
            print(f"⚡ {elapsed}s elapsed | {second_activity} requests this second | {total_requests} total requests | {total_tokens:,} tokens | {remaining}s remaining")

        # Update status message
        total_requests = sum(model["requests"] for model in model_data.values())
        total_tokens = sum(model["tokens_used"] for model in model_data.values())

        if i < 5:
            status = "🟡 **Status:** Warming up systems..."
        elif i < duration_seconds - 5:
            status = f"🟢 **Status:** Active monitoring - {total_requests} requests, {total_tokens:,} tokens processed"
        else:
            status = "🟠 **Status:** Winding down monitoring..."

        status_section.update(f"## 📊 System Status\n{status}")

        # Update usage table every second for more frequent updates
        if i % 1 == 0 or i >= duration_seconds - 3:
            table_data: dict[str, list] = {
                "Model": [],
                "Provider": [],
                "Tokens Used": [],
                "Requests": [],
                "Avg Response (ms)": [],
                "Est. Cost ($)": [],
                "Status": []
            }

            for model_info in model_data.values():
                estimated_cost = model_info["tokens_used"] * model_info["cost_per_token"]

                table_data["Model"].append(model_info["name"])
                table_data["Provider"].append(model_info["provider"])
                table_data["Tokens Used"].append(f"{model_info['tokens_used']:,}")
                table_data["Requests"].append(str(model_info["requests"]))
                table_data["Avg Response (ms)"].append(f"{model_info['avg_response_time']:.0f}")
                table_data["Est. Cost ($)"].append(f"${estimated_cost:.4f}")
                table_data["Status"].append(model_info["status"])

            usage_table.update(table_data)

        # Add some interesting events
        if i == 10:
            event_md = Markdown(markdown="## 🚨 Events\n- **10s**: High load detected on GPT-4")
            dashboard.append(event_md)

        elif i == 15:
            event_md.update("## 🚨 Events\n- **10s**: High load detected on GPT-4\n- **15s**: Gemini Pro showing excellent response times")

        elif i == 20:
            event_md.update("## 🚨 Events\n- **10s**: High load detected on GPT-4\n- **15s**: Gemini Pro showing excellent response times\n- **20s**: Claude 3 Opus processing complex queries")

        elif i == duration_seconds - 3:
            # Remove events section to demonstrate removal
            dashboard.remove(event_md)

        time.sleep(1)  # Real-time delay

    # Final status update
    final_total_requests = sum(model['requests'] for model in model_data.values())
    final_total_tokens = sum(model['tokens_used'] for model in model_data.values())
    final_total_cost = sum(model['tokens_used'] * model['cost_per_token'] for model in model_data.values())

    print(f"🏁 Simulation complete! Final stats: {final_total_requests} requests, {final_total_tokens:,} tokens, ${final_total_cost:.4f} estimated cost")
    status_section.update("## 📊 System Status\n🔴 **Status:** Monitoring session completed")
    # progress_bar.update("100.0")

    # Add final summary
    final_summary = Markdown(markdown=f"""
## 📈 Session Summary
- **Total Duration:** {duration_seconds} seconds
- **Total Requests:** {final_total_requests}
- **Total Tokens:** {final_total_tokens:,}
- **Total Estimated Cost:** ${final_total_cost:.4f}
- **Average Requests/Second:** {final_total_requests/duration_seconds:.2f}
    """)
    dashboard.append(final_summary)

    print("📋 Dashboard artifact updated with final summary")
    return model_data


@task
def generate_final_report(model_data: Dict[str, Dict]) -> str:
    """Generate a final usage report."""
    print("📊 Generating comprehensive final report...")

    total_cost = sum(model["tokens_used"] * model["cost_per_token"] for model in model_data.values())
    total_requests = sum(model["requests"] for model in model_data.values())
    total_tokens = sum(model["tokens_used"] for model in model_data.values())

    # Find the most used model
    most_used = max(model_data.items(), key=lambda x: x[1]["tokens_used"])

    # Find fastest and most expensive models
    fastest_model = min(model_data.items(), key=lambda x: x[1]["avg_response_time"] if x[1]["avg_response_time"] > 0 else float('inf'))
    most_expensive = max(model_data.items(), key=lambda x: x[1]["tokens_used"] * x[1]["cost_per_token"])

    print(f"📈 Analysis complete - Most active: {most_used[1]['name']}, Fastest: {fastest_model[1]['name']}")

    report = f"""
Model Usage Monitoring Session Complete!

Summary:
- Most Active Model: {most_used[1]['name']} ({most_used[1]['tokens_used']:,} tokens, {most_used[1]['requests']} requests)
- Fastest Model: {fastest_model[1]['name']} ({fastest_model[1]['avg_response_time']:.0f}ms avg)
- Highest Cost Model: {most_expensive[1]['name']} (${most_expensive[1]['tokens_used'] * most_expensive[1]['cost_per_token']:.4f})
- Total API Calls: {total_requests}
- Total Tokens: {total_tokens:,}
- Total Estimated Cost: ${total_cost:.4f}
- Average Cost per Request: ${total_cost/total_requests:.6f}

Per-Model Breakdown:"""

    for model_key, model_info in model_data.items():
        model_cost = model_info["tokens_used"] * model_info["cost_per_token"]
        report += f"""
  {model_info['name']}:
    - Requests: {model_info['requests']}
    - Tokens: {model_info['tokens_used']:,}
    - Avg Response Time: {model_info['avg_response_time']:.0f}ms
    - Cost: ${model_cost:.4f}"""

    print("✅ Final report generated with detailed per-model breakdown")
    return report.strip()


@flow(name="Model Usage Dashboard")
def model_usage(duration_minutes: int = 1):
    """
    Demo flow showcasing real-time artifact updates with simulated LLM usage data.

    This flow creates multiple artifacts with different update patterns:
    - Real-time dashboard: Updates every second during simulation
    - Dynamic news feed: Updates every 5 seconds with AI-generated content
    - Static analysis report: Generated once at the end with complete data

    Args:
        duration_minutes: How long to run the simulation (default: 1 minute)
    """
    duration_seconds = duration_minutes * 60
    print(f"🚀 Starting Model Usage Monitoring Dashboard for {duration_minutes} minute(s)...")
    print("📋 This flow will generate 3 different artifacts with varying update patterns")

    # Initialize model configurations
    models = initialize_models()
    print(f"📊 Initialized {len(models)} models for monitoring")

    # Start parallel execution of dynamic tasks
    print(f"⚡ Starting parallel execution of dynamic tasks for {duration_seconds} seconds...")

    # Submit both dynamic tasks to run in parallel
    from prefect import Task

    # Run simulation with real-time updates (main dashboard)
    simulation_future = simulate_model_usage.submit(models, duration_seconds=duration_seconds)

    # Run news feed updates (updates every 5 seconds)
    news_future = generate_dynamic_news_feed.submit(duration_seconds)

    print("🔄 Both dynamic tasks running in parallel...")

    # Wait for both tasks to complete
    updated_models = simulation_future.result()
    news_result = news_future.result()

    print(f"✅ Simulation complete")
    print(f"✅ News feed complete: {news_result}")

    # Generate final report
    report = generate_final_report(updated_models)
    print("\n" + "="*60)
    print(report)
    print("="*60)

    # Generate static analysis report at the end (runs once, after all data is collected)
    print("🧠 Generating final static analysis report with complete data...")
    analysis_result = generate_static_analysis_report(updated_models)
    print(f"✅ Static analysis complete: {analysis_result}")

    print("\n🎯 Artifact Summary:")
    print("  📊 llm-usage-dashboard: Real-time updates (every second)")
    print(f"  📰 ai-industry-news: Dynamic updates (every 5 seconds, {news_result})")
    print("  🔍 llm-analysis-report: Static content (generated at end with final data)")

    return {
        "models": updated_models,
        "analysis": analysis_result,
        "news": news_result
    }


if __name__ == "__main__":
    model_usage()