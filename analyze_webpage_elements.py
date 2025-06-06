from prefect import flow, task
from bs4 import BeautifulSoup
import httpx
from typing import List
from dataclasses import dataclass
from pydantic import AnyHttpUrl
from prefect.artifacts import create_markdown_artifact

def format_number(num: int) -> str:
    """Format a number with commas for thousands."""
    return f"{num:,}"

def format_size(size_bytes: int) -> str:
    """Convert bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

@dataclass
class ElementAnalysis:
    tag: str
    size: int
    content: str

@task
async def fetch_webpage(url: AnyHttpUrl) -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get(str(url), follow_redirects=True)
        response.raise_for_status()
        return response.text

@task
def parse_html(html_content: str) -> BeautifulSoup:
    return BeautifulSoup(html_content, 'html.parser')

@task
def analyze_elements(soup: BeautifulSoup) -> List[ElementAnalysis]:
    elements = []
    
    for element in soup.find_all():
        content = element.get_text(strip=True)
        size = len(content)
        
        if size > 0:
            elements.append(ElementAnalysis(
                tag=element.name,
                size=size,
                content=content[:100] + '...' if len(content) > 100 else content
            ))
    
    return sorted(elements, key=lambda x: x.size, reverse=True)

@task
def create_analysis_artifact(elements: List[ElementAnalysis], url: AnyHttpUrl) -> None:
    """Create a markdown artifact with the analysis results."""
    markdown_content = f"# Element Analysis for {url}\n\n"
    markdown_content += "## 10 large elements\n\n"
    
    total_size = sum(e.size for e in elements)
    
    for i, element in enumerate(elements[:10], 1):
        percentage = round((element.size / total_size) * 100)
        markdown_content += f"### {i}. `{element.tag}`\n"
        markdown_content += f"**Size**: {format_number(element.size)} characters ({format_size(element.size)}) - {percentage}% of total\n"
        markdown_content += f"**Text excerpt**: {element.content}\n\n"
    
    markdown_content += "## Summary\n"
    markdown_content += f"**Elements analyzed**: {format_number(len(elements))}\n"
    markdown_content += f"**Total content size**: {format_number(total_size)} characters ({format_size(total_size)})\n"
    
    create_markdown_artifact(
        key="webpage-analysis",
        markdown=markdown_content,
        description=f"Analysis of largest elements in {url}"
    )

@flow
async def analyze_webpage(url: AnyHttpUrl = AnyHttpUrl("https://prefect.io")) -> List[ElementAnalysis]:
    html_content = await fetch_webpage(url)
    soup = parse_html(html_content)
    elements = analyze_elements(soup)
    
    create_analysis_artifact(elements, url)
    
    return elements

if __name__ == "__main__":
    import asyncio
    
    async def main():
        url = AnyHttpUrl("https://prefect.io")
        await analyze_webpage(url)
    
    asyncio.run(main())
