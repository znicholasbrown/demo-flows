from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact, create_image_artifact
import time
from random import uniform, choice

image_map = {
    "Science_Fiction": "https://cdn.midjourney.com/afe0c1d4-2645-4729-82d6-7a5b4dcf8600/0_2.png",
    "Fantasy": "https://cdn.midjourney.com/5b9b6b22-cc57-43a0-a2f4-e57ca4948a13/0_1.png",
    "Mystery": "https://cdn.midjourney.com/7f9fb32b-9742-44b8-9ae4-78864d12c703/0_0.png",
    "Romance": "https://cdn.midjourney.com/90f3ce8d-6285-4c44-8d24-6833efd3c0f3/0_2.png",
    "Horror": "https://cdn.midjourney.com/7cd1be65-34dc-40a0-a093-57f4f324891b/0_3.png",
}

@task
def generate_idea(subject: str, sleep_time: float = 0.5):
    logger = get_run_logger()
    logger.info(f"Generating idea for subject: {subject}")
    time.sleep(sleep_time)
    
    genres = ["Science_Fiction", "Fantasy", "Mystery", "Romance", "Horror"]
    settings = ["Ancient Civilization", "Dystopian Future", "Magical Kingdom", "Space Colony", "Underwater City"]
    conflicts = ["Man vs. Nature", "Good vs. Evil", "Self-Discovery", "Technological Dilemma", "Moral Quandary"]
    
    idea = f"{choice(genres)} story set in a {choice(settings)} dealing with {choice(conflicts)}"
    logger.info(f"Generated idea: {idea}")
    return idea

@task
def create_idea_image(idea: str, sleep_time: float = 1.0):
    logger = get_run_logger()
    logger.info(f"Creating image for idea: {idea}")
    time.sleep(sleep_time)
    
    # In a real scenario, this would generate or retrieve an actual image
    image_url = image_map[idea.split(" ")[0]]
    
    create_image_artifact(
        key=f"idea-image-{hash(idea)}",
        image_url=image_url,
        description=f"Generated image for idea: {idea}"
    )
    logger.info(f"Created image artifact for idea: {idea}")

@task
def create_idea_details(idea: str, sleep_time: float = 0.75):
    logger = get_run_logger()
    logger.info(f"Creating detailed setup for idea: {idea}")
    time.sleep(sleep_time)
    
    details = f"""
    ## Detailed Setup for: {idea}

    ### Character Archetypes:
    1. The Reluctant Hero
    2. The Wise Mentor
    3. The Cunning Antagonist

    ### Key Plot Points:
    1. Inciting Incident: {choice(["Natural Disaster", "Technological Breakthrough", "Political Upheaval"])}
    2. First Plot Turn: {choice(["Betrayal", "Unexpected Alliance", "Discovery of Hidden Truth"])}
    3. Midpoint: {choice(["Major Setback", "Revelation of True Stakes", "Personal Transformation"])}
    4. Second Plot Turn: {choice(["All Seems Lost", "Unexpected Aid", "Sacrifice"])}
    5. Climax: {choice(["Epic Confrontation", "Moral Decision", "Race Against Time"])}

    ### Themes to Explore:
    - {choice(["Identity and Self-Discovery", "Power and Corruption", "Love and Sacrifice"])}
    - {choice(["Technology and Humanity", "Nature vs. Nurture", "The Cost of Progress"])}

    ### Potential Subplots:
    1. {choice(["Forbidden Romance", "Political Intrigue", "Quest for Ancient Artifact"])}
    2. {choice(["Family Secrets", "Ecological Crisis", "Artificial Intelligence Uprising"])}

    Start brainstorming scenes and character backstories to flesh out this narrative framework!
    """
    
    create_markdown_artifact(
        key=f"idea-details-{hash(idea)}",
        markdown=details,
        description=f"Detailed setup for idea: {idea}"
    )
    logger.info(f"Created detailed setup artifact for idea: {idea}")

@flow
def NarrativeSparkGenerator(subject: str, num_ideas: int = 3, sleep_range: tuple[float, float] = (0.5, 2.0)):
    logger = get_run_logger()
    logger.info(f"Starting NarrativeSparkGenerator for subject: {subject}")
    
    ideas = []
    for i in range(num_ideas):
        sleep_time = uniform(*sleep_range)
        idea = generate_idea(subject, sleep_time)
        ideas.append(idea)
        
        create_idea_image(idea, sleep_time * 1.5)
        create_idea_details(idea, sleep_time * 1.2)
    
    ideas_markdown = "\n".join([f"{i+1}. {idea}" for i, idea in enumerate(ideas)])
    create_markdown_artifact(
        key=f"narrative-sparks-{hash(subject)}",
        markdown=f"## Narrative Sparks for '{subject}'\n\n{ideas_markdown}",
        description=f"Generated narrative sparks for subject: {subject}"
    )
    
    logger.info(f"NarrativeSparkGenerator completed for subject: {subject}")
    return ideas

if __name__ == "__main__":
    NarrativeSparkGenerator("Time Travel", num_ideas=5, sleep_range=(1.0, 3.0))