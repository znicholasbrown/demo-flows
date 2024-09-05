from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from time import sleep
from random import uniform, choice

@task
def PreprocessFeedback(sleep_time: float = 0.5):
    logger = get_run_logger()
    logger.info("Preprocessing customer feedback...")
    sleep(sleep_time)
    logger.debug("Removed special characters")
    logger.debug("Converted to lowercase")
    logger.debug("Tokenized text")
    logger.info("Preprocessing complete")

@task
def PerformSentimentAnalysis(sleep_time: float = 0.5):
    logger = get_run_logger()
    logger.info("Performing sentiment analysis...")
    sleep(sleep_time)
    sentiment = choice(['Positive', 'Neutral', 'Negative'])
    logger.info(f"Sentiment detected: {sentiment}")
    return sentiment

@task
def DetectEmotion(sleep_time: float = 0.5):
    logger = get_run_logger()
    logger.info("Detecting emotion in feedback...")
    sleep(sleep_time)
    emotion = choice(['Happy', 'Sad', 'Angry', 'Surprised', 'Frustrated'])
    logger.info(f"Emotion detected: {emotion}")
    return emotion

@task
def GenerateInsights(sentiment: str, emotion: str, sleep_time: float = 0.5):
    logger = get_run_logger()
    logger.info("Generating insights from analysis...")
    sleep(sleep_time)
    
    insights = f"""
    ## Customer Feedback Insights
    
    - **Sentiment**: {sentiment}
    - **Emotion**: {emotion}
    
    ### Key Takeaways:
    1. Customer's overall mood is {sentiment.lower()} with hints of {emotion.lower()} emotion.
    2. This feedback requires {'immediate' if sentiment == 'Negative' else 'standard'} attention.
    3. Consider {'reaching out personally' if emotion in ['Angry', 'Frustrated'] else 'sending a follow-up survey'}.
    """
    
    create_markdown_artifact(
        key=f"feedback-insights-{sentiment.lower()}-{emotion.lower()}",
        markdown=insights,
        description=f"Generated insights from customer feedback analysis: {sentiment} / {emotion}"
    )
    
    logger.info("Insights generated and stored as an artifact")
    return sentiment, emotion

@flow
def ProcessFeedbackNode(node_number: int, sleep_range: tuple[float, float]):
    logger = get_run_logger()
    logger.info(f"Processing node {node_number}")
    
    sleep_time = uniform(*sleep_range)
    
    preprocess = PreprocessFeedback(sleep_time)
    sentiment = PerformSentimentAnalysis(sleep_time)
    emotion = DetectEmotion(sleep_time)
    result = GenerateInsights(sentiment, emotion, sleep_time)
    
    return result

@flow
def CustomerFeedbackMoodTracker(num_nodes: int = 4, node_sleep_range: tuple[float, float] = (0.5, 2.0)):
    logger = get_run_logger()
    logger.info(f"Starting CustomerFeedbackMoodTracker with {num_nodes} nodes")
    
    results = []
    
    for i in range(num_nodes):
        result = ProcessFeedbackNode(i+1, node_sleep_range)
        results.append(result)
        sleep_time = uniform(*[sleep_time * 2 for sleep_time in  node_sleep_range])
        sleep(sleep_time)
    
    # Create a summary table artifact
    summary_data = [{"Node": i+1, "Sentiment": s, "Emotion": e} for i, (s, e) in enumerate(results)]
    create_table_artifact(
        key="mood-tracking-summary",
        table=summary_data,
        description="Summary of sentiment and emotion analysis across all nodes"
    )
    
    logger.info("CustomerFeedbackMoodTracker completed successfully")

if __name__ == "__main__":
    CustomerFeedbackMoodTracker(num_nodes=20, node_sleep_range=(1, 5))