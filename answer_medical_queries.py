from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact, create_table_artifact, create_progress_artifact, update_progress_artifact
import time
from random import uniform

@task
def LoadMedicalKnowledgeBase(sleep_time: float = 0.5):
    logger = get_run_logger()
    logger.info("Loading medical knowledge base...")
    time.sleep(sleep_time)
    logger.debug("Loaded: Medical textbooks")
    logger.debug("Loaded: Recent research papers")
    logger.debug("Loaded: Drug interactions database")
    logger.info("Medical knowledge base loaded successfully")

@task
def ParseQuery(sleep_time: float = 0.2):
    logger = get_run_logger()
    logger.info("Parsing medical query...")
    time.sleep(sleep_time)
    logger.debug("Identified: Key medical terms")
    logger.debug("Identified: Query intent")
    logger.debug("Identified: Required specialties")
    logger.info("Query parsed successfully")

@task
def RetrieveRelevantInformation(sleep_time: float = 0.3, failure_rate: float = 0.1):
    logger = get_run_logger()
    logger.info("Retrieving relevant medical information...")
    time.sleep(sleep_time)
    logger.debug("Retrieved: Relevant research papers")
    logger.debug("Retrieved: Clinical guidelines")
    logger.debug("Retrieved: Case studies")
    logger.info("Relevant information retrieved successfully")

    if uniform(0, 1) < failure_rate:
        raise ValueError("Failed to retrieve relevant information")

@task
def GenerateAnswer(sleep_time: float = 0.4):
    logger = get_run_logger()
    progress_artifact_id = create_progress_artifact(key="answer-generation", progress=0)
    
    logger.info("Generating answer to medical query...")
    for i in range(1, 6):
        time.sleep(sleep_time / 5)
        logger.debug(f"Generating answer part {i}/5")
        update_progress_artifact(progress_artifact_id, progress=i*20)
    
    logger.info("Answer generated successfully")
    update_progress_artifact(progress_artifact_id, progress=100)

@flow
def SynthesizeEvidence(failure_rate: float = 0.1):
    logger = get_run_logger()
    logger.info("Synthesizing medical evidence...")
    
    retrieve = RetrieveRelevantInformation(failure_rate=failure_rate)
    time.sleep(0.2)
    
    evidence_summary = """
    ## Evidence Summary

    1. Recent meta-analysis shows treatment A is more effective than B for condition X.
    2. Clinical guidelines recommend starting with lowest effective dose.
    3. Potential drug interaction identified between treatment A and medication Y.
    4. Case studies suggest monitoring of liver function is crucial.
    """
    create_markdown_artifact(key="evidence-summary", markdown=evidence_summary)
    
    logger.info("Evidence synthesis complete")

@flow
def AnswerMedicalQueries(sleep_time: float = 0.3, failure_rate: float = 0.1):
    logger = get_run_logger()
    logger.info("Starting medical query answering process...")
    
    load_kb = LoadMedicalKnowledgeBase(sleep_time)
    parse = ParseQuery(sleep_time, wait_for=[load_kb])
    evidence = SynthesizeEvidence(wait_for=[parse], failure_rate=failure_rate)
    answer = GenerateAnswer(sleep_time, wait_for=[evidence])
    
    response_quality = [
        {"Metric": "Accuracy", "Score": "95%"},
        {"Metric": "Completeness", "Score": "92%"},
        {"Metric": "Relevance", "Score": "98%"},
        {"Metric": "Clarity", "Score": "90%"}
    ]
    create_table_artifact(key="response-quality-metrics", table=response_quality)
    
    logger.info("Medical query answered successfully")
    return answer

if __name__ == "__main__":
    AnswerMedicalQueries(failure_rate=1, sleep_time=10)