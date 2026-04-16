import re
import time
import textwrap
import logging
from pathlib import Path
from typing import List, Optional
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field

# Import global configuration
try:
    from src import config
except ImportError:
    # Fallback for direct execution if src package is not in path
    import sys
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src import config

# --- CONFIGURATION ---
MARKDOWN_DIR = config.MARKDOWN_DIR
logger = logging.getLogger(__name__)

# --- 1. THE SCHEMA ---
class DatasetMention(BaseModel):
    dataset_name: str = Field(
        ...,
        description='The formal name, acronym, or clear descriptive name of the dataset (e.g., "GLEAM", "US Census Data", "Survey of Income and Program Participation"). Avoid generic terms like "data" or "results" unless they refer to a specific, identifiable dataset.'
    )
    reference_directness: str = Field(
        default="none",
        description='Classify as "direct" (the paper uses the dataset directly for analysis) or "indirect" (the paper discusses the dataset or uses it for background/comparison). Output "none" if unclear.'
    )
    mention_in_abstract: str = Field(
        default="none",
        description='The exact verbatim sentence from the Abstract mentioning the dataset. If not mentioned in the abstract, output "none".'
    )
    mention_in_full_text: str = Field(
        default="none",
        description='The exact verbatim sentence where the dataset is first significantly introduced or described in the main text.'
    )
    mention_section: str = Field(
        default="none",
        description='The section heading where the dataset is primarily introduced (e.g., "2. Data", "Methodology", "Results").'
    )
    standardized_section: str = Field(
        default="none",
        description='Map the Mention Section to a standard category: "Introduction", "Methodology", "Results", "Discussion", "Data Availability", or "Other".'
    )
    reference_title: str = Field(
        default="none",
        description='The full title of the cited paper or report associated with the dataset, found in the References section. Cross-reference citations (e.g., "[Smith et al., 2020]") to the bibliography.'
    )
    persistent_identifier: str = Field(
        default="none",
        description='The DOI, URL, or other persistent identifier (e.g., handle, ARK) associated with the dataset or its citation.'
    )
    dataset_authors: str = Field(
        default="none",
        description='The author(s) or organization responsible for the dataset (e.g., "NASA", "Smith, J.", "World Bank").'
    )
    dataset_year: str = Field(
        default="none",
        description='The year of publication or release of the dataset. If not explicitly stated, infer from the citation year.'
    )
    dataset_url: str = Field(
        default="none",
        description='Direct URL to the dataset download or landing page, if mentioned in the text or references.'
    )
    placement_type: str = Field(
        default="none",
        description='Where the dataset is primarily cited or mentioned: "inline citation", "footnote", "data availability statement", or "bibliography only".'
    )
    placement_content: str = Field(
        default="none",
        description='The exact citation marker or text used to refer to the dataset in the body (e.g., "(Smith et al., 2020)", "[12]", "footnote 3").'
    )
    reference_material: str = Field(
        default="none",
        description='Type of reference material: "data paper" (a paper describing the dataset), "repository" (e.g., GitHub, Zenodo), "website" (project page), or "supplementary material".'
    )
    material_year: str = Field(
        default="none",
        description='The publication year of the reference material (may differ from dataset year).'
    )
    dataset_version: str = Field(
        default="none",
        description='The specific version of the dataset used (e.g., "v1.0", "Release 2021"). Output "none" if not specified.'
    )
    access_date: str = Field(
        default="none",
        description='The date the authors state they accessed or downloaded the dataset. Output "none" if not mentioned.'
    )

class ExtractionResult(BaseModel):
    extractions: List[DatasetMention] = Field(
        description="A list of all unique datasets identified in the text. Consolidate multiple mentions of the same dataset into a single entry."
    )

# --- 2. PROMPTS ---
SYSTEM_PROMPT = textwrap.dedent("""\
    You are an Expert Academic Data Librarian and Metadata Specialist.
    Your task is to analyze academic publications and extract structured metadata for all datasets used, created, or discussed.

    **CORE OBJECTIVES:**
    1. **Identify Datasets**: Locate all datasets mentioned. These may be:
       - **Explicit**: Named datasets (e.g., "World Values Survey", "ImageNet").
       - **Implicit**: Described datasets (e.g., "we collected survey responses from 500 participants", "tweets scraped from 2020-2021").
       - **Cited**: Datasets referenced via formal citations.
    
    2. **Distinguish Roles**:
       - **Primary Use**: Datasets used to generate the paper's results.
       - **Comparison/Context**: Datasets discussed for background or comparison.
       - **Creation**: New datasets generated by this research.

    3. **Extraction Rules**:
       - **Verbatim Accuracy**: When extracting text (sentences, titles), copy exactly as it appears. Do not paraphrase.
       - **Citation Resolution**: If a dataset is cited (e.g., "[12]" or "(Doe, 2021)"), you MUST look up the corresponding entry in the References section to fill in details like `reference_title`, `dataset_authors`, and `persistent_identifier`.
       - **Context Matters**: Use the "Methodology", "Data", and "Results" sections to understand how the dataset was used.
       - **No Hallucination**: If a field cannot be found or reasonably inferred, explicitly output "none".
       - **Consolidation**: If a dataset is mentioned multiple times, combine the information into a single comprehensive entry.

    4. **Implicit Datasets**:
       - For implicit datasets (e.g., "we conducted interviews"), name them descriptively (e.g., "Author-generated Interview Data").
""")

def get_user_prompt(text_chunk: str) -> str:
    return f"""
    Please analyze the following text segment from an academic publication.
    Extract all dataset mentions according to the defined schema.
    
    TEXT SEGMENT:
    ---
    {text_chunk}
    ---
    """

# --- 3. EXECUTION LOGIC ---
def get_client() -> instructor.Instructor:
    """Initialize the instructor client with configuration settings."""
    return instructor.from_openai(
        OpenAI(
            base_url=config.LLM_BASE_URL,
            api_key=config.LLM_API_KEY,
        ),
        mode=instructor.Mode.JSON
    )

def chunk_text(text: str, chunk_size: int = 8000, overlap: int = 500) -> List[str]:
    """
    Split text into chunks with overlap to maintain context.
    Simple character-based chunking for now, but could be improved to be paragraph-aware.
    """
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += (chunk_size - overlap)
    return chunks

def extract_datasets_from_text(text: str, client: instructor.Instructor, model_name: str = "qwen/qwen3-32b") -> List[DatasetMention]:
    """
    Main extraction function. 
    1. Chunks the text.
    2. Runs extraction on each chunk.
    3. (Optional) Could add a consolidation step here if chunks produce duplicate partial datasets.
    """
    chunks = chunk_text(text)
    all_datasets = []
    
    logger.info("Split text into %s chunks.", len(chunks))

    for i, chunk in enumerate(chunks):
        logger.info("Analyzing chunk %s/%s...", i + 1, len(chunks))
        try:
            resp = client.chat.completions.create(
                model=model_name,
                response_model=ExtractionResult,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": get_user_prompt(chunk)}
                ],
                temperature=0.0, # Deterministic output
            )
            
            # Filter out empty/none results
            valid_results = [
                d for d in resp.extractions 
                if d.dataset_name.lower() != "none" and len(d.dataset_name) > 2
            ]
            
            if valid_results:
                logger.info("Found %s candidates in chunk %s.", len(valid_results), i + 1)
                all_datasets.extend(valid_results)
            else:
                logger.info("No candidates found in chunk %s.", i + 1)
                
        except Exception as e:
            logger.exception("Error processing chunk %s/%s", i + 1, len(chunks))

    return all_datasets

def run_extraction_pipeline(file_path: Path):
    """
    Orchestrates the extraction for a single file.
    """
    if not file_path.exists():
        logger.error("File not found: %s", file_path)
        return

    logger.info("Processing: %s", file_path.name)
    text = file_path.read_text(encoding="utf-8")
    
    client = get_client()
    # Note: Using a model that supports JSON mode well. 
    # If using local Ollama, might need to adjust model name (e.g. "qwen2.5:7b").
    # If using Groq/OpenAI, use appropriate model name.
    # We'll default to a high-quality model if available, or fall back to config.
    
    # Heuristic: check if we are using local ollama or remote
    model = "qwen2.5:7b" if "localhost" in config.LLM_BASE_URL else "qwen/qwen3-32b"
    
    start_time = time.time()
    datasets = extract_datasets_from_text(text, client, model_name=model)
    duration = time.time() - start_time
    
    logger.info("Extraction complete: found %s potential datasets in %.2fs.", len(datasets), duration)
    
    # Display results
    for idx, d in enumerate(datasets):
        logger.info("--- DATASET %s: %s ---", idx + 1, d.dataset_name)
        # Print non-none fields for cleaner output
        for k, v in d.model_dump().items():
            if v and v.lower() != "none":
                logger.info("  %s: %s", k, v)

if __name__ == "__main__":
    # Simple CLI for testing
    import argparse
    parser = argparse.ArgumentParser(description="Extract dataset metadata from markdown files.")
    parser.add_argument("--file", type=str, help="Path to a specific markdown file to process.")
    parser.add_argument("--all", action="store_true", help="Process all markdown files in the directory.")
    
    args = parser.parse_args()
    
    if args.file:
        run_extraction_pipeline(Path(args.file))
    elif args.all:
        files = list(MARKDOWN_DIR.glob("*.md"))
        logger.info("Found %s markdown files.", len(files))
        for f in files:
            run_extraction_pipeline(f)
    else:
        # Default behavior: run on the first file found (similar to original script)
        files = list(MARKDOWN_DIR.glob("*.md"))
        if files:
            logger.info("No arguments provided. Running on the first available file as a test.")
            run_extraction_pipeline(files[0])
        else:
            logger.warning("No markdown files found in %s", MARKDOWN_DIR)
