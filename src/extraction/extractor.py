import re
import time
import textwrap
from pathlib import Path
from typing import List
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field

# --- CONFIGURATION ---
MARKDOWN_DIR = Path(__file__).parent.parent.parent / "data" / "markdown" 

client = instructor.from_openai(
    OpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama",
    ),
    mode=instructor.Mode.JSON
)

# --- 1. THE "PROMPT-ENGINEERED" SCHEMA ---
# The Field descriptions are the most important part of the prompt in Instructor!
class DatasetMention(BaseModel):
    dataset_name: str = Field(
        default="none", 
        description='The formal name or acronym of the dataset (e.g., "GLEAM", "Global Land Data Assimilation System (GLDAS)", "JRA55"). Do not extract pure algorithms.'
    )
    reference_directness: str = Field(
        default="none", 
        description='Classify as "direct" (leads to raw data/repo) or "indirect" (leads to a descriptive paper). Output "none" if not found.'
    )
    mention_in_abstract: str = Field(
        default="none", 
        description='The exact verbatim sentence from the "## Abstract" section mentioning the dataset. If it is only mentioned in the main body, output "none".'
    )
    mention_in_full_text: str = Field(
        default="none", 
        description='The exact verbatim sentence where the dataset is formally introduced in the main body (e.g., "The Global Land Evaporation Amsterdam Model (GLEAM) has two...").'
    )
    mention_section: str = Field(
        default="none", 
        description='The exact sub-heading where the dataset is introduced (e.g., "2) GLEAM" or "a. Selected ET datasets").'
    )
    standardized_section: str = Field(
        default="none", 
        description='Map the Mention Section to a standard category: "Methodology", "Results", "Introduction", or "Data Sources".'
    )
    reference_title: str = Field(
        default="none", 
        description='The full title of the cited paper found in the "## References" section. You MUST cross-reference the inline citation (e.g., "[Martens, 2017]") to the bibliography to find this!'
    )
    persistent_identifier: str = Field(
        default="none", 
        description='The DOI or URL extracted from the matched entry in the "## References" section (e.g., "10.5194/gmd-10-1903-2017").'
    )
    dataset_authors: str = Field(
        default="none", 
        description='The author(s) of the dataset or the descriptive paper (e.g., "Martens", "Dee", "Kobayashi").'
    )
    dataset_year: str = Field(
        default="none", 
        description='The year associated with the dataset or its citation (e.g., "2017", "2011"). Be aware: Year of reference material != Year of dataset'
    )
    dataset_url: str = Field(
        default="none", 
        description='Any explicit web URL pointing to the dataset. If only a DOI exists, output "none".'
    )
    placement_type: str = Field(
        default="none", 
        description='Where the citation lives: "inline text" or "bibliography".'
    )
    placement_content: str = Field(
        default="none", 
        description='The exact citation marker found in the text (e.g., "[(Martens et al. 2017;=Martens, 2017]" or "[Dee, 2011]").'
    )
    reference_material: str = Field(
        default="none", 
        description='Type of reference: "data paper", "repository", or "website".'
    )
    material_year: str = Field(
        default="none", 
        description='The publication year of the reference material.'
    )
    dataset_version: str = Field(
        default="none", 
        description='Version numbers mentioned in the text (e.g., "v3.3a", "version 1.0", "version 2.0"). Output "none" if missing.'
    )
    access_date: str = Field(
        default="none", 
        description='The date the authors accessed the dataset. Usually "none" unless explicitly stated.'
    )

class ExtractionResult(BaseModel):
    extractions: List[DatasetMention] = Field(description="A list of the primary datasets evaluated or used in the study.")

# --- 2. THE SYSTEM PROMPT ---
SYSTEM_PROMPT = textwrap.dedent("""\
    You are an expert academic data librarian. Your task is to extract highly structured metadata about datasets used in the provided academic text.

    CRITICAL RULES FOR THIS TEXT:
    1. PRIMARY vs SECONDARY: The text introduces primary datasets under specific numbered subheadings (e.g., "1) CSIRO", "2) GLEAM", "3) GLDAS"). Focus heavily on these primary datasets.
    2. CITATION RESOLUTION: The text uses messy XML artifacts for citations, such as `[(Gelaro et al. 2017)=Gelaro, 2017]`. 
       - You MUST capture this messy string exactly as it appears for the 'placement_content' field.
       - You MUST use the name/year in that string to search the `## References` section at the bottom of the text to find the full 'reference_title', 'persistent_identifier' (DOI), and 'dataset_authors'.
    3. STRICT VERBATIM: Do not paraphrase sentences. Copy them exactly as they appear in the markdown.
    4. NO HALLUCINATION: If a field is missing, output "none".
""")

# --- 3. EXECUTION LOGIC ---
def chunk_text(text: str, chunk_size: int = 6000) -> List[str]:
    # We increased chunk size to 6000 so the references section stays attached to the text!
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

def is_null_extraction(d: DatasetMention) -> bool:
    return d.dataset_name.strip().lower() == "none"

def run_test():
    md_files = list(Path(MARKDOWN_DIR).glob("*.md"))
    if not md_files:
        print(f"❌ No markdown files found in {MARKDOWN_DIR}")
        return

    test_file = md_files[0]
    print(f"📄 Testing on: {test_file.name}")

    # Notice: We removed the `strip_references_section` function!
    # The model NEEDS the references section to resolve the DOIs and Titles.
    document = test_file.read_text(encoding="utf-8")
    chunks = chunk_text(document, chunk_size=8000) 
    
    all_extracted_datasets = []

    try:
        start_time = time.time()
        
        for i, chunk in enumerate(chunks):
            print(f"🚀 Processing chunk {i+1}/{len(chunks)}...")
            
            result = client.chat.completions.create(
                model="qwen2.5:7b", # or 7b depending on your local hardware limits
                response_model=ExtractionResult,
                temperature=0.0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract the datasets from the following text:\n\n{chunk}"}
                ]
            )
            
            valid = [d for d in result.extractions if not is_null_extraction(d)]
            if valid:
                all_extracted_datasets.extend(valid)
                print(f"   ✅ Found {len(valid)} dataset(s).")
            else:
                print("   ℹ️ No datasets found.")

        elapsed_time = time.time() - start_time
        print(f"\n🎉 Extraction Complete! Took {elapsed_time:.1f} seconds.")
        
        for idx, dataset in enumerate(all_extracted_datasets):
            print(f"\n--- DATASET {idx + 1}: {dataset.dataset_name} ---")
            for key, value in dataset.model_dump().items():
                print(f"{key.replace('_', ' ').title()}: {value}")

    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")

if __name__ == "__main__":
    run_test()