import time
import textwrap
import re
from pathlib import Path
import langextract as lx
from langextract.providers.openai import OpenAILanguageModel
from config import LLM_API_KEY, MARKDOWN_DIR

# --- CONFIGURATION ---
# Resolve markdown dir relative to project root
MARKDOWN_DIR = Path(__file__).parent.parent.parent / "data" / "markdown" 

# Run local qwen model via ollama
local_model = OpenAILanguageModel(
    model_id="qwen2.5:7b",  
    api_key="ollama",       # Dummy key, the local server ignores this
    base_url="http://localhost:11434/v1", 
    default_query={
        "temperature": 0.0, 
        "response_format": {"type": "json_object"}
    }
)

# The actual prompt instruction
prompt = textwrap.dedent("""\
    /no_think
    # ROLE AND OBJECTIVE
    You are an expert academic data librarian and advanced natural language processing specialist, highly trained in bibliometrics, data curation, and scientific text mining. 
    Your primary objective is to meticulously extract dataset references, implicit data mentions, and their associated metadata from academic publications. You must parse complex scientific texts to identify exactly where and how researchers cite or describe the data used in their research.

    # EXTRACTION RULES & CONSTRAINTS
    1. STRICT VERBATIM EXTRACTION: You must use the exact, verbatim text from the source document for all text-based extractions. Do not paraphrase, summarize, alter punctuation, or truncate segments.
    2. NO HALLUCINATION: You must rely solely on the provided text. If a specific data point (e.g., a DOI, version number, or access date) is not explicitly present or unambiguously inferable from the text, you must output "none". Do not guess or fabricate URLs or identifiers.
    3. NO OVERLAPPING ENTITIES: Ensure that extracted text segments are distinct and do not improperly overlap across different schema fields.
    4. CONTEXTUAL ACCURACY: Maintain the scientific context. Differentiate between a paper *mentioning* a dataset in passing versus *formally citing* it as a primary source for their methodology.
    5. STRICT JSON ONLY: You must output ONLY valid JSON. Do not include any introductory text, conversational filler (e.g., "Here are the results"), or concluding remarks. Your entire response must be parseable by a standard JSON decoder.

    # SCHEMA DEFINITIONS
    Provide meaningful attributes for each dataset entity based strictly on the following definitions:

    * Dataset Name: The formal or colloquial name/acronym of the dataset cited in the paper (e.g., "ARCOS", "Medicaid.gov").
    * Reference Directness: Classify as "direct" (the reference leads directly to the raw data/repository) or "indirect" (the reference leads to a secondary paper that describes the data).
    * Mention in Abstract Text: The exact, verbatim sentence(s) within the abstract that imply or state the usage of this dataset.
    * Mention in Text Full-text: The exact, verbatim sentence(s) in the main body of the paper formally describing, introducing, or citing the dataset.
    * Mention Section: The specific, exact sub-heading where the dataset is first formally introduced (e.g., "Data sources", "2.1 Data Collection").
    * Standardized Section: Map the 'Mention Section' to its standard scientific parent category (e.g., "Methodology", "Results", "Introduction").
    * Reference Title: The full title of the cited reference, data paper, or web page corresponding to the dataset.
    * Persistent Identifier: A permanent link enabling unambiguous discovery (e.g., a DOI or Handle). Output "none" if not present.
    * Dataset Authors: The creator(s), organization(s), or agency responsible for the data (e.g., "Drug Enforcement Administration"). Defaults to data-paper authors if applicable.
    * Dataset Year: The specific publication or release year of the dataset itself.
    * Dataset URL: The exact URL pointing to the dataset repository, landing page, or source.
    * Placement Type: The structural location of the citation within the document (e.g., "inline text", "footnote", "bibliography").
    * Placement Content: The verbatim text of the citation placeholder (e.g., "[8]") or the full bibliographic entry string.
    * Reference Material: Categorize the type of source material (e.g., "website", "repository", "data paper", "supplementary file").
    * Material Year: The publication year of the specific reference material or bibliography entry.
    * Dataset Version: The specific version, release number, or wave identifier for the dataset.
    * Access Date: The precise date the authors accessed the dataset (commonly found in "accessed on" or "cited" brackets for dynamic websites).
""")

# few shot examples (following the framework scope)
examples = [
    # EXAMPLE 1: ARCOS DATASET
    lx.data.ExampleData(
        text="""Abstract: Methods: The distribution of hydromorphone in the US (in grams) was provided by US Drug Enforcement Administration's Automated Reports and Consolidated Orders System (ARCOS) by state, zip code, and by business types (pharmacies, hospitals, providers, etc.). ... 2. Materials and Methods: Data sources: The US Drug Enforcement Administration's Automated Reports and Consolidated Orders System (ARCOS) provided the distribution of hydromorphone (grams) nationally, by state, zip code, and by business types (pharmacies, hospitals, providers, etc.) [8]. ... References: 8. Drug Enforcement Administration. ARCOS Retail Drug Summary Reports [Internet]. Washington (DC): U.S. Departmentof Justice; [cited 2025 Jul 3]. Available from: https://www.deadiversion.usdoj.gov/arcos/retail_drug_summary/arcos-drug-summary-reports.html""",
        extractions=[
            lx.data.Extraction(
                extraction_class="dataset",
                extraction_text="ARCOS",
                attributes={
                    "Dataset Name": "Automated Reports and Consolidated Orders System (ARCOS)",
                    "Reference Directness": "direct",
                    "Mention in Abstract Text": "Methods: The distribution of hydromorphone in the US 15 (in grams) was provided by US Drug Enforcement Administration's Automated Reports 16 and Consolidated Orders System (ARCOS) by state, zip code, and by business types 17 (pharmacies, hospitals, providers, etc.).",
                    "Mention in Text Full-text": "The US Drug Enforcement Administration's Automated Reports and 69 Consolidated Orders System (ARCOS) provided the distribution of hydromorphone 70 (grams) nationally, by state, zip code, and by business types (pharmacies, hospitals, pro- 71 viders, etc.) [8].",
                    "Mention Section": "Data sources",
                    "Standardized Section": "Methodology",
                    "Reference Title": "ARCOS Retail Drug Summary Reports",
                    "Persistent Identifier": "none",
                    "Dataset Authors": "Drug Enforcement Administration; U.S. Department of Justice",
                    "Dataset Year": "none",
                    "Dataset URL": "https://www.deadiversion.usdoj.gov/arcos/retail_drug_summary/arcos-drug-summary-reports.html",
                    "Placement Type": "bibliography",
                    "Placement Content": "8. Drug Enforcement Administration. ARCOS Retail Drug Summary Reports [Internet]. Washington (DC): U.S. Departmentof Justice; [cited 2025 Jul 3]. Available from: https://www.deadiversion.usdoj.gov/arcos/retail_drug_summary/arcos-drug-summary-reports.html",
                    "Reference Material": "website",
                    "Material Year": "none",
                    "Dataset Version": "none",
                    "Access Date": "2025 Jul 3"
                }
            )
        ]
    ),

    lx.data.ExampleData(
        text="""Abstract: Hydromorphone prescriptions claims were also examined using the Medicaid and Medicare Part D programs from 2010 to 2023. Results: Hydromorphone increased by +30.6% by 2013, followed by a decrease of -55.9% by 2023 in ARCOS. ... 2. Materials and Methods: Data sources: Medicaid.gov provided the number of prescriptions (brand and generic) per state in the US from 2013-2023 [9]. Data.Medicaid.gov provided the number of Medicaid enrollees by state for December of 2015 and 2023 [15]. ... References: 9. Medicaid.gov. State Drug Utilization Data [Internet]. Baltimore (MD): Centers for Medicare & Medicaid Services; [cited 2025 Jul 3]. Available from: https://www.medicaid.gov/medicaid/prescription-drugs/state-drug-utilization-data""",
        extractions=[
            lx.data.Extraction(
                extraction_class="dataset",
                extraction_text="Medicaid.gov",
                attributes={
                    "Dataset Name": "Medicaid.gov",
                    "Reference Directness": "direct",
                    "Mentioggn in Abstract Text": "Hydromorphone prescriptions claims were also examined using the Medicaid and Medicare Part D programs from 2010 to 2023.",
                    "Mention in Text Full-text": "Medicaid.gov provided the number of prescriptions (brand and generic) per state in 74 the US from 2013-2023 [9].",
                    "Mention Section": "Data sources",
                    "Standardized Section": "Methodology",
                    "Reference Title": "State Drug Utilization Data",
                    "Persistent Identifier": "none",
                    "Dataset Authors": "Centers for Medicare & Medicaid Services",
                    "Dataset Year": "none",
                    "Dataset URL": "https://www.medicaid.gov/medicaid/prescription-drugs/state-drug-utilization-data",
                    "Placement Type": "bibliography",
                    "Placement Content": "9. Medicaid.gov. State Drug Utilization Data [Internet]. Baltimore (MD): Centers for Medicare & Medicaid Services; [cited 2025 Jul 3]. Available from: https://www.medicaid.gov/medicaid/prescription-drugs/state-drug-utilization-data",
                    "Reference Material": "website",
                    "Material Year": "none",
                    "Dataset Version": "none",
                    "Access Date": "2025 Jul 3"
                }
            )
        ]
    ),

    lx.data.ExampleData(
        text="""2. Data and method a. Selected ET datasets and data processing ... Table 1. Global ET datasets used in the study ... b CSIRO: https://data.csiro.au/dap/landingpage?pid=csiro%3A17375 ... 1) CSIRO The Commonwealth Scientific and Industrial Research Organisation (CSIRO) dataset is based on the Penman-Monteith-Leuning model, using the WATCH-Forcing-DATA-ERA-Interim and the Princeton Global Forcing as meteorological forcing data (Zhang et al. 2016a,b). ... References: Zhang, Y., ..., 2016a: Monthly global observation-driven Penman-Monteith-Leuning (PML) evapotranspiration and components, v2. CSIRO, accessed 5 June 2020, https://doi.org/10.4225/08/5719A5C48DB85.""",
        extractions=[
            lx.data.Extraction(
                extraction_class="dataset",
                extraction_text="CSIRO",
                attributes={
                    "Dataset Name": "Commonwealth Scientific and Industrial Research Organisation (CSIRO)",
                    "Reference Directness": "direct",
                    "Mention in Abstract Text": "none",
                    "Mention in Text Full-text": "The Commonwealth Scientific and Industrial Research Organisation (CSIRO) dataset is based on the Penman-Monteith-Leuning model, using the WATCH-Forcing-DATA-ERA-Interim and the Princeton Global Forcing as meteorological forcing data (Zhang et al. 2016a,b).",
                    "Mention Section": "2. Data and method",
                    "Standardized Section": "Methodology",
                    "Reference Title": "Monthly global observation-driven Penman-Monteith-Leuning (PML) evapotranspiration and components",
                    "Persistent Identifier": "https://doi.org/10.4225/08/5719A5C48DB85",
                    "Dataset Authors": "Zhang, Y., J. Pena Arancibia, T. McVicar, F. Chiew, J. Vaze, H. Zheng, and Y. P. Wang",
                    "Dataset Year": "2016",
                    "Dataset URL": "https://data.csiro.au/dap/landingpage?pid=csiro%3A17375",
                    "Placement Type": "footnote and bibliography",
                    "Placement Content": "b CSIRO: https://data.csiro.au/dap/landingpage?pid=csiro%3A17375 | 2016a: Monthly global observation-driven Penman-Monteith-Leuning (PML) evapotranspiration and components, v2. CSIRO, accessed 5 June 2020, https://doi.org/10.4225/08/5719A5C48DB85.",
                    "Reference Material": "repository",
                    "Material Year": "2016",
                    "Dataset Version": "v2",
                    "Access Date": "5 June 2020"
                }
            )
        ]
    )
]

# execution logic
def run_test():
    md_files = list(Path(MARKDOWN_DIR).glob("*.md"))
    if not md_files:
        print(f"No markdown files found in {MARKDOWN_DIR}")
        return

    test_file = md_files[0]
    print(f"Testing on: {test_file.name} - An Assessment of Concurrency in Evapotranspiration Trends across Multiple Global Datasets")

    # Read the file
    document = test_file.read_text(encoding="utf-8")
    original_len = len(document)

    try:
        print("Sending text to Qwen... (Check the Ollama terminal for action!)")
        start_time = time.time() # Start the stopwatch!

        # Run the extraction 
        result = lx.extract(
            text_or_documents=document,
            prompt_description=prompt,
            examples=examples,
            model=local_model,
            max_char_buffer= 5000
        )
        
        elapsed_time = time.time() - start_time 

        print(f"\n✅ Extraction Complete! Took {elapsed_time:.1f} seconds.")
        print("\n--- Parsed Results ---")
        for ext in result.extractions:
            print(f"Type: {ext.extraction_class.upper()}")
            print(f"Text: {ext.extraction_text}")
            print(f"Attributes: {ext.attributes}\n")

    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")

if __name__ == "__main__":
    run_test()