import textwrap
import langextract as lx
# Directly import the OpenAI provider to bypass the config bug
from langextract.providers.openai import OpenAILanguageModel

# 1. Initialize the model pointing to your running IPEX-LLM server
local_model = OpenAILanguageModel(
    model_id="qwen2.5:7b",  
    api_key="ollama",       # Dummy key, the local server ignores this
    base_url="http://localhost:11434/v1", 
    default_query={"options": {"temperature": 0.6}}
)

# 2. Define what you want to extract
prompt = textwrap.dedent("""\
    /no_think
    Extract the medications, dosages, and frequencies from the text.
    Use exact text for extractions. Do not paraphrase.
""")

# 3. Provide a high-quality "few-shot" example
examples = [
    lx.data.ExampleData(
        text="Take 250mg of Ibuprofen every 4 hours.",
        extractions=[
            lx.data.Extraction(
                extraction_class="medication",
                extraction_text="Ibuprofen",
                attributes={
                    "dosage": "250mg", 
                    "frequency": "every 4 hours"
                }
            )
        ]
    )
]

# The unstructured text
input_text = "The patient was prescribed 500mg Amoxicillin twice daily for the infection."

# 4. Run the extraction LOCALLY on your Arc iGPU
try:
    print("Sending text to local IPEX-LLM Qwen 2.5...")
    result = lx.extract(
        text_or_documents=input_text,
        prompt_description=prompt,
        examples=examples,
        model=local_model  
    )

    print("\n--- Results ---")
    for ext in result.extractions:
        print(f"Type: {ext.extraction_class.upper()}")
        print(f"Text: {ext.extraction_text}")
        print(f"Attributes: {ext.attributes}\n")

except Exception as e:
    print(f"Extraction failed: {e}")