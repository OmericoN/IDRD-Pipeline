"""
Global configuration for Citation Context Analysis project
"""
from pathlib import Path

from dotenv import load_dotenv

# Load .env file from the parent directory (project root)
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
import os

# Semantic Scholar API
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "")


#LLM Configuration
