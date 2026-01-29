"""
IDRD-Pipeline: A multi-stage pipeline for fetching and evaluating publications with implicit dataset references.

This package contains four main stages:
1. Fetching: Retrieve publications using Semantic Scholar API
2. Extraction: Extract features from publications
3. Embeddings: Embed university database for affiliation and similarity checking
4. Evaluation: Evaluate formality of references

Version: 0.1.0
"""

__version__ = "0.1.0"
__author__ = "IDRD-Pipeline Team"

# Import subpackages for easier access
from . import fetching
from . import extraction
from . import embeddings
from . import evaluation

__all__ = [
    "fetching",
    "extraction",
    "embeddings",
    "evaluation",
]
