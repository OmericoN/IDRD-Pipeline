# IDRD-Pipeline Source Code

## Overview
This directory contains the source code for the IDRD-Pipeline, organized as a multi-stage pipeline for fetching publications with implicit dataset references and evaluating their formality.

## Package Structure

```
src/
├── __init__.py                 # Main package initialization
├── README.md                   # This file
│
├── fetching/                   # Stage 1: Publication Fetching
│   ├── __init__.py
│   └── README.md
│
├── extraction/                 # Stage 2: Feature Extraction
│   ├── __init__.py
│   └── README.md
│
├── embeddings/                 # Stage 3: Affiliation & Similarity
│   ├── __init__.py
│   └── README.md
│
└── evaluation/                 # Stage 4: Reference Formality
    ├── __init__.py
    └── README.md
```

## Pipeline Stages

### 1. Fetching (`src.fetching`)
The first stage fetches publications from the Semantic Scholar API based on search criteria. This stage handles API communication, rate limiting, and initial data collection.

**Key Responsibilities:**
- Connect to Semantic Scholar API
- Retrieve publication metadata
- Handle pagination and rate limits

### 2. Extraction (`src.extraction`)
The second stage extracts relevant features from the fetched publications for further analysis.

**Key Responsibilities:**
- Parse publication content
- Extract relevant features
- Normalize and prepare data

### 3. Embeddings (`src.embeddings`)
The third stage embeds the university database to check author affiliations and calculate similarity metrics.

**Key Responsibilities:**
- Create embeddings from university data
- Match author affiliations
- Calculate similarity scores

### 4. Evaluation (`src.evaluation`)
The fourth stage evaluates the formality of dataset references found in the publications.

**Key Responsibilities:**
- Assess reference formality
- Classify reference types
- Generate evaluation metrics

## Installation

To use this package:

```bash
pip install -e .
```

## Usage

```python
# Import the entire pipeline
import src

# Or import individual stages
from src import fetching, extraction, embeddings, evaluation

# Or import specific modules (when implemented)
# from src.fetching import api_client
# from src.extraction import feature_extractor
# from src.embeddings import affiliation_matcher
# from src.evaluation import formality_evaluator
```

## Development

Each package contains:
- `__init__.py` - Package initialization with documentation
- `README.md` - Detailed package documentation

When implementing, add your modules to the appropriate package directory.

## Notes

This is a package structure only. Implementation of specific functionality should be added to each package as needed.
