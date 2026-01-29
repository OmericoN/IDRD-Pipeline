# IDRD-Pipeline

A 4-Stage pipeline for efficiently fetching publications containing implicit dataset references and evaluating their formality.

## Overview

The IDRD-Pipeline is a Python-based multi-stage pipeline designed to:
1. Fetch publications from Semantic Scholar API
2. Extract relevant features from publications
3. Embed university database for affiliation and similarity checking
4. Evaluate the formality of dataset references

## Project Structure

```
IDRD-Pipeline/
├── src/                        # Source code
│   ├── __init__.py
│   ├── README.md
│   │
│   ├── fetching/              # Stage 1: Fetch publications via Semantic Scholar API
│   │   ├── __init__.py
│   │   └── README.md
│   │
│   ├── extraction/            # Stage 2: Extract features from publications
│   │   ├── __init__.py
│   │   └── README.md
│   │
│   ├── embeddings/            # Stage 3: Embed university DB for affiliation/similarity
│   │   ├── __init__.py
│   │   └── README.md
│   │
│   └── evaluation/            # Stage 4: Evaluate formality of references
│       ├── __init__.py
│       └── README.md
│
├── .gitignore
├── LICENSE
├── README.md                   # This file
└── pyproject.toml             # Python project configuration
```

## Pipeline Stages

### Stage 1: Fetching (`src.fetching`)
Fetches publications from the Semantic Scholar API based on search criteria.
- API communication and authentication
- Query building and execution
- Rate limiting and pagination handling
- Initial data collection and storage

### Stage 2: Extraction (`src.extraction`)
Extracts relevant features from the fetched publications.
- Publication content parsing
- Feature extraction and selection
- Data normalization and preprocessing
- Preparation for embedding stage

### Stage 3: Embeddings (`src.embeddings`)
Embeds university database to check affiliation and calculate similarity.
- University database embedding
- Author affiliation matching
- Similarity metric calculation
- Institution-publication matching

### Stage 4: Evaluation (`src.evaluation`)
Evaluates the formality of dataset references.
- Reference formality assessment
- Reference type classification
- Quality and completeness scoring
- Evaluation report generation

## Installation

```bash
# Clone the repository
git clone https://github.com/OmericoN/IDRD-Pipeline.git
cd IDRD-Pipeline

# Install in development mode
pip install -e .
```

## Usage

```python
# Import the pipeline
import src

# Import individual stages
from src import fetching, extraction, embeddings, evaluation

# Future usage (when modules are implemented):
# from src.fetching import api_client
# from src.extraction import feature_extractor
# from src.embeddings import affiliation_matcher
# from src.evaluation import formality_evaluator
```

## Development

Each package is self-contained with its own documentation. See individual package READMEs for details:
- [src/README.md](src/README.md) - Main source documentation
- [src/fetching/README.md](src/fetching/README.md) - Fetching stage
- [src/extraction/README.md](src/extraction/README.md) - Extraction stage
- [src/embeddings/README.md](src/embeddings/README.md) - Embeddings stage
- [src/evaluation/README.md](src/evaluation/README.md) - Evaluation stage

## License

See [LICENSE](LICENSE) file for details.
