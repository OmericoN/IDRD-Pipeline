# IDRD-Pipeline

A 4-Stage pipeline for efficiently fetching publications containing implicit dataset references and evaluating their formality.

## Overview

The IDRD-Pipeline is a Python-based multi-stage pipeline designed to:
1. Fetch publications from Semantic Scholar API
2. Extract relevant features from publications
3. Embed university database for affiliation and similarity checking
4. Evaluate the formality of dataset references

## Main Orchestration Control
```python
# Full pipeline
python src/main.py --query "Transformers" --limit 50

# Fetch only
python src/main.py --query "Machine Learning" --limit 100 --fetch-only

# Download only
python src/main.py --download-only

# Convert only
python src/main.py --convert-only

# Check status
python src/main.py --status

# Reset pipeline
python src/main.py --reset status
```



## Pipeline Stages
To be written
<!-- ### Stage 1: Fetching (`src.fetching`)
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
- Dataset affiliation with UM via vector embedding on UM database
- Author affiliation matching
- Similarity metric calculation
- Institution-publication matching

### Stage 4: Evaluation (`src.evaluation`)
Evaluates the formality of dataset references.
- Reference formality assessment
- Reference type classification
- Quality and completeness scoring
- Evaluation report generation -->

### Tests - downloader & converter
```python

    # Run all tests
    python src/extractor/tests.py

    # Run specific test class
    python -m unittest src.extractor.tests.TestPDFDownloader

    # Run specific test
    python -m unittest src.extractor.tests.TestPDFDownloader.test_download_paper_success

    # Run with coverage (requires coverage package)
    pip install coverage
    coverage run src/extractor/tests.py
    coverage report
    coverage html
```

## Installation

```bash
# Clone the repository
git clone https://github.com/OmericoN/IDRD-Pipeline.git
cd IDRD-Pipeline

# Install in development mode
pip install -e .
```


## License

See [LICENSE](LICENSE) file for details.
