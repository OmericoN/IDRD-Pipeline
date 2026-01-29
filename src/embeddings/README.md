# Embeddings Package

## Overview
The **Embeddings** package is the third stage of the IDRD-Pipeline. It handles embedding the university database to check affiliation and calculate similarity.

## Purpose
This package is responsible for:
- Creating embeddings from university database
- Checking author affiliations against known institutions
- Calculating similarity metrics between publications and institutions
- Matching publications to university affiliations

## Structure
```
embeddings/
├── __init__.py          # Package initialization and exports
└── README.md            # This file
```

## Future Modules
When implementing this package, consider adding:
- `embedding_model.py` - Embedding model implementation
- `affiliation_matcher.py` - Match affiliations to universities
- `similarity.py` - Similarity calculation utilities
- `database_loader.py` - Load university database
- `data_models.py` - Data models for embeddings
- `utils.py` - Helper utilities

## Usage
```python
from src.embeddings import ...
# Add your implementation here
```
