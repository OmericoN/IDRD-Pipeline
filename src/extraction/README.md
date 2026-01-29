# Extraction Package

## Overview
The **Extraction** package is the second stage of the IDRD-Pipeline. It handles extracting features from the fetched publications.

## Purpose
This package is responsible for:
- Parsing publication content and metadata
- Extracting relevant features for analysis
- Processing and normalizing extracted data
- Preparing features for the embedding stage

## Structure
```
extraction/
├── __init__.py          # Package initialization and exports
└── README.md            # This file
```

## Future Modules
When implementing this package, consider adding:
- `feature_extractor.py` - Main feature extraction logic
- `parsers.py` - Parse different publication formats
- `text_processor.py` - Text processing utilities
- `data_models.py` - Data models for extracted features
- `utils.py` - Helper utilities

## Usage
```python
from src.extraction import ...
# Add your implementation here
```
