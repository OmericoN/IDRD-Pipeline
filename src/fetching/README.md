# Fetching Package

## Overview
The **Fetching** package is the first stage of the IDRD-Pipeline. It handles retrieving publications from the Semantic Scholar API.

## Purpose
This package is responsible for:
- Connecting to the Semantic Scholar API
- Searching and fetching publications based on criteria
- Managing API rate limits and pagination
- Storing publication metadata for downstream processing

## Structure
```
fetching/
├── __init__.py          # Package initialization and exports
└── README.md            # This file
```

## Future Modules
When implementing this package, consider adding:
- `api_client.py` - Semantic Scholar API client
- `query_builder.py` - Build search queries
- `data_models.py` - Data models for publications
- `fetcher.py` - Main fetching logic
- `utils.py` - Helper utilities

## Usage
```python
from src.fetching import ...
# Add your implementation here
```
