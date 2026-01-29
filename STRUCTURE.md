# IDRD-Pipeline Package Structure

## Visual Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      IDRD-Pipeline                          │
│                   (Python Package v0.1.0)                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         src/                                │
│                  (Main Source Package)                      │
├─────────────────────────────────────────────────────────────┤
│  • __init__.py      - Package initialization                │
│  • README.md        - Documentation                         │
└─────────────────────────────────────────────────────────────┘
        │
        ├─────────────────┬─────────────────┬─────────────────┐
        ▼                 ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Stage 1    │  │   Stage 2    │  │   Stage 3    │  │   Stage 4    │
│  FETCHING    │  │  EXTRACTION  │  │  EMBEDDINGS  │  │  EVALUATION  │
├──────────────┤  ├──────────────┤  ├──────────────┤  ├──────────────┤
│              │  │              │  │              │  │              │
│ Semantic     │  │ Feature      │  │ University   │  │ Reference    │
│ Scholar API  │──▶ Extraction   │──▶ Database     │──▶ Formality    │
│ Integration  │  │              │  │ Embedding &  │  │ Assessment   │
│              │  │              │  │ Affiliation  │  │              │
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘
      │                 │                 │                 │
      ▼                 ▼                 ▼                 ▼
  __init__.py       __init__.py       __init__.py       __init__.py
  README.md         README.md         README.md         README.md
```

## Directory Structure

```
IDRD-Pipeline/
├── .gitignore                 # Git ignore patterns
├── LICENSE                    # License file
├── README.md                  # Main project documentation
├── STRUCTURE.md               # This file - structure overview
├── pyproject.toml             # Python project configuration
│
└── src/                       # Main source package
    ├── __init__.py            # Package initialization (v0.1.0)
    ├── README.md              # Source code documentation
    │
    ├── fetching/              # Stage 1: Publication Fetching
    │   ├── __init__.py        # Fetching package initialization
    │   └── README.md          # Fetching documentation
    │
    ├── extraction/            # Stage 2: Feature Extraction  
    │   ├── __init__.py        # Extraction package initialization
    │   └── README.md          # Extraction documentation
    │
    ├── embeddings/            # Stage 3: Affiliation & Similarity
    │   ├── __init__.py        # Embeddings package initialization
    │   └── README.md          # Embeddings documentation
    │
    └── evaluation/            # Stage 4: Reference Formality
        ├── __init__.py        # Evaluation package initialization
        └── README.md          # Evaluation documentation
```

## Package Details

### Main Package (`src`)
- **Location**: `src/__init__.py`
- **Version**: 0.1.0
- **Exports**: All four pipeline stages
- **Documentation**: Comprehensive docstrings and README

### Stage 1: Fetching (`src.fetching`)
- **Purpose**: Fetch publications from Semantic Scholar API
- **Input**: Search criteria, query parameters
- **Output**: Publication metadata
- **Key Features**: API client, rate limiting, pagination

### Stage 2: Extraction (`src.extraction`)
- **Purpose**: Extract features from publications
- **Input**: Publication metadata from Stage 1
- **Output**: Extracted and normalized features
- **Key Features**: Parsing, feature extraction, normalization

### Stage 3: Embeddings (`src.embeddings`)
- **Purpose**: Embed university DB and match affiliations
- **Input**: Features from Stage 2, university database
- **Output**: Affiliation matches, similarity scores
- **Key Features**: Embedding models, similarity metrics, matching

### Stage 4: Evaluation (`src.evaluation`)
- **Purpose**: Evaluate formality of references
- **Input**: Processed data from Stage 3
- **Output**: Formality scores, evaluation reports
- **Key Features**: Formality assessment, classification, scoring

## File Types

### Python Files (`*.py`)
- **`__init__.py`**: Package initialization files with:
  - Module-level docstrings
  - Version information
  - Export definitions (`__all__`)
  - Package metadata

### Documentation Files (`*.md`)
- **README.md**: Comprehensive documentation for each package
  - Overview and purpose
  - Responsibilities
  - Structure diagram
  - Future modules
  - Usage examples

### Configuration Files
- **`pyproject.toml`**: Python project configuration
  - Build system requirements
  - Project metadata
  - Dependencies (to be added)
  - Development dependencies
  - Tool configurations (black, pytest, mypy)

## Import Examples

```python
# Import entire pipeline
import src

# Import all stages
from src import fetching, extraction, embeddings, evaluation

# Access version info
print(src.__version__)  # "0.1.0"

# Future imports (when modules are implemented)
# from src.fetching import api_client
# from src.extraction import feature_extractor
# from src.embeddings import affiliation_matcher
# from src.evaluation import formality_evaluator
```

## Design Principles

1. **Modularity**: Each stage is a self-contained package
2. **Clarity**: Clear naming and comprehensive documentation
3. **Extensibility**: Easy to add new modules to each package
4. **Standards**: Follows Python packaging best practices
5. **Documentation**: Every package has docstrings and README files

## Next Steps for Implementation

When ready to implement functionality:

1. Add module files to each package (e.g., `api_client.py`, `feature_extractor.py`)
2. Update `__init__.py` files to export new modules
3. Add dependencies to `pyproject.toml`
4. Create tests directory with unit tests
5. Add example scripts or CLI entry points
6. Update documentation with API references

## Package Status

- ✅ Package structure created
- ✅ All `__init__.py` files with documentation
- ✅ README files for all packages
- ✅ Project configuration (`pyproject.toml`)
- ✅ Main documentation updated
- ⏳ Implementation (ready for development)
- ⏳ Tests (to be added)
- ⏳ Dependencies (to be specified)
