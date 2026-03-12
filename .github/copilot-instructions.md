# Copilot Instructions — IDRD Pipeline

## Project Overview

The IDRD Pipeline is a multi-stage research paper processing system that:
1. Fetches academic publications from the **Semantic Scholar API**
2. Downloads open-access PDFs
3. Converts PDFs to TEI XML via **GROBID** (runs in Docker)
4. Extracts structured Markdown from the TEI XML
5. (Phase 3, in progress) Extracts dataset mentions using LLM + Pydantic + Instructor

The pipeline stores all metadata and processing state in **PostgreSQL**. Each pipeline stage updates status flags in the DB so runs can be resumed.

## Setup

```bash
pip install -e .
```

Requires a `.env` file at the repo root:
```
SEMANTIC_SCHOLAR_API_KEY=...
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=idrd_pipeline
POSTGRES_USER=postgres
POSTGRES_PASSWORD=...
LLM_API_KEY=...
```

GROBID must be running as a Docker container before running conversion.

## Running the Pipeline

```bash
# Full pipeline
python src/main.py --query "implicit dataset references" --limit 50

# Resume from last incomplete stage
python src/main.py --resume

# Individual stages
python src/main.py --fetch-only --query "..." --limit 100
python src/main.py --download-only [--dl-limit 20 --dl-delay 1.0]
python src/main.py --convert-only [--delete-pdfs --cv-overwrite]
python src/main.py --extract-only [--ex-limit 20 --ex-overwrite]

# Utilities
python src/main.py --status
python src/main.py --reset {status|full}
```

**Ground truth experiment** (isolated from main pipeline):
```bash
python experiments/ground_truth/gt_runner.py [--fetch-only|--no-xml|--no-extract|--extract-only]
```

## Tests

Tests use `unittest` (not pytest), located in `src/ingestion/tests.py`:

```bash
python -m unittest src/ingestion/tests
# Run a single test class:
python -m unittest src.ingestion.tests.TestPDFDownloader
# Run a single test method:
python -m unittest src.ingestion.tests.TestPDFDownloader.test_filename_generation
```

## Architecture

### Pipeline Stages and Data Flow

```
Semantic Scholar API
        │
  pubfetcher/client.py (SemanticScholarClient)
        │  saves paper metadata
        ▼
  db/db.py (PublicationDatabase) ◄── PostgreSQL (state + metadata)
        │
  ingestion/downloader.py (PDFDownloader)
        │  saves to data/pdf/
        ▼
  ingestion/converter.py (GrobidConverter) ◄── GROBID Docker container
        │  saves to data/xml/
        ▼
  ingestion/renderer.py (extract_markdown)
        │  saves to data/markdown/
        ▼
  extraction/extractor.py (LLM via Instructor + Pydantic)
```

`src/main.py` owns the `IDRDPipeline` class which instantiates all components with a **shared `db` instance** — this is the pattern throughout: always pass `db=self.db` to avoid duplicate connections.

`src/config.py` is the single source of truth for all paths (`DATA_DIR`, `PDF_DIR`, `XML_DIR`, etc.) and environment variables. Do not hardcode paths; import from config.

### Ground Truth Experiment

`experiments/ground_truth/` is fully isolated — it uses separate data directories (`data/gt_experiment/`) and does not write to the main DB. It reads from `data/ground_truth/ground_truth.csv`.

## Key Conventions

**Return shapes**: Functions return dicts with a consistent shape — use `{'success': bool, 'message': str}` for single operations and `{'successful': int, 'failed': int, 'skipped': int}` for batch operations.

**File paths**: Always use `pathlib.Path` (already imported everywhere). Directories are created at config init with `path.mkdir(parents=True, exist_ok=True)`.

**Filenames**: Paper IDs are sanitized with `re.sub(r'[<>:\"/\\|?*]', '', paper_id)` before use as filenames. Follow this pattern when creating new file-based outputs.

**PDF validation**: Validate PDFs by checking magic bytes (`f.read(4) == b'%PDF'`) before storing, not by file extension.

**Status tracking**: The DB has per-paper boolean flags for each stage (e.g., `pdf_downloaded`, `xml_converted`). Use `get_pipeline_status()` for aggregate status; update flags after each operation rather than at batch end.

**Error persistence**: Store error messages in DB fields (e.g., `pdf_download_error`) alongside print statements. Do not silently swallow errors.

**CLI flags**: Follow the existing namespace prefixes — `--dl-*` for download, `--cv-*` for convert, `--ex-*` for extract — when adding new CLI arguments.

**Per-run logs**: Save run metadata as JSON to `logs/runs/<timestamp>/metadata/`. See existing metadata files for the schema.

## Phase Roadmap Context

- **Phase 1** ✅ Complete: fetch, download, convert, extract pipeline; GT experiment
- **Phase 2** 🔧 Planned: Replace `print` with `logging`, add `alembic` migrations, connection pooling, full `pytest` suite, Docker Compose
- **Phase 3** 🤖 In progress: LLM-based feature extraction (methodology, datasets, metrics) via `extraction/extractor.py`
- **Phase 4** 🔍 Planned: RAG + pgvector embeddings

When working on Phase 2+ features, prefer additive changes that don't break the existing CLI interface.
