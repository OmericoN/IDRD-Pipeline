# Copilot Instructions for IDRD-Pipeline

## Build, test, and lint commands

- Install/sync dependencies: `uv sync`
- Run full test suite: `uv run pytest -q`
- Run a single test file: `uv run pytest -q tests/test_db.py`
- Run a single test case: `uv run pytest -q tests/test_db.py::test_get_pipeline_status`

There is no dedicated lint/format command configured in this repo (`ruff`, `flake8`, `black`, etc. are not configured).

## High-level architecture

- The project is a resumable, stage-based pipeline orchestrated by `src/main.py` (`IDRDPipeline`):
  1. Fetch paper metadata from Semantic Scholar
  2. Download PDFs
  3. Convert PDFs to TEI XML with GROBID (Docker-managed)
  4. Render TEI XML to Markdown
  5. LLM feature extraction (placeholder/not wired into pipeline run)
- Pipeline state is persisted in PostgreSQL (`src/db/db.py`) using stage flags in `publications` (`pdf_downloaded`, `xml_converted`, `sections_extracted`, `features_extracted`).
- Resume behavior is data-driven: `resume_pipeline()` checks `get_papers_needing_download()`, `get_papers_needing_conversion()`, and `get_papers_needing_rendering()` and runs only the next incomplete stage.
- Runtime telemetry for full pipeline runs is written to per-run folders: `logs/runs/<timestamp>/metadata/` (including `runtime_events.jsonl`).
- Core paths and operational settings come from `src/config.py`; data/log directories are created automatically on import.

## Key repository-specific conventions

- **Results-first components:** ingestion components are designed to return typed result objects (`DownloadResult`, `ConversionResult`, `RenderResult` in `src/models/results.py`) rather than writing DB state directly.
- **Persistence separated from execution:** `IDRDPipeline` persists stage outcomes via helpers in `src/utils/db_utils.py` (`persist_download_results`, `persist_conversion_results`) after batch execution.
- **Shared DB queue contract:** each stage pulls work from DB queue methods (`get_papers_needing_*`) instead of scanning files directly, then updates stage flags in `publications`.
- **`paperId` is canonical and quoted in SQL:** keep using exact `"paperId"` column naming in SQL statements and joins (mixed-case identifier).
- **Concurrent mode is stage-internal only:** `--mode concurrent` parallelizes work inside stages (thread pools), but stage order remains fetch → download → convert → render.
- **Download/convert robustness patterns are intentional:** downloader validates MIME type and `%PDF` magic bytes; converter manages GROBID container lifecycle (`start_grobid`/`stop_grobid`) and checks `/api/isalive`.
- **Renderer output assumptions feed extraction:** markdown rendering preserves citation markers and writes only cited references, which downstream extraction logic depends on.
