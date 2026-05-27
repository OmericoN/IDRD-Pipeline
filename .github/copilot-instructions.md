# Copilot Instructions for IDRD-Pipeline

## Build, test, and lint commands

- Install/sync backend dependencies from `backend/`: `uv sync`
- Run the backend test suite from `backend/`: `uv run pytest -q`
- Run a single backend test file from `backend/`: `uv run pytest -q tests/test_repository.py`
- Run a single backend test case from `backend/`: `uv run pytest -q tests/test_stage_registry.py::test_stage_registry_builds_discover_arguments`
- Install frontend dependencies from `frontend/`: `bun install`
- Run frontend checks from `frontend/`: `bun run build` and `bun run test`

There is no dedicated lint/format command configured in this repo (`ruff`, `flake8`, `black`, etc. are not configured).

## High-level architecture

- The project is a resumable, stage-based pipeline orchestrated through `idrd.application.orchestrator` and exposed by `idrd.interfaces.cli.main`:
  1. Fetch paper metadata from Semantic Scholar
  2. Download PDFs
  3. Convert PDFs to TEI XML with GROBID (Docker-managed)
  4. Render TEI XML to Markdown
  5. LLM feature extraction (placeholder/not wired into pipeline run)
- Pipeline state is persisted in PostgreSQL through `idrd.infrastructure.persistence.PipelineRepository`.
- Run orchestration records pipeline runs, stage runs, and structured events in PostgreSQL.
- Core paths and operational settings come from `idrd.config`; generated storage directories are created automatically on import.

## Key repository-specific conventions

- **Results-first components:** ingestion components are designed to return typed result objects (`DownloadResult`, `ConversionResult`, `RenderResult` in `idrd.domain.results`) rather than writing DB state directly.
- **Persistence separated from execution:** application use cases persist stage outcomes through repository methods after batch execution.
- **Shared DB queue contract:** each stage pulls work from DB queue methods (`get_papers_needing_*`) instead of scanning files directly, then updates stage flags in `publications`.
- **`paper_id` is canonical internally:** keep API payload conversion and SQL mappings consistent with the current Alembic schema.
- **Queue mode is orchestration-level only:** full runs can execute locally or through Celery, but stage order remains discover → download → convert → render → detect → extract → match → export.
- **Download/convert robustness patterns are intentional:** downloader validates MIME type and `%PDF` magic bytes; converter manages GROBID container lifecycle (`start_grobid`/`stop_grobid`) and checks `/api/isalive`.
- **Renderer output assumptions feed extraction:** markdown rendering preserves citation markers and writes only cited references, which downstream extraction logic depends on.
