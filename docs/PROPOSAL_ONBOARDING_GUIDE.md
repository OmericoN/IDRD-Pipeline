# IDRD Pipeline — Proposal & Onboarding Guide

> Historical note: this proposal guide is retained as background. Some prototype module names have been retired from the active codebase.

This document explains the project proposal, architecture decisions, and practical onboarding flow for new contributors.

---

## 1. Proposal Summary

The **IDRD Pipeline** (Implicit Dataset Reference Detection) is designed to build a high-quality corpus of papers for studying how datasets are referenced:

1. **Explicitly** (formal citation markers and bibliography entries)
2. **Implicitly** (dataset mention in narrative text without direct dataset citation)

### Core hypothesis

If we convert papers into a structured, LLM-friendly Markdown representation and preserve citation-linking context, we can reliably extract both explicit and implicit dataset references at scale.

### Primary research objective

Create a reproducible pipeline that:
- acquires relevant papers,
- transforms them into structured text,
- and enables robust downstream LLM extraction of dataset mentions and metadata.

---

## 2. Why This Approach

Raw PDFs are noisy and inconsistent for direct extraction. The proposal uses a staged architecture that improves reliability:

1. **Fetch metadata** from Semantic Scholar (discover relevant papers).
2. **Download PDFs** (acquire full text).
3. **Convert PDFs to TEI XML** using GROBID (recover structure).
4. **Render XML to curated Markdown** (remove low-signal content, preserve high-signal context).
5. **Extract dataset mentions with an LLM** (schema-driven, citation-aware).

This balances precision and scalability: deterministic preprocessing + probabilistic extraction.

---

## 3. LLM-Servable Design Principles

The pipeline is intentionally designed for LLM extraction quality:

- **Section-aware filtering:** reduce irrelevant text that confuses extraction.
- **Citation preservation:** keep in-text markers and linked reference labels.
- **Reference grounding:** include references in output for attribution and DOI resolution.
- **Conservative text cleaning:** remove OCR/XML artifacts without over-normalizing content.
- **Resumable state tracking:** stage flags in DB avoid duplicate or partial processing.

---

## 4. Section Policy (What We Keep vs Drop)

Current renderer policy in `src/ingestion/renderer.py` is aligned to your goal:

### Excluded section patterns

- Related Work
- Literature Review
- Discussion
- Conclusion
- Acknowledgements
- Funding
- Conflict of Interest / Declarations
- Supplementary / Appendix
- Author Contributions
- Abbreviations
- Ethical statements

### Included high-signal content

- Title and Authors
- Abstract
- Introduction
- Data / Methods / Methodology sections
- Results-oriented body sections (unless explicitly excluded)
- Inline citations
- Footnotes used in retained body text
- References (only those cited in retained content)

This keeps the text focused on dataset introduction and usage context while preserving citation grounding.

---

## 5. System Architecture (End-to-End)

```text
Semantic Scholar API
    -> Fetch metadata (src/pubfetcher/client.py)
    -> PostgreSQL state + metadata (src/db/db.py)
    -> Download PDFs (src/ingestion/downloader.py)
    -> Convert PDF -> TEI XML via GROBID (src/ingestion/converter.py)
    -> Render TEI -> Markdown (src/ingestion/renderer.py)
    -> LLM extraction (src/extraction/extractor.py; phase-dependent wiring)
```

### Pipeline orchestration

`uv run idrd` runs pipeline commands through the layered CLI interface:
- `pdf_downloaded`
- `xml_converted`
- `sections_extracted`
- `features_extracted`

---

## 6. Repository Map for New Contributors

| Path | Responsibility |
|---|---|
| `backend/src/idrd/interfaces/cli/main.py` | CLI entrypoint |
| `src/config.py` | Central configuration and directory setup |
| `src/db/db.py` | PostgreSQL schema, queries, stage queues |
| `src/pubfetcher/` | Semantic Scholar retrieval logic |
| `src/ingestion/downloader.py` | PDF acquisition and validation |
| `src/ingestion/converter.py` | GROBID lifecycle + TEI conversion |
| `src/ingestion/renderer.py` | TEI-to-Markdown rendering + section filtering |
| `src/extraction/` | LLM dataset extraction components |
| `src/utils/db_utils.py` | Shared persistence/state helper methods |
| `tests/` | Unit tests for core behavior |
| `logs/runs/<timestamp>/metadata/` | Per-run telemetry and runtime events |

---

## 7. Local Setup (Onboarding Quick Start)

## Prerequisites

- Python + `uv`
- PostgreSQL running locally
- Docker running (for GROBID conversion stage)

## Install dependencies

```bash
uv sync
```

## Configure environment

Create `.env` in repo root:

```env
SEMANTIC_SCHOLAR_API_KEY=your_key
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=idrd_pipeline
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
LLM_API_KEY=your_key
```

---

## 8. Operational Runbook

## Full pipeline

```bash
uv run idrd run-all --query "implicit dataset references" --limit 50 --um-datasets data/um_datasets.csv --output storage/exports/insights.csv
```

## Concurrent stage-internal execution

```bash
uv run idrd run-all --query "implicit dataset references" --limit 50 --um-datasets data/um_datasets.csv --output storage/exports/insights.csv --mode enqueue
```

## Resume from last incomplete stage

```bash
uv run idrd run-all --query "implicit dataset references" --limit 50 --um-datasets data/um_datasets.csv --output storage/exports/insights.csv --mode guided
```

## Stage-only commands

```bash
uv run idrd run-local discover --query "dataset" --limit 100
uv run idrd run-local download_pdf
uv run idrd run-local grobid_convert
uv run idrd run-local extract_features
```

## Status and tests

```bash
uv run idrd doctor
uv run pytest -q
```

---

## 9. Data and Persistence Model

The pipeline is **results-first and persistence-separated**:

- Stage components return structured results (`DownloadResult`, `ConversionResult`, `RenderResult`).
- Orchestrator-level helpers persist outcomes to DB.
- Stage queues are DB-driven (`get_papers_needing_*`) to ensure resumability and idempotent progression.

The mixed-case `"paperId"` identifier is canonical and should remain quoted in SQL.

---

## 10. Quality and Reliability Strategy

- **Idempotent reruns:** queue-based state checks prevent duplicate work.
- **Strict PDF validation:** MIME and `%PDF` magic-byte checks in downloader.
- **Managed conversion service:** converter starts/stops GROBID and checks health endpoint.
- **Reference-aware rendering:** references included only when cited in retained sections.
- **Telemetry:** runtime artifacts written per run under `logs/runs/.../metadata/`.

---

## 11. Contributor Workflow

1. Sync env (`uv sync`) and run tests (`uv run pytest -q`).
2. Pick one stage boundary (fetch/download/convert/render/extract).
3. Keep changes results-based and DB-agnostic at component level.
4. Persist via shared DB utilities where applicable.
5. Add/adjust tests for behavior changes.
6. Verify `--resume` and stage queue behavior are preserved.

---

## 12. Near-Term Roadmap

- Fully wire LLM feature extraction into the main orchestrated run.
- Expand evaluation for implicit-vs-explicit mention accuracy.
- Improve section normalization for better cross-paper consistency.
- Add richer lineage from in-text mention -> bibliography entry -> identifier.

---

## 13. First-Day Onboarding Checklist

1. Read `README.md` for command surface and flags.
2. Read `docs/SYSTEM_OVERVIEW.md` for deep technical context.
3. Run one paper through fetch -> extract to understand artifacts.
4. Inspect one generated markdown file in `data/markdown/`.
5. Review `src/ingestion/renderer.py` section filtering and citation handling.
6. Run `uv run pytest -q` before opening a PR.

