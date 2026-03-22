# IDRD Pipeline — System Overview

> **Document purpose:** A detailed technical reference for each package and module in the IDRD Pipeline. Intended for progress reporting, internal documentation, and onboarding.
>
> **Last updated:** 2026-03-12

---

## Table of Contents

1. [Project Purpose](#1-project-purpose)
2. [System Architecture](#2-system-architecture)
3. [Technology Stack](#3-technology-stack)
4. [Configuration Layer — `src/config.py`](#4-configuration-layer)
5. [Pipeline Orchestrator — `src/main.py`](#5-pipeline-orchestrator)
6. [Database Layer — `src/db/`](#6-database-layer)
7. [Publication Fetcher — `src/pubfetcher/`](#7-publication-fetcher)
8. [PDF Downloader — `src/ingestion/downloader.py`](#8-pdf-downloader)
9. [GROBID Converter — `src/ingestion/converter.py`](#9-grobid-converter)
10. [Markdown Renderer — `src/ingestion/renderer.py`](#10-markdown-renderer)
11. [LLM Dataset Extractor — `src/extraction/extractor.py`](#11-llm-dataset-extractor)
12. [Utility Modules — `src/utils/`](#12-utility-modules)
13. [Ground Truth Experiment — `experiments/ground_truth/`](#13-ground-truth-experiment)
14. [Development Status & Roadmap](#14-development-status--roadmap)

---

## 1. Project Purpose

The **IDRD Pipeline** (Implicit Dataset Reference Detection) is a research data pipeline designed to build a structured corpus of academic publications for studying how datasets are referenced in scientific literature.

The core research question driving the system is: *How do academic papers reference datasets — both explicitly (with a formal citation) and implicitly (by describing the dataset in narrative text without a direct citation entry)?* Answering this at scale requires automated acquisition, processing, and structured information extraction from large numbers of papers.

The pipeline addresses four sub-problems in sequence:
1. **Discovery** — Identifying open-access academic papers relevant to the research topic via the Semantic Scholar API.
2. **Acquisition** — Downloading the raw PDF documents.
3. **Structuring** — Converting unstructured PDFs into machine-readable structured text using GROBID (a deep-learning-based document parser).
4. **Extraction** — Using a locally-hosted Large Language Model (LLM) to identify and classify dataset mentions within the structured text.

---

## 2. System Architecture

The system follows a **linear, resumable pipeline** architecture. Each stage transforms data and records its completion status in a central PostgreSQL database, enabling any run to be interrupted and resumed without reprocessing already-completed work.

```
┌──────────────────────────────────────────────────────────────────────┐
│                         IDRD Pipeline                                │
│                                                                      │
│  ┌─────────────────┐                                                 │
│  │ Semantic Scholar │  REST API (HTTPS)                              │
│  │      API         │──────────────────┐                             │
│  └─────────────────┘                   ▼                             │
│                              ┌───────────────────┐                  │
│                              │  pubfetcher/       │                  │
│                              │  client.py         │  Paper metadata  │
│                              │  SemanticScholar   │────────────────► │
│                              │  Client            │                  │
│                              └───────────────────┘                  │
│                                        │                             │
│                                        ▼                             │
│                              ┌───────────────────┐                  │
│                              │  db/db.py          │◄─────────────── │
│                              │  Publication       │  All stages      │
│                              │  Database          │  write status    │
│                              │  (PostgreSQL)      │  flags here      │
│                              └───────────────────┘                  │
│                                        │                             │
│                         PDF URL queried│                             │
│                                        ▼                             │
│                              ┌───────────────────┐                  │
│                              │  ingestion/        │                  │
│                              │  downloader.py     │  data/pdf/       │
│                              │  PDFDownloader     │──────────────►   │
│                              └───────────────────┘                  │
│                                        │                             │
│                                        ▼                             │
│  ┌─────────────────┐         ┌───────────────────┐                  │
│  │  GROBID          │  HTTP  │  ingestion/        │                  │
│  │  Docker          │◄───────│  converter.py      │  data/xml/       │
│  │  Container       │        │  GrobidConverter   │──────────────►   │
│  │  (port 8070)     │────────►                    │                  │
│  └─────────────────┘  TEI   └───────────────────┘                  │
│                        XML                                           │
│                                        │                             │
│                                        ▼                             │
│                              ┌───────────────────┐                  │
│                              │  ingestion/        │                  │
│                              │  renderer.py       │  data/markdown/  │
│                              │  extract_markdown  │──────────────►   │
│                              └───────────────────┘                  │
│                                        │                             │
│                                        ▼                             │
│  ┌─────────────────┐         ┌───────────────────┐                  │
│  │  Ollama          │ HTTP   │  extraction/       │                  │
│  │  (qwen2.5:7b)    │◄───────│  extractor.py      │  Structured      │
│  │  Local LLM       │        │  extract_datasets  │  JSON output     │
│  │  (port 11434)    │────────►                    │──────────────►   │
│  └─────────────────┘        └───────────────────┘                  │
└──────────────────────────────────────────────────────────────────────┘
```

**Data directories** produced by each stage:

| Stage | Input | Output directory |
|-------|-------|-----------------|
| Fetch | Semantic Scholar API | PostgreSQL DB |
| Download | `open_access_pdf_url` from DB | `data/pdf/` |
| Convert | `data/pdf/*.pdf` | `data/xml/*.tei.xml` |
| Render | `data/xml/*.tei.xml` | `data/markdown/*.md` |
| Extract | `data/markdown/*.md` | Structured records (Phase 3) |

**Per-run logs** are saved to `logs/runs/<timestamp>/metadata/` as JSON files for each stage.

---

## 3. Technology Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Language | Python 3.x | Entire pipeline |
| Database | PostgreSQL + psycopg2 | Paper metadata & pipeline state |
| PDF parsing | GROBID 0.8.0 (Docker) | PDF → TEI XML conversion |
| XML parsing | lxml | TEI XML → structured Markdown |
| API client | requests | Semantic Scholar & GROBID REST APIs |
| LLM inference | Ollama (qwen2.5:7b) | Dataset mention extraction |
| LLM structured output | Instructor + Pydantic | Schema-enforced JSON from LLM |
| Container management | Docker Python SDK | GROBID container lifecycle |
| Progress display | tqdm | CLI progress bars |
| Configuration | python-dotenv | `.env`-based secrets management |
| Data export | pandas | DataFrame, CSV, Excel export |

---

## 4. Configuration Layer

**File:** `src/config.py`

The configuration module is the **single source of truth** for all settings, paths, and credentials in the system. Every other module imports constants from here rather than defining them locally.

```python
# All paths are derived from the project root — no hardcoded absolute paths
PROJECT_ROOT = Path(__file__).parent.parent
PDF_DIR      = PROJECT_ROOT / "data" / "pdf"
XML_DIR      = PROJECT_ROOT / "data" / "xml"
MARKDOWN_DIR = PROJECT_ROOT / "data" / "markdown"
RUNS_DIR     = PROJECT_ROOT / "logs" / "runs"
```

All four data directories are **created automatically on import** (`path.mkdir(parents=True, exist_ok=True)`), so the system is self-bootstrapping — no manual directory setup is needed.

Credentials (API keys, database connection details) are loaded from a `.env` file at the project root using `python-dotenv`. The config module constructs a `POSTGRES_DSN` connection string from the individual components. An `LLM_BASE_URL` constant is already defined for the Phase 3 Groq integration.

**Key constants exposed:**

| Constant | Purpose |
|----------|---------|
| `SEMANTIC_SCHOLAR_API_URL` | Base URL for the Semantic Scholar Graph API |
| `SEMANTIC_SCHOLAR_API_KEY` | API key (10 req/s with key vs. 1 req/s without) |
| `POSTGRES_DSN` | Full PostgreSQL connection string |
| `PDF_DIR`, `XML_DIR`, `MARKDOWN_DIR` | Stage output directories |
| `RUNS_DIR` | Root for per-run log directories |
| `LLM_API_KEY`, `LLM_BASE_URL` | For Phase 3 LLM integration |

---

## 5. Pipeline Orchestrator

**File:** `src/main.py` | **Class:** `IDRDPipeline`

`main.py` is the top-level entry point for the system. It owns the `IDRDPipeline` class, which coordinates all five pipeline stages, and exposes a fully featured CLI via `argparse`.

### `IDRDPipeline` Class

On instantiation, the class:
- Opens the sole `IDRDDatabase` connection for the session.
- Creates a timestamped run directory under `logs/runs/`.
- Prints a startup banner with all configured paths.

All downstream components (`PDFDownloader`, `GrobidConverter`) receive this shared `db` instance via dependency injection:

```python
self.db = IDRDDatabase()
downloader = PDFDownloader(db=self.db)  # shared, no duplicate connection
converter  = GrobidConverter(db=self.db)
```

This is a key architectural decision — it ensures there is exactly one database connection per pipeline run.

### Pipeline Steps

| Method | Stage | Key action |
|--------|-------|-----------|
| `step_1_fetch_papers(query, limit, open_access_only)` | Fetch | Calls `SemanticScholarClient`, stores results in DB, saves JSON log |
| `step_2_download_pdfs(limit, overwrite, delay)` | Download | Syncs disk state with DB, calls `PDFDownloader.download_from_database()` |
| `step_3_convert_to_xml(limit, overwrite, delete_pdf)` | Convert | Starts GROBID container, calls `GrobidConverter.convert_from_database()` |
| `step_4_extract_markdown(limit, overwrite)` | Render | Iterates XML files, calls `extract_markdown()`, updates `sections_extracted` flag |
| `step_5_extract_features()` | LLM extract | Placeholder — Phase 3 |

Each step logs a JSON results file to `logs/runs/<timestamp>/metadata/`.

### Resume Logic

The `resume_pipeline()` method reads per-stage counts from the DB and automatically determines where to restart:

```
total == 0         → resume from Step 1 (fetch)
pdf_downloaded == 0 → resume from Step 2 (download)
xml_converted == 0  → resume from Step 3 (convert)
sections_extracted == 0 → resume from Step 4 (render)
all complete       → notify user, suggest --reset
```

This makes the pipeline safe to interrupt and restart at any point without reprocessing completed work.

### CLI Interface

The CLI is structured with **argument group prefixes** to keep flags organised:

| Group | Prefix | Example flags |
|-------|--------|--------------|
| Pipeline modes | — | `--fetch-only`, `--resume`, `--status`, `--reset` |
| Fetch options | — | `--query`, `--limit`, `--fields-of-study` |
| Download options | `--dl-*` | `--dl-limit`, `--dl-delay`, `--dl-overwrite` |
| Convert options | `--cv-*` | `--cv-limit`, `--cv-overwrite`, `--delete-pdfs` |
| Extract options | `--ex-*` | `--ex-limit`, `--ex-overwrite` |

The `--reset full` command requires two manual confirmation steps (typing `'yes'` and then the researcher's full name) to prevent accidental data loss.

---

## 6. Database Layer

**File:** `src/db/db.py` | **Class:** `IDRDDatabase`

The database layer uses **PostgreSQL** (via `psycopg2`) as the backbone of the pipeline. It stores all paper metadata, tracks pipeline stage completion per paper, and exposes query methods that the pipeline stages use to retrieve their work queues.

### Singleton Guard

The class includes a class-level instantiation counter that prints a warning and stack trace if more than one instance is created in a session:

```python
IDRDDatabase._init_count += 1
if IDRDDatabase._init_count > 1:
    traceback.print_stack(limit=8)  # surfaces accidental double-instantiation
```

All queries use `psycopg2.extras.RealDictCursor` so rows are returned as dictionaries rather than plain tuples.

### Database Schema

The schema consists of **12 tables** across two concerns: bibliographic metadata and pipeline state.

#### Core Metadata Tables

**`publications`** — central table; one row per paper
| Column | Type | Description |
|--------|------|-------------|
| `paperId` | TEXT PK | Semantic Scholar paper ID |
| `title`, `abstract`, `year`, `venue` | TEXT/INT | Core bibliographic fields |
| `citation_count`, `reference_count`, `influential_citation_count` | INT | Citation metrics |
| `doi`, `url` | TEXT | Identifiers and links |
| `is_open_access`, `open_access_pdf_url`, `open_access_pdf_status` | BOOL/TEXT | Open access status |
| `tldr` | TEXT | AI-generated summary from Semantic Scholar |
| `pdf_downloaded` | BOOL | **Pipeline flag** — Stage 2 complete |
| `pdf_path`, `pdf_download_error`, `pdf_download_date` | TEXT/TS | Download result details |
| `xml_converted` | BOOL | **Pipeline flag** — Stage 3 complete |
| `xml_path`, `xml_conversion_error`, `xml_conversion_date` | TEXT/TS | Conversion result details |
| `sections_extracted` | BOOL | **Pipeline flag** — Stage 4 complete |
| `features_extracted` | BOOL | **Pipeline flag** — Stage 5 complete (Phase 3) |
| `created_at`, `updated_at` | TIMESTAMP | Record timestamps |

**`authors`** — deduplicated author registry
- `authorId` (UNIQUE), `name` — linked to publications via `publication_authors` join table

**`publication_authors`** — many-to-many join (paper ↔ author) with `author_order`

**`external_ids`** — DOI, ArXiv, PubMed, DBLP, CorpusId per paper

**`open_access`** — URL, status (GREEN/GOLD/etc.), license, disclaimer

**`journals`** — journal name, volume, pages per paper

**`publication_types`** — normalised list (`JournalArticle`, `Conference`, etc.)

**`fields_of_study`** — normalised list (`Computer Science`, `Environmental Science`, etc.)

#### Citation Sub-schema

**`citations`** — one row per citing-paper→cited-paper relationship

**`citation_contexts`** — the sentence(s) in the citing paper where the citation appears

**`citation_intents`** — Semantic Scholar's intent classification (`background`, `methodology`, `result`)

**`citation_authors`** — authors of citing papers, attached to citation rows

#### All inserts use `ON CONFLICT ... DO UPDATE` (upsert) to make the pipeline idempotent — re-fetching the same paper updates its record without creating duplicates.

### Key Query Methods

| Method | Returns | Used by |
|--------|---------|---------|
| `get_pipeline_status()` | Stage counts across all papers | `main.py` (status display, resume logic) |
| `get_papers_needing_download(limit)` | Papers with a PDF URL but not yet downloaded | `PDFDownloader` |
| `get_papers_needing_conversion(limit)` | Papers downloaded but not yet converted | `GrobidConverter` |
| `search_publications(...)` | Filtered paper list with joins | Ad-hoc querying |
| `get_publication(paper_id)` | Full paper record including citations | Detail views |
| `get_statistics()` | Aggregate DB metrics | Reporting |
| `clear_db()` | Deletes all rows (keeps schema) | `--reset status` |

---

## 7. Publication Fetcher

**File:** `src/pubfetcher/client.py` | **Class:** `SemanticScholarClient`

This module interfaces with the [Semantic Scholar Graph API](https://api.semanticscholar.org/graph/v1) to search for and retrieve academic paper metadata.

### Rate Limiting & Retry Strategy

The API has two tiers: 1 request/second (anonymous) and 10 requests/second (authenticated). The client uses a 150ms inter-request delay (`_REQUEST_DELAY = 0.15`), which stays comfortably within the authenticated limit while avoiding hammering the server.

For transient errors, the `_fetch_batch()` private method implements **exponential backoff** with up to 10 retries:

```python
backoff = 5  # initial wait in seconds; doubles each retry
for attempt in range(max_retries):
    response = requests.get(url, ...)
    if response.status_code == 429:   # rate limited
        time.sleep(backoff * (2 ** attempt))
        continue
    if response.status_code >= 500:   # server error — transient
        time.sleep(backoff * (2 ** attempt))
        continue
    if 400 <= response.status_code < 500:
        return [], 0, f"Client error {response.status_code}"  # permanent, don't retry
```

This differentiates between **transient** errors (429, 5xx — retry) and **permanent** errors (4xx — fail immediately), avoiding wasted retries on invalid requests.

### Pagination

The API returns a maximum of 100 papers per request. For large queries, `search_papers()` automatically batches requests and uses a `tqdm` progress bar. It also adjusts the progress bar total if the actual number of results is smaller than the requested `limit`.

### Fields Requested

By default the client requests all fields relevant to the pipeline:

```
paperId, title, abstract, year, authors, citationCount, referenceCount,
influentialCitationCount, venue, publicationDate, publicationTypes,
journal, fieldsOfStudy, url, externalIds, isOpenAccess, openAccessPdf, tldr
```

The `open_access_pdf=True` filter sends both `isOpenAccess` and `openAccessPdf` parameters to the API, ensuring results have a downloadable PDF URL.

---

## 8. PDF Downloader

**File:** `src/ingestion/downloader.py` | **Class:** `PDFDownloader`

The downloader retrieves open-access PDF files for papers stored in the database and saves them to `data/pdf/`.

### Shared Database Pattern

The class accepts an optional `db` parameter. If provided (as it is when called from `IDRDPipeline`), it uses the shared connection. If not, it creates its own. A `_owns_db` flag controls whether `close()` should close the connection:

```python
self._owns_db = db is None
self.db = db if db is not None else IDRDDatabase()
...
def close(self):
    if self._owns_db:   # only close if we opened it
        self.db.close()
```

This same pattern is used in `GrobidConverter` and prevents accidental double-close errors.

### PDF Validation

Downloaded files are validated using **magic bytes** rather than file extension — the first 4 bytes of a valid PDF are always `%PDF`:

```python
def is_valid_pdf(self, filepath: Path) -> bool:
    with open(filepath, 'rb') as f:
        return f.read(4) == b'%PDF'
```

If validation fails after download, the file is deleted and the download is retried. This guards against servers returning HTML error pages with a `.pdf` URL and a 200 status code.

### Filename Sanitisation

Paper IDs from Semantic Scholar can contain characters invalid in filenames on Windows (`<>:"/\|?*`). The downloader strips these before saving:

```python
def generate_filename(self, paper_id: str) -> str:
    return f"{re.sub(r'[<>:\"/\\|?*]', '', paper_id)}.pdf"
```

### Content-Type Check

Before writing to disk, the response `Content-Type` header is checked. If neither `pdf` nor `application/octet-stream` appears in the content type, the download is rejected — catching cases where a server serves a redirect page or a login wall.

### Statistics Tracking

The class accumulates `{'successful', 'failed', 'skipped', 'total_size'}` counters and provides `get_statistics()` / `print_statistics()` methods for per-run reporting.

---

## 9. GROBID Converter

**File:** `src/ingestion/converter.py` | **Class:** `GrobidConverter`

GROBID (GeneRation Of BIbliographic Data) is an open-source machine learning library that parses academic PDFs and produces structured **TEI XML** output. The converter manages the GROBID Docker container and submits PDFs to its REST API.

### Container Lifecycle

The class uses the Docker Python SDK to:
1. **Pull** the GROBID image (`lfoppiano/grobid:0.8.0`) if not locally present.
2. **Start** the container (or restart if it exists but is stopped).
3. **Poll** the `/api/isalive` endpoint until GROBID signals readiness (up to 30 seconds).
4. **Stop** the container when conversion is complete.

The class implements the **context manager protocol** (`__enter__` / `__exit__`), so it can be used as:
```python
with GrobidConverter(db=db) as converter:
    converter.convert_from_database()
# container is automatically stopped here
```

### PDF-to-XML Conversion

Each PDF is submitted to GROBID's `processFulltextDocument` endpoint as a multipart POST:

```python
response = requests.post(
    f"{self.grobid_url}/api/processFulltextDocument",
    files={'input': f},
    timeout=300,   # 5-minute timeout for large PDFs
)
```

GROBID returns a TEI XML string on success (HTTP 200). The converter saves this to `data/xml/<paper_id>.tei.xml` and updates the database.

The `delete_pdf=True` option removes source PDFs after successful conversion to reclaim disk space.

### TEI XML Format

GROBID's output follows the [TEI (Text Encoding Initiative)](https://tei-c.org/) standard, a structured XML schema for scholarly texts. The output encodes:
- Document structure (title, authors, abstract, body sections, footnotes)
- Inline citations with links to the bibliography
- A full bibliographic entry list
- Tables and figures

This TEI XML is the input consumed by the Markdown Renderer in Stage 4.

---

## 10. Markdown Renderer

**File:** `src/ingestion/renderer.py` | **Entry point:** `extract_markdown(xml_path: Path) -> str`

The renderer converts a GROBID TEI XML file into a structured Markdown string suitable for downstream LLM processing. It uses `lxml` with namespace-aware XPath queries throughout.

### Section Filtering

Not all parts of a paper are relevant for dataset mention extraction. The renderer **silently drops** sections whose headings match any of these patterns:

```
discuss, conclusion, acknowledg, funding, conflict of interest,
declaration, supplementar, appendix, author contribution,
abbreviation, ethical
```

This focuses the downstream LLM on the sections most likely to contain dataset descriptions: **Introduction**, **Data Sources**, and **Methodology**.

### Markdown Output Structure

A rendered document contains the following sections in order:

```markdown
# Paper Title

**Authors:** First Last, First Last, ...

---

## Abstract

[Abstract text]

---

### 1. Introduction

[Body text with inline citations like [Dee, 2011] or [Smith et al., 2019]]

### 2. Data and Method

[...]

---

## References

- **[Dee, 2011]** Dee (2011). The ERA-Interim reanalysis. *Quarterly Journal...*
  DOI: [10.1002/qj.828](https://doi.org/10.1002/qj.828)
```

### Citation Resolution

Inline citations in the body text are rendered with dual labels that preserve both the original XML marker and the resolved bibliographic label:

```python
# If the original marker differs from the resolved label:
return f"[{original}={label}]"   # e.g., [(Dee et al. 2011)=Dee, 2011]
# If they match:
return f"[{label}]"              # e.g., [Dee, 2011]
```

This dual-label format is intentional — the LLM extractor uses the original marker as `placement_content` and the resolved label to look up the full reference.

### Reference Filtering

Only references that are **actually cited in the body text** appear in the `## References` section. The renderer tracks which `xml:id` values appear in `<ref type="bibr">` elements as it processes the body, then filters the bibliography accordingly. This significantly reduces the length of the output for papers with large bibliographies.

### Text Cleaning

The renderer applies conservative text cleaning:
- Strips XML conversion artefacts (runs of isolated uppercase letters like `K I M E T`).
- Removes control characters.
- Collapses multiple spaces.
- Drops `<sup>`, `<sub>`, and `<formula>` elements (footnote markers, superscripts, equations) from body text while preserving surrounding text.

Tables are rendered as Markdown tables, limited to 10 rows to avoid bloating the output. Figures are rendered as bold captions.

---

## 11. LLM Dataset Extractor

**File:** `src/extraction/extractor.py` | **Entry point:** `extract_datasets(text: str) -> List[DatasetMention]`

The extractor is the Phase 3 component. It uses a locally-hosted LLM (`qwen2.5:7b` via Ollama) with the **Instructor** library to extract structured dataset mentions from the Markdown documents produced by the renderer.

### Architecture: Instructor + Pydantic

[Instructor](https://python.useinstructor.com/) wraps the OpenAI-compatible API exposed by Ollama and enforces that the model's response conforms to a Pydantic schema. This is more reliable than asking the model to produce JSON directly, because Instructor handles retries and validation internally.

```python
client = instructor.from_openai(
    OpenAI(base_url="http://localhost:11434/v1", api_key="ollama"),
    mode=instructor.Mode.JSON
)
```

### `DatasetMention` Schema

Each extracted dataset is represented as a `DatasetMention` Pydantic model with 17 fields. The field `description` attributes serve as the per-field prompt for the LLM:

| Field | Description |
|-------|-------------|
| `dataset_name` | Formal name or acronym of the dataset (e.g., "ERA5", "GLEAM") |
| `mention_type` | `"explicit"` (has inline citation) or `"implicit"` (described without formal citation) |
| `reference_directness` | `"direct"` (points to data repository) or `"indirect"` (points to descriptive paper) |
| `mention_in_abstract` | Verbatim sentence from `## Abstract` mentioning the dataset |
| `mention_in_full_text` | Verbatim sentence from body text where dataset is first introduced |
| `mention_section` | Exact section heading under which the dataset is introduced |
| `standardized_section` | Mapped to one of: `Introduction`, `Data Sources`, `Methodology`, `Results` |
| `reference_title` | Full title from the `## References` section (requires cross-referencing) |
| `persistent_identifier` | DOI or URL from the matching reference entry |
| `dataset_authors` | Author surnames from the citation |
| `dataset_year` | Year of the dataset citation |
| `dataset_url` | Direct URL to data repository (if present) |
| `placement_type` | `"inline text"` or `"bibliography"` |
| `placement_content` | Exact citation marker as it appears in text |
| `reference_material` | `"data paper"`, `"repository"`, or `"website"` |
| `material_year` | Publication year of the reference material |
| `dataset_version` | Version identifier if mentioned |
| `access_date` | Date authors accessed the dataset |

All fields default to `"none"` — the model is instructed never to hallucinate values.

### System Prompt Design

The system prompt is concise (optimised for a 7b-parameter model) and includes four rules:

1. **Scope** — Only extract datasets used in the paper's own methodology/research; skip related work.
2. **Implicit vs Explicit** — Explicit means a formal citation marker (`[Author, Year]`) links the dataset to the bibliography; implicit means the dataset is described in narrative text without such a link.
3. **Citation Resolution** — Cross-reference citation markers to the `## References` section to find titles and DOIs.
4. **No Hallucination** — If a value cannot be found in the text, output `"none"`.

### Semantic Chunking

The full Markdown document is too large to process in a single LLM call. The chunking strategy is designed to maximise context quality:

1. The `## References` section is extracted first and stored separately.
2. The remaining body is split on Markdown section headings (`##` or `###`).
3. Sections are grouped into chunks up to ~12,000 characters (well within qwen2.5:7b's 32k token context).
4. The **full References section is appended to every chunk** — this ensures citation resolution always has access to the bibliography, regardless of which chunk the citation appears in.

```python
body, references = _extract_references_section(text)
sections = _split_into_sections(body)   # split on ## / ###
# group sections into ≤12000 char chunks, then:
chunk = "\n\n".join(section_group) + "\n\n" + references
```

### Deduplication

After all chunks are processed, results are deduplicated by `dataset_name` (case-insensitive). When the same dataset appears in multiple chunks, the most **complete** record is kept — measured by the number of non-`"none"` fields:

```python
def _completeness_score(d: DatasetMention) -> int:
    return sum(1 for v in d.model_dump().values()
               if str(v).strip().lower() != "none")
```

---

## 12. Utility Modules

### `src/utils/dict_parser.py` — `PaperDictParser`

`PaperDictParser` transforms raw Semantic Scholar API response dictionaries into clean, consistently structured Python dicts and provides multi-format export.

**Open access status** is derived with a priority rule: first checks the explicit `isOpenAccess` flag, then falls back to checking the `openAccessPdf.status` field for GREEN/GOLD/HYBRID/BRONZE values.

**Export methods:**

| Method | Output |
|--------|--------|
| `to_json(filename)` | JSON file with full nested structure |
| `to_dataframe()` | Flattened pandas DataFrame |
| `to_csv(filename)` | CSV of the flattened DataFrame |
| `to_excel(filename)` | Excel file (requires openpyxl) |
| `save_all_formats(base_filename)` | JSON + CSV + Excel in one call |

**Statistics** (`get_statistics()`): returns year range, citation mean/median/max, counts of papers with abstracts/DOIs/open-access, top 5 venues, and detailed citation context statistics.

**Filtering** (`filter_papers(...)`): supports filtering by minimum citation count, year range, presence of abstract, and open-access status.

### `src/utils/db_utils.py` — Shared DB Helpers

This module centralises repeated SQL patterns to prevent code duplication across the downloader, converter, and orchestrator:

| Function | Purpose |
|----------|---------|
| `print_download_status(db, output_dir)` | Prints download counts from DB + disk file count |
| `print_conversion_status(db, xml_dir)` | Prints conversion counts from DB + XML file count |
| `sync_existing_pdfs(db, pdf_dir)` | Marks DB records as downloaded if PDF exists on disk but flag is unset (recovers from interrupted runs) |
| `update_pdf_status(db, paper_id, success, ...)` | Atomic DB update for a single PDF download result |
| `update_xml_status(db, paper_id, success, ...)` | Atomic DB update for a single XML conversion result |

The `sync_existing_pdfs` function is particularly important for resilience: if the pipeline was interrupted mid-download, files on disk may not match DB flags. This function reconciles the two on the next run.

---

## 13. Ground Truth Experiment

**Directory:** `experiments/ground_truth/`

The ground truth experiment is a **fully isolated** validation pipeline that runs the complete processing chain on a curated set of 20 hand-selected papers, independent of the main pipeline and its database.

### Purpose

The main pipeline is designed for large-scale, exploratory corpus building. The ground truth experiment serves a different purpose: measuring how well the pipeline performs on a known set of papers where the expected outcome (dataset mentions, coverage) is pre-defined. This makes it a reproducible evaluation tool.

### Isolation Design

The experiment deliberately avoids touching any main pipeline state:

| Resource | Main Pipeline | GT Experiment |
|----------|--------------|---------------|
| Database | PostgreSQL (`idrd_pipeline`) | **None** — no DB connection |
| PDFs | `data/pdf/` | `data/gt_experiment/pdf/` |
| XML | `data/xml/` | `data/gt_experiment/xml/` |
| Markdown | `data/markdown/` | `data/gt_experiment/markdown/` |

### Ground Truth CSV

`data/ground_truth/ground_truth.csv` contains 20 manually curated papers with fields:
`Publication ID | Title | Type | Year | Authors | DOI | URL`

Papers are identified by both title and DOI for robust matching against the Semantic Scholar API.

### Modules

**`gt_fetcher.py`** — Loads `ground_truth.csv` and queries the Semantic Scholar API by title and DOI for each paper. Returns structured paper records.

**`gt_downloader.py`** — Downloads PDFs to `data/gt_experiment/pdf/`. Uses the same validation and retry logic as the main `PDFDownloader` but without DB dependency.

**`gt_runner.py`** — Orchestrates the full GT pipeline in sequence (fetch → download → convert → extract → report). Wipes the experiment directories at the start of each run for reproducibility. Supports partial runs via CLI flags:

```bash
python experiments/ground_truth/gt_runner.py              # full run
python experiments/ground_truth/gt_runner.py --no-extract # skip Markdown extraction
python experiments/ground_truth/gt_runner.py --no-xml     # skip GROBID conversion
python experiments/ground_truth/gt_runner.py --fetch-only # fetch metadata only
```

**`gt_report.py`** — Builds a coverage report as a JSON file saved to `data/gt_experiment/report_<timestamp>.json`. The report records per-paper and aggregate success rates across each pipeline stage (fetch, download, convert, extract).

---

## 14. Development Status & Roadmap

### Current Status

| Stage | Status | Notes |
|-------|--------|-------|
| **Stage 1** — Fetch | ✅ Complete | Semantic Scholar API, pagination, retry |
| **Stage 2** — Download | ✅ Complete | Retry, validation, DB integration |
| **Stage 3** — Convert | ✅ Complete | GROBID Docker, TEI XML output |
| **Stage 4** — Render | ✅ Complete | TEI XML → Markdown, citation resolution |
| **Stage 5** — Extract | 🔧 In Progress | LLM extraction working standalone; not yet wired into `main.py` |
| Ground Truth Experiment | ✅ Complete | Fully isolated, coverage report generation |

### Planned Phases

**Phase 2 — Engineering Quality**
- Replace `print` statements with the `logging` module throughout
- Add `alembic` for database schema migrations
- Add connection pooling for PostgreSQL
- Build a full `pytest` test suite (currently only `unittest` in `src/ingestion/tests.py`)
- Docker Compose file to start all services (PostgreSQL + GROBID + Ollama) in one command

**Phase 3 — LLM Feature Extraction (In Progress)**
- Wire `extract_datasets()` from `extractor.py` into `main.py` as `step_5_extract_features()`
- Store extraction results in the database (`dataset_mentions` table)
- Evaluate extraction quality against the ground truth corpus

**Phase 4 — RAG & Vector Search**
- Add `pgvector` extension to PostgreSQL
- Generate embeddings for paper abstracts and dataset mention contexts
- Implement semantic similarity search over the corpus

**Phase 5 — Scale & Reliability**
- Ingest the full S2ORC (Semantic Scholar Open Research Corpus) snapshot
- Thread pooling for parallel downloads and conversions
- CI/CD pipeline
- Monitoring dashboard

### Known Limitations

- The LLM extractor (`qwen2.5:7b`) is not yet integrated into the main pipeline CLI — it must be run as a standalone script.
- GROBID's PDF parsing quality varies by document; scanned PDFs and non-standard layouts produce lower-quality TEI XML.
- The `sections_extracted` DB flag (Stage 4) is updated but `features_extracted` (Stage 5) is not yet set by any code.
- No automated test coverage for `pubfetcher`, `db`, `renderer`, or `extractor` modules.
