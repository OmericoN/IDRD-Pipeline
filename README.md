# IDRD Pipeline

A multi-stage pipeline for fetching academic publications, downloading their PDFs,
converting them to structured XML, extracting Markdown sections, and (in future phases)
extracting features for a RAG system.

---

## Requirements
- uv ( [installation guide](https://docs.astral.sh/uv/getting-started/installation/) )
```bash
uv sync
```
```bash
uv add <package_name>
```
```bash
uv run main.py -args
```

## Getting Started
```bash
git clone https://github.com/OmericoN/IDRD-Pipeline.git
```


Create a `.env` file in the project root:

```
SEMANTIC_SCHOLAR_API_KEY=your_key
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=idrd_pipeline
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
LLM_API_KEY=your_key
```

Make sure **PostgreSQL** is running and the database exists:

```bash
psql -U postgres -c "CREATE DATABASE idrd_pipeline;"
```

The schema is created automatically on first run.

---

## Running the Pipeline

All commands are run from the **project root**.

### Full Pipeline (recommended starting point)
Fetch → Download → Convert → Extract Markdown in one command.

(Example)
```bash
uv run src/main.py --query "implicit dataset references" --limit 50
```

---

### Resume Pipeline
Automatically detects the last incomplete stage and continues from there.
No need to specify which step to run - the pipeline reads the database state.

```bash
uv run src/main.py --resume
```

If no papers exist in the database yet, provide a query so Step 1 can run:
```bash
uv run src/main.py --resume --query "implicit dataset references" --limit 50
```

| DB State | Resumes at |
|---|---|
| No papers in DB | Step 1 — Fetch (requires `--query`) |
| Papers exist, no PDFs | Step 2 — Download |
| PDFs downloaded, no XML | Step 3 — Convert |
| XML converted, no Markdown | Step 4 — Extract |
| All stages complete | Prints "nothing to resume" |

---

### Individual Steps

#### Step 1 — Fetch papers only
Store papers in the database without downloading anything.

```bash
uv run src/main.py --query "Transformers NLP" --limit 100 --fetch-only
```

Filter by field of study:
```bash
uv run src/main.py --query "dataset" --limit 200 --fetch-only --fields-of-study "Computer Science"
```

Include non-open-access papers (no PDF URL required):
```bash
uv run src/main.py --query "dataset" --limit 200 --fetch-only --all-access
```

---

#### Step 2 — Download PDFs only
Download PDFs for papers already in the database.

```bash
uv run src/main.py --download-only
```

Limit how many to download, set delay, or force re-download:
```bash
uv run src/main.py --download-only --dl-limit 20 --dl-delay 1.0 --dl-overwrite
```

---

#### Step 3 — Convert PDFs to XML only
Requires **Docker** running. Starts a GROBID container automatically.

```bash
uv run src/main.py --convert-only
```

Delete PDFs after successful conversion (saves disk space):
```bash
uv run src/main.py --convert-only --delete-pdfs
```

Re-convert already converted files:
```bash
uv run src/main.py --convert-only --cv-overwrite
```

---

#### Step 4 — Extract Markdown only
Extract structured Markdown from TEI XML files already in `data/xml/`.

```bash
uv run src/main.py --extract-only
```

Re-extract, overwriting existing `.md` files:
```bash
uv run src/main.py --extract-only --ex-overwrite
```

Limit how many to extract:
```bash
uv run src/main.py --extract-only --ex-limit 20
```

---

### Check Status
See how many papers are at each pipeline stage.

```bash
uv run src/main.py --status
```

Example output:
```
══════════════════════════════════════════════════════════════════════
PIPELINE STATUS
══════════════════════════════════════════════════════════════════════
  Total papers          : 150
  PDFs downloaded       : 120
  Converted to XML      : 98
  Sections extracted    : 87
  Features extracted    : 0
  Download errors       : 5
  Conversion errors     : 2
══════════════════════════════════════════════════════════════════════
```

---

### Reset

Reset pipeline tracking flags (keeps all papers, allows re-running steps):
```bash
uv run src/main.py --reset status
```

Full database wipe — **deletes everything** (requires double confirmation):
```bash
uv run src/main.py --reset full
```

---


## All CLI Options

| Flag | Description | Default |
|---|---|---|
| `--query TEXT` | Semantic Scholar search query | required for fetch |
| `--limit N` | Max papers to fetch | 100 |
| `--fetch-only` | Only fetch papers (Step 1) | off |
| `--download-only` | Only download PDFs (Step 2) | off |
| `--convert-only` | Only convert PDFs to XML (Step 3) | off |
| `--extract-only` | Only extract Markdown from XMLs (Step 4) | off |
| `--resume` | Resume from last incomplete pipeline stage | off |
| `--no-xml` | Skip Step 3 in full pipeline | off |
| `--no-extract` | Skip Step 4 in full pipeline | off |
| `--all-access` | Include non-open-access papers | off |
| `--fields-of-study TEXT` | Filter by field e.g. `"Computer Science"` | none |
| `--dl-limit N` | Max PDFs to download | all |
| `--dl-overwrite` | Re-download existing PDFs | off |
| `--dl-delay N` | Seconds between downloads | 0.5 |
| `--cv-limit N` | Max PDFs to convert | all |
| `--cv-overwrite` | Re-convert existing XMLs | off |
| `--cv-delay N` | Seconds between conversions | 0.1 |
| `--delete-pdfs` | Delete PDFs after conversion | off |
| `--ex-limit N` | Max XMLs to extract | all |
| `--ex-overwrite` | Re-extract existing `.md` files | off |
| `--status` | Show pipeline status and exit | — |
| `--reset {status\|full}` | Reset pipeline tracking or full DB | — |

---

## Pipeline Stages

| # | Stage | Module | Status |
|---|---|---|---|
| 1 | Fetch papers | `src/pubfetcher/client.py` | ✅ Done |
| 2 | Download PDFs | `src/ingestion/downloader.py` | ✅ Done |
| 3 | Convert PDF → XML | `src/ingestion/converter.py` | ✅ Done |
| 4 | Extract Markdown | `src/ingestion/extractor.py` | ✅ Done |
| 5 | LLM feature extraction | `src/extraction/` | 🔲 Phase 3 |
| 6 | RAG / Vector search | `src/evaluation/` | 🔲 Phase 4 |
| 7 | Formality Evaluation | `` | 🔲 Phase 4 |

---

## License

See [LICENSE](LICENSE) file for details.
