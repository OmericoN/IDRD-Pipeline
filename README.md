# IDRD Pipeline

A multi-stage pipeline for fetching academic publications, downloading their PDFs,
converting them to structured XML, and (in future phases) extracting features for
a RAG system.

---

## Requirements

```bash
pip install psycopg2-binary python-dotenv requests tqdm docker pandas openpyxl
```

Create a `.env` file in the project root (already present):

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=idrd_pipeline
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
SEMANTIC_SCHOLAR_API_KEY=your_key
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
Fetch papers → download PDFs → convert to XML in one command.

```bash
python src/main.py --query "implicit dataset references" --limit 50
```

---

### Individual Steps

#### Step 1 — Fetch papers only
Store papers in the database without downloading anything.

```bash
python src/main.py --query "Transformers NLP" --limit 100 --fetch-only
```

Skip citation context fetching (faster):
```bash
python src/main.py --query "Transformers NLP" --limit 100 --fetch-only --no-citations
```

Filter by field of study:
```bash
python src/main.py --query "dataset" --limit 200 --fetch-only --fields-of-study "Computer Science"
```

Include non-open-access papers (no PDF URL required):
```bash
python src/main.py --query "dataset" --limit 200 --fetch-only --all-access
```

---

#### Step 2 — Download PDFs only
Download PDFs for papers already in the database.

```bash
python src/main.py --download-only
```

Limit how many to download, set delay, or force re-download:
```bash
python src/main.py --download-only --dl-limit 20 --dl-delay 1.0 --dl-overwrite
```

---

#### Step 3 — Convert PDFs to XML only
Requires **Docker** running. Starts a GROBID container automatically.

```bash
python src/main.py --convert-only
```

Delete PDFs after successful conversion (saves disk space):
```bash
python src/main.py --convert-only --delete-pdfs
```

Re-convert already converted files:
```bash
python src/main.py --convert-only --cv-overwrite
```

---

### Check Status
See how many papers are at each pipeline stage.

```bash
python src/main.py --status
```

Example output:
```
══════════════════════════════════════════════════════════════════════
PIPELINE STATUS
══════════════════════════════════════════════════════════════════════
  Total papers          : 150
  PDFs downloaded       : 120
  Converted to XML      : 98
  Sections extracted    : 0
  Features extracted    : 0
  Download errors       : 5
  Conversion errors     : 2
══════════════════════════════════════════════════════════════════════
```

---

### Reset

Reset pipeline tracking flags (keeps all papers, allows re-running download/convert):
```bash
python src/main.py --reset status
```

Full database wipe — **deletes everything** (requires double confirmation):
```bash
python src/main.py --reset full
```

---

## All CLI Options

| Flag | Description | Default |
|---|---|---|
| `--query TEXT` | Semantic Scholar search query | required for fetch |
| `--limit N` | Max papers to fetch | 100 |
| `--fetch-only` | Only fetch papers, stop after Step 1 | off |
| `--download-only` | Only download PDFs (Step 2) | off |
| `--convert-only` | Only convert PDFs to XML (Step 3) | off |
| `--no-xml` | Skip Step 3 in full pipeline | off |
| `--no-citations` | Skip fetching citation contexts | off |
| `--all-access` | Include non-open-access papers | off |
| `--fields-of-study TEXT` | Filter by field e.g. `"Computer Science"` | none |
| `--dl-limit N` | Max PDFs to download | all |
| `--dl-overwrite` | Re-download existing PDFs | off |
| `--dl-delay N` | Seconds between downloads | 0.5 |
| `--cv-limit N` | Max PDFs to convert | all |
| `--cv-overwrite` | Re-convert existing XMLs | off |
| `--cv-delay N` | Seconds between conversions | 0.1 |
| `--delete-pdfs` | Delete PDFs after conversion | off |
| `--status` | Show pipeline status and exit | — |
| `--reset {status\|full}` | Reset pipeline tracking or full DB | — |

---

## Pipeline Stages

| # | Stage | Module | Status |
|---|---|---|---|
| 1 | Fetch papers | `src/pubfetcher/client.py` | ✅ Done |
| 2 | Download PDFs | `src/extractor/downloader.py` | ✅ Done |
| 3 | Convert PDF → XML | `src/extractor/converter.py` | ✅ Done |
| 4 | Extract sections | `src/extractor/extractor.py` | 🔲 Phase 2 |
| 5 | LLM feature extraction | `src/llm/` | 🔲 Phase 3 |
| 6 | RAG / Vector search | `src/rag/` | 🔲 Phase 4 |

---

## Tests

```bash
# Run all tests
python src/extractor/tests.py

# Run a specific test class
python -m unittest src.extractor.tests.TestPDFDownloader

# Run with coverage
pip install coverage
coverage run src/extractor/tests.py
coverage report
```

---

## Project Structure

```
IDRD-Pipeline/
├── src/
│   ├── main.py                  ← pipeline entry point (run this)
│   ├── config.py                ← all settings in one place
│   ├── db/
│   │   ├── db.py                ← PostgreSQL database manager
│   │   └── __init__.py
│   ├── pubfetcher/
│   │   └── client.py            ← Semantic Scholar API client
│   ├── extractor/
│   │   ├── downloader.py        ← PDF downloader
│   │   ├── converter.py         ← GROBID PDF → XML converter
│   │   ├── extractor.py         ← section extractor (Phase 2)
│   │   └── tests.py             ← unit tests
│   └── utils/
│       ├── db_utils.py          ← shared DB helpers
│       └── dict_parser.py       ← paper dict parser / exports
├── outputs/
│   ├── pdf/                     ← downloaded PDFs
│   ├── xml/                     ← converted TEI XML files
│   └── metadata/                ← JSON backups & results
├── .env                         ← credentials (do not commit)
├── ROADMAP.md
└── README.md
```

## Installation

```bash
# Clone the repository
git clone https://github.com/OmericoN/IDRD-Pipeline.git
cd IDRD-Pipeline

# Install in development mode
pip install -e .
```


## License

See [LICENSE](LICENSE) file for details.
