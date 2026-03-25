# IDRD Pipeline

A multi-stage pipeline for fetching academic publications, downloading their PDFs,
converting them to structured TEI-XML, extracting Markdown sections, and (in future phases)
extracting dataset features for a RAG system to evaluate affiliation with Maastricht University & check formality.

**NEW**: All components now support **database-agnostic operation** - use with DataFrames, JSON, or any data source! See [DataFrame Examples](#dataframe-usage-no-database).

---

## Requirements

```bash
pip install -e . 
```

Create a `.env` file in the project root:

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
Fetch → Download → Convert → Render Markdown in one command.

```bash
python src/main.py --query "implicit dataset references" --limit 50
```

---

### Resume Pipeline
Automatically detects the last incomplete stage and continues from there.
No need to specify which step to run — the pipeline reads the database state.

```bash
python src/main.py --resume
```

If no papers exist in the database yet, provide a query so Step 1 can run:
```bash
python src/main.py --resume --query "implicit dataset references" --limit 50
```

| DB State | Resumes at |
|---|---|
| No papers in DB | Step 1 — Fetch (requires `--query`) |
| Papers exist, no PDFs | Step 2 — Download |
| PDFs downloaded, no XML | Step 3 — Convert |
| XML converted, no Markdown | Step 4 — Render |
| All stages complete | Prints "nothing to resume" |

---

### Individual Steps

#### Step 1 — Fetch papers only
Store papers in the database without downloading anything.

```bash
python src/main.py --query "Transformers NLP" --limit 100 --fetch-only
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

#### Step 4 — Render Markdown only
Render structured Markdown from TEI XML files already in `data/xml/`.

```bash
python src/main.py --extract-only
```

Re-extract, overwriting existing `.md` files:
```bash
python src/main.py --extract-only --ex-overwrite
```

Limit how many to extract:
```bash
python src/main.py --extract-only --ex-limit 20
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
python src/main.py --reset status
```

Full database wipe — **deletes everything** (requires double confirmation):
```bash
python src/main.py --reset full
```

---

## Ground Truth Experiment

Run the pipeline in a fully isolated mode against the ground truth CSV.
Does **not** touch the main database or `data/pdf/`, `data/xml/`, `data/markdown/`.

```bash
# Full run — fetch + download + convert + extract markdown
python experiments/ground_truth/gt_runner.py

# Fetch metadata only — see what Semantic Scholar has before committing
python experiments/ground_truth/gt_runner.py --fetch-only

# Fetch + download, skip GROBID
python experiments/ground_truth/gt_runner.py --no-xml

# Skip markdown extraction
python experiments/ground_truth/gt_runner.py --no-extract

# Re-run extractor on existing XMLs only (no network calls)
python experiments/ground_truth/gt_runner.py --extract-only

# Use a custom CSV
python experiments/ground_truth/gt_runner.py --csv data/ground_truth/temp.csv
```

Output goes to `data/gt_experiment/`:
```
data/gt_experiment/
├── pdf/          ← downloaded PDFs
├── xml/          ← GROBID TEI XML
├── markdown/     ← extracted .md files
└── report_*.json ← coverage report per run
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
| 4 | Render Markdown | `src/ingestion/renderer.py` | ✅ Done |
| 5 | LLM feature extraction | `src/extraction/` | 🔲 Phase 3 |
| 6 | RAG / Vector search | `src/rag/` | 🔲 Phase 4 |

---

## DataFrame Usage (No Database)

**NEW**: All pipeline components can now be used without a database! Perfect for experiments, prototyping, and integration with existing pandas workflows.

### Quick Example

```python
import pandas as pd
from ingestion.downloader import PDFDownloader
from ingestion.converter import GrobidConverter
from ingestion.renderer import render_papers

# Download PDFs from a DataFrame
papers_df = pd.DataFrame([
    {'paperId': '123', 'url': 'https://example.com/paper.pdf'}
])

downloader = PDFDownloader(output_dir="./downloads")
results = downloader.download_papers(papers_df.to_dict('records'))

# Results are returned as structured objects
for r in results:
    print(f"{r.paper_id}: {r.success} - {r.message}")

# Convert results to DataFrame for analysis
results_df = pd.DataFrame([
    {'id': r.paper_id, 'success': r.success, 'size_mb': r.file_size_mb}
    for r in results
])

# Save to CSV instead of database
results_df.to_csv("results.csv")
```

### Full Examples

See `examples/dataframe_pipeline.py` for complete examples including:
- Download PDFs with DataFrame
- Convert PDFs to XML with DataFrame
- Render Markdown with DataFrame
- Full pipeline using only DataFrames and CSV files (no database!)

### Benefits

✅ **No database setup** - Perfect for quick experiments  
✅ **Pandas integration** - Works seamlessly with existing data workflows  
✅ **Easy testing** - Unit tests don't need database mocking  
✅ **Flexible storage** - Save to CSV, JSON, Parquet, or any format  
✅ **Reusable components** - Same code works with DB, DataFrame, or API

---

## Architecture: Results-Based Design

All components now follow a **results-based** architecture:

1. **Components** accept simple data (List[Dict]) - no database required
2. **Business logic** performs the operation (download, convert, render)
3. **Results** are returned as structured dataclasses (DownloadResult, ConversionResult, RenderResult)
4. **You decide** how to persist: database, DataFrame, JSON, or skip persistence entirely

```python
# Old (tightly coupled to database)
downloader = PDFDownloader(db=database)
downloader.download_from_database()  # ← Automatically updates DB

# New (database-agnostic)
downloader = PDFDownloader()  # No DB required!
results = downloader.download_papers(papers)  # ← Returns results

# You choose how to persist
persist_download_results(db, results)  # ← To database
results_df = pd.DataFrame([...])       # ← To DataFrame
json.dump(results, file)               # ← To JSON
```

---

## Project Structure

```
IDRD-Pipeline/
├── src/
│   ├── main.py                      ← pipeline entry point
│   ├── config.py                    ← all settings in one place
│   ├── db/
│   │   ├── db.py                    ← PostgreSQL database manager
│   │   └── __init__.py
│   ├── pubfetcher/
│   │   └── client.py                ← Semantic Scholar API client
│   ├── ingestion/
│   │   ├── downloader.py            ← PDF downloader
│   │   ├── converter.py             ← GROBID PDF → TEI XML converter
│   │   └── renderer.py              ← TEI XML → Markdown renderer
│   └── utils/
│       ├── db_utils.py              ← shared DB helpers
│       └── dict_parser.py           ← paper dict parser / exports
├── experiments/
│   └── ground_truth/
│       ├── gt_runner.py             ← isolated GT experiment entry point
│       ├── gt_fetcher.py            ← fetch GT papers from Semantic Scholar
│       ├── gt_downloader.py         ← download GT PDFs
│       └── gt_report.py             ← coverage report builder
├── data/
│   ├── pdf/                         ← downloaded PDFs
│   ├── xml/                         ← GROBID TEI XML files
│   ├── markdown/                    ← extracted Markdown files
│   └── ground_truth/
│       └── ground_truth.csv         ← ground truth paper list
├── data/gt_experiment/              ← isolated GT experiment output
│   ├── pdf/
│   ├── xml/
│   ├── markdown/
│   └── report_*.json
├── logs/
│   └── runs/                        ← per-run logs and metadata
├── .env                             ← credentials (do not commit)
├── ROADMAP.md
└── README.md
```

---

## Installation

```bash
git clone https://github.com/OmericoN/IDRD-Pipeline.git
cd IDRD-Pipeline
pip install -e .
```

---

## License

See [LICENSE](LICENSE) file for details.
