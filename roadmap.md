# IDRD Pipeline — Roadmap

## Project Overview
A research paper processing pipeline that fetches academic papers from Semantic
Scholar, downloads PDFs, converts them to structured TEI XML, extracts Markdown
sections, and (eventually) uses an LLM to extract features for a RAG system.

---

## Current Status

| Step | Module | Status | Notes |
|------|--------|--------|-------|
| 1. Fetch papers | `src/pubfetcher/client.py` | ✅ Done | |
| 2. Parse & store | `src/db/db.py` | ✅ Done | PostgreSQL |
| 3. Download PDFs | `src/ingestion/downloader.py` | ✅ Done | |
| 4. Convert PDFs → XML | `src/ingestion/converter.py` | ✅ Done | GROBID via Docker |
| 5. Extract Markdown | `src/ingestion/extractor.py` | ✅ Done | TEI XML → `.md` |
| 6. Ground truth experiment | `experiments/ground_truth/` | ✅ Done | Fully isolated |
| 7. LLM feature extraction | `src/llm/` | 🔲 Phase 3 | Not started |
| 8. RAG / Vector search | `src/rag/` | 🔲 Phase 4 | pgvector planned |

---

## Phase 1 — Core Pipeline ✅ COMPLETE

- [x] Fetch papers from Semantic Scholar API
- [x] Parse and store in PostgreSQL
- [x] Download open-access PDFs
- [x] Convert PDFs to TEI XML via GROBID (Docker)
- [x] Extract structured Markdown from TEI XML
- [x] CLI with individual step flags and full pipeline mode
- [x] Ground truth experiment runner (fully isolated)
- [x] SQL injection hardening (`psycopg2.sql.Identifier`, parameterised LIMIT)
- [x] Per-run logs saved to `logs/runs/`

---

## Phase 2 — Quality & Robustness 🔧

- [ ] Add proper logging (`logging` module, replace `print` statements)
- [ ] Add `alembic` for DB schema migrations
- [ ] Add connection pooling (`psycopg2.pool`)
- [ ] Full test suite with `pytest` + `pytest-postgresql`
- [ ] Docker Compose for Postgres + GROBID + app together
- [ ] `requirements.txt` audit and pin versions

---

## Phase 3 — LLM Feature Extraction 🤖

#### 3.1 Create `src/llm/` module
- `client.py` — LLM API wrapper (OpenAI / local Ollama)
- `prompts.py` — prompt templates per section type
- `extractor.py` — run prompts over extracted Markdown sections

#### 3.2 Features to extract (per paper)
- Methodology used
- Datasets referenced (implicit + explicit)
- Metrics and results
- Limitations
- Key findings

#### 3.3 Add `features` table to `db.py`
Store structured LLM output per paper.

#### 3.4 Wire up `step_5_extract_features()` in `main.py`
Currently a placeholder — implement once `src/llm/` is ready.

#### 3.5 Update `config.py`
Add `LLM_MODEL`, `LLM_API_KEY`, `LLM_BASE_URL`, `MAX_TOKENS`.

---

## Phase 4 — RAG & Vector Search 🔍

#### 4.1 Enable pgvector
```sql
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE publications ADD COLUMN embedding vector(1536);
CREATE INDEX ON publications USING hnsw (embedding vector_cosine_ops);
```

#### 4.2 Create `src/rag/` module
- `embedder.py` — generate embeddings from Markdown sections
- `retriever.py` — vector similarity search via pgvector
- `pipeline.py` — end-to-end RAG query handler

#### 4.3 Populate embeddings
Run embedder over all extracted sections and store in DB.

---

## Phase 5 — Scale (Long Term) 🚀

- [ ] Replace Semantic Scholar API with local S2ORC snapshot (~300 GB metadata)
  - Drop-in replacement for `client.py` — same `search_papers()` interface
  - Load S2ORC JSONL shards into PostgreSQL
  - Elasticsearch or pgvector for full-text title/abstract search
- [ ] `ThreadPoolExecutor` in downloader (3–5 workers) for I/O throughput
- [ ] CI/CD with GitHub Actions
- [ ] Monitoring dashboard for pipeline runs

---

## Project Structure (Current)

```
IDRD-Pipeline/
├── src/
│   ├── main.py                      ← pipeline entry point
│   ├── config.py                    ← all settings
│   ├── db/
│   │   ├── db.py                    ← PostgreSQL manager
│   │   └── __init__.py
│   ├── pubfetcher/
│   │   └── client.py                ← Semantic Scholar client
│   ├── ingestion/
│   │   ├── downloader.py            ← PDF downloader
│   │   ├── converter.py             ← GROBID converter
│   │   └── extractor.py             ← TEI XML → Markdown
│   └── utils/
│       ├── db_utils.py              ← shared helpers
│       └── dict_parser.py           ← paper dict parser
├── experiments/
│   └── ground_truth/
│       ├── gt_runner.py             ← GT experiment entry point
│       ├── gt_fetcher.py            ← fetch GT papers
│       ├── gt_downloader.py         ← download GT PDFs
│       └── gt_report.py             ← coverage report
├── data/
│   ├── pdf/
│   ├── xml/
│   ├── markdown/
│   ├── ground_truth/
│   │   └── ground_truth.csv
│   └── gt_experiment/
│       ├── pdf/
│       ├── xml/
│       ├── markdown/
│       └── report_*.json
├── logs/
│   └── runs/
├── .env
├── ROADMAP.md
└── README.md
```