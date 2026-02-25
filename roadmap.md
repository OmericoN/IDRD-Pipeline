# IDRD Pipeline — Roadmap

## Project Overview
A research paper processing pipeline that fetches academic papers from Semantic
Scholar, downloads PDFs, converts them to structured XML, extracts sections, and
(eventually) uses an LLM to extract features for a RAG system.

---

## Current Status

| Step | Module | Status | Notes |
|------|--------|--------|-------|
| 1. Fetch papers | `src/pubfetcher/client.py` | ✅ Done | Renamed from `fetching.py` |
| 2. Parse & store | `src/db/db.py` | ✅ Done | Migrated SQLite → PostgreSQL |
| 3. Download PDFs | `src/extractor/downloader.py` | ✅ Done | Still has SQLite-style raw SQL |
| 4. Convert PDFs → XML | `src/extractor/converter.py` | ✅ Done | Still has SQLite-style raw SQL |
| 5. Extract sections | `src/extractor/extractor.py` | ❌ Empty | Not started |
| 6. LLM feature extraction | TBD | ❌ Not started | RAG prep |
| 7. RAG / Vector search | TBD | ❌ Not started | pgvector planned |
| Tests | `src/extractor/tests.py` | ⚠️ Partial | Tests still import old-style DB |
| Config | `src/config.py` | ⚠️ Incomplete | Missing DB + LLM config |

---

## Known Issues Right Now

1. **`downloader.py` and `converter.py`** still use raw SQLite-style queries
   (`?` placeholders, `= 0`, `= 1` for booleans) — they bypass `db.py` methods
   and talk directly to `self.db.cursor` with old syntax.
2. **`main.py`** calls `self.db.cursor` and `self.db.db_path` directly — both
   are no longer valid after the Postgres migration.
3. **`config.py`** is incomplete — no DB config, no LLM config.
4. **`extractor.py`** is empty.
5. **`src/db/__init__.py`** is empty — `PublicationDatabase` is not exported.
6. **Tests** mock `db_path` (SQLite) and will fail with the new DB class.
7. **Redundancy**: DB queries are duplicated inline in `downloader.py`,
   `converter.py`, and `main.py` instead of using `db.py` methods.

---

## Roadmap

### Phase 1 — Stabilise & Refactor (NOW) 🔧

#### 1.1 Fix `src/db/__init__.py`
Export `PublicationDatabase` so imports are clean across the project.

#### 1.2 Complete `src/config.py`
Add Postgres and future LLM config in one place.

#### 1.3 Fix `downloader.py`
- Replace all raw SQL + `?` placeholders with calls to `db.py` methods.
- Remove `db_path` argument (no longer relevant for Postgres).

#### 1.4 Fix `converter.py`
- Same as downloader — remove raw SQL, use `db.py` methods.
- Remove `self.db.db_path` reference.

#### 1.5 Fix `main.py`
- Remove all direct `self.db.cursor` calls.
- Use `db.get_pipeline_status()` and `db.get_papers_needing_download()` etc.

#### 1.6 Add `src/utils/db_utils.py`
Centralise repeated query patterns (sync PDFs, print status, etc.)

#### 1.7 Update tests
- Mock `psycopg2` instead of SQLite.
- Or use a test Postgres DB / `pytest-postgresql`.

---

### Phase 2 — Section Extraction 📄

#### 2.1 Implement `src/extractor/extractor.py`
Parse TEI XML output from GROBID and extract:
- Title, Abstract
- Introduction, Related Work, Methods, Results, Conclusion
- References

#### 2.2 Add `sections` table to `db.py`
Store extracted sections per paper.

#### 2.3 Update pipeline in `main.py`
Wire up `step_4_extract_sections()`.

---

### Phase 3 — LLM Feature Extraction 🤖

#### 3.1 Create `src/llm/` module
- `client.py` — LLM API wrapper (OpenAI / local)
- `prompts.py` — prompt templates
- `extractor.py` — run prompts over sections

#### 3.2 Define features to extract
Examples: methodology, dataset used, metrics, findings, limitations.

#### 3.3 Add `features` table to `db.py`
Store structured LLM output per paper/section.

#### 3.4 Update `config.py`
Add `LLM_MODEL`, `LLM_API_KEY`, `MAX_TOKENS` etc.

---

### Phase 4 — RAG & Vector Search 🔍

#### 4.1 Enable pgvector
```sql
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE publications ADD COLUMN embedding vector(1536);
CREATE INDEX ON publications USING hnsw (embedding vector_cosine_ops);
```

#### 4.2 Create `src/rag/` module
- `embedder.py` — generate embeddings from sections/features
- `retriever.py` — vector similarity search via pgvector
- `pipeline.py` — end-to-end RAG query handler

#### 4.3 Populate embeddings
Run embedder over all extracted sections and store in DB.

---

### Phase 5 — Quality & Production 🚀

- [ ] Add connection pooling (`psycopg2.pool` or `asyncpg`)
- [ ] Add proper logging (`logging` module, replace `print`)
- [ ] Add `alembic` for DB migrations
- [ ] Docker Compose for Postgres + GROBID + app
- [ ] CI/CD with GitHub Actions
- [ ] Full test coverage with `pytest` + `pytest-postgresql`

---

## Suggested Project Structure (Target)

```
IDRD-Pipeline/
├── src/
│   ├── config.py               ✅ exists — needs completion
│   ├── main.py                 ✅ exists — needs fixes
│   ├── db/
│   │   ├── __init__.py         ⚠️  empty  — needs export
│   │   └── db.py               ✅ migrated to Postgres
│   ├── pubfetcher/
│   │   └── client.py           ✅ renamed
│   ├── extractor/
│   │   ├── downloader.py       ⚠️  needs raw SQL removed
│   │   ├── converter.py        ⚠️  needs raw SQL removed
│   │   ├── extractor.py        ❌  empty
│   │   └── tests.py            ⚠️  needs Postgres mocks
│   ├── llm/                    ❌  not created
│   │   ├── client.py
│   │   ├── prompts.py
│   │   └── extractor.py
│   ├── rag/                    ❌  not created
│   │   ├── embedder.py
│   │   └── retriever.py
│   └── utils/
│       ├── dict_parser.py      ✅ exists
│       └── db_utils.py         ❌  not created — needed now
├── outputs/
│   ├── pdf/
│   ├── xml/
│   └── metadata/
├── .env                        ✅ exists
├── ROADMAP.md                  ✅ this file
└── requirements.txt            ❓  check exists / up to date
```

---

## Immediate Next Steps (Priority Order)

1. `src/db/__init__.py` — export `PublicationDatabase`
2. `src/config.py` — add DB + LLM config
3. `src/utils/db_utils.py` — extract shared helpers
4. Fix `downloader.py` — remove raw SQL
5. Fix `converter.py` — remove raw SQL
6. Fix `main.py` — remove direct cursor access
7. Implement `extractor.py` — section parsing
8. Update tests — Postgres mocks