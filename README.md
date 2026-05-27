# IDRD Pipeline

IDRD finds hidden dataset references in scholarly publications and matches them against Maastricht University dataset metadata. The point is to recover dataset impact that normal publication citation metadata misses, especially informal narrative mentions in full text.

The active pipeline is:

```text
discover -> download_pdf -> grobid_convert -> render_document -> detect_mentions -> extract_features -> match_um_dataset -> export_insights
```

## Stack

| Concern | Technology |
|---|---|
| Language | Python 3.12 |
| Database | PostgreSQL + pgvector |
| Queue | Celery + Redis |
| PDF parsing | GROBID |
| Migrations | Alembic |
| Validation | Pydantic |
| Setup | Docker Compose + uv |

The canonical backend is `PipelineRepository` over the Alembic schema. Legacy local database code and experimental extraction scripts have been removed from the active package.

## Fast Setup

Start the local stack:

```bash
docker compose up -d postgres redis grobid
docker compose run --rm migrate
```

Check readiness:

```bash
docker compose run --rm app uv run src/main.py doctor
```

Start a worker for queued runs:

```bash
docker compose up worker
```

For host-based development, install dependencies with:

```bash
uv sync
uv run alembic upgrade head
```

Host `.env` defaults should point to local services:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=idrd_pipeline
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

REDIS_URL=redis://localhost:6379/0
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
GROBID_BASE_URL=http://localhost:8070
```

## CLI

List stages:

```bash
uv run src/main.py stages
```

Import UM dataset metadata from JSON or CSV:

```bash
uv run src/main.py import-um-datasets --path data/um_datasets.csv
```

Run the full pipeline with guidance:

```bash
uv run src/main.py run-all ^
  --query "Maastricht dataset reuse" ^
  --limit 25 ^
  --um-datasets data/um_datasets.csv ^
  --output outputs/insights.csv
```

Use explicit local or queued mode:

```bash
uv run src/main.py run-all --query "Maastricht dataset reuse" --limit 25 --um-datasets data/um_datasets.csv --output outputs/insights.csv --mode local
uv run src/main.py run-all --query "Maastricht dataset reuse" --limit 25 --um-datasets data/um_datasets.csv --output outputs/insights.csv --mode enqueue
```

Run one stage locally:

```bash
uv run src/main.py run-local detect_mentions --limit 20
```

Enqueue one stage:

```bash
uv run src/main.py enqueue download_pdf --limit 20
```

## Data Flow

- `discover` stores publication metadata in `publications`.
- `download_pdf`, `grobid_convert`, and `render_document` store file artifacts in `artifacts`.
- `detect_mentions` stores high-recall mention windows in `mention_candidates`.
- `extract_features` promotes unprocessed candidates into validated `dataset_mentions`.
- `import-um-datasets` stores UM metadata in `um_datasets`.
- `match_um_dataset` stores deterministic match decisions in `um_match_decisions`.
- `export_insights` writes a CSV joined across publications, mentions, UM datasets, and match decisions.

The v1 extraction path is intentionally rule-based and auditable. LLM extraction and formality evaluation remain future layers on top of the persisted mention/evidence model.

## Tests

```bash
uv run pytest -q
uv run basedpyright
```

The current suite covers CLI parsing, mention detection, UM matching, UM metadata loading, and repository SQL behavior.
