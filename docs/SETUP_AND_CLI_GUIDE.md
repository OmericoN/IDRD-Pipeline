# IDRD Pipeline Setup And CLI Guide

This is the current active setup path. Docker Compose starts the infrastructure, Alembic owns the schema, and the CLI runs or enqueues stages against PostgreSQL.

## 1. Start The Stack

From the project root:

```powershell
docker compose up -d postgres redis grobid
docker compose run --rm migrate
docker compose up api worker
```

Useful ports:

```text
Postgres: localhost:5433
Redis:    localhost:6379
GROBID:   localhost:8070
```

Check the app can see the database and queue:

```powershell
docker compose run --rm app uv run idrd doctor
```

For queued execution, start the worker:

```powershell
docker compose up worker
```

The HTTP API is available at:

```text
http://localhost:8000/docs
```

The browser GUI can be started from a second terminal:

```powershell
cd frontend
npm install
npm run dev
```

Open:

```text
http://localhost:5173
```

## 2. Host Development

If you prefer running Python on Windows instead of inside the `app` service:

```powershell
uv sync
uv run alembic upgrade head
uv run idrd doctor
```

Use this `.env` shape for host runs:

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

SEMANTIC_SCHOLAR_API_KEY=
OPENALEX_MAILTO=
LLM_BASE_URL=https://api.groq.com/openai/v1
LLM_API_KEY=
VECTOR_DIMENSIONS=1536
```

Inside Compose, these values point to service names such as `postgres`, `redis`, and `grobid`.

## 3. Import UM Dataset Metadata

Matching needs UM dataset records before `match_um_dataset` runs.

JSON can be either a list or an object with a `datasets` list. CSV should include at least:

```text
um_dataset_id,title
```

Optional CSV columns include:

```text
aliases,creators,doi,url,year,repository,keywords
```

Use semicolons for list-like CSV fields:

```csv
um_dataset_id,title,aliases,creators,year
um-1,Maastricht Health Survey,MHS;Health Survey,Jane Doe;John Doe,2024
```

Import:

```powershell
uv run idrd import-um-datasets --path data/um_datasets.csv
```

Or through Compose:

```powershell
docker compose run --rm app uv run idrd import-um-datasets --path data/um_datasets.csv
```

## 4. Run The Full Pipeline

Guided mode checks database, Redis, and worker readiness. If a worker is available, it prompts for queued mode; otherwise it defaults to local mode in non-interactive shells.

```powershell
uv run idrd run-all `
  --query "Maastricht dataset reuse" `
  --limit 25 `
  --um-datasets data/um_datasets.csv `
  --output storage/exports/insights.csv
```

Force local mode:

```powershell
uv run idrd run-all `
  --query "Maastricht dataset reuse" `
  --limit 25 `
  --um-datasets data/um_datasets.csv `
  --output storage/exports/insights.csv `
  --mode local
```

Force queue mode:

```powershell
uv run idrd run-all `
  --query "Maastricht dataset reuse" `
  --limit 25 `
  --um-datasets data/um_datasets.csv `
  --output storage/exports/insights.csv `
  --mode enqueue
```

## 5. Run Individual Stages

```powershell
uv run idrd stages
uv run idrd run-local discover --query "Maastricht dataset reuse" --limit 5
uv run idrd run-local download_pdf --limit 5
uv run idrd run-local grobid_convert --limit 5
uv run idrd run-local render_document --limit 5
uv run idrd run-local detect_mentions --limit 5
uv run idrd run-local extract_features --limit 5
uv run idrd run-local match_um_dataset --limit 5
uv run idrd run-local export_insights --output storage/exports/insights.csv
```

Queued single-stage examples:

```powershell
uv run idrd worker-command
uv run idrd enqueue discover --query "Maastricht dataset reuse" --limit 5
uv run idrd enqueue download_pdf --limit 5
```

## 6. What Gets Persisted

The active schema stores:

- publications
- artifacts for PDFs, TEI XML, and Markdown
- mention candidates
- extracted dataset mentions
- imported UM datasets
- UM match decisions
- pipeline, stage run, and structured event records

This durable state is the base for the future GUI and later formality evaluation.

## 7. Verification

```powershell
uv run pytest -q
uv run basedpyright
cd frontend
npm run build
npm run test
```
