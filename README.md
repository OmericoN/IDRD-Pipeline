# IDRD Pipeline

IDRD finds hidden dataset references in scholarly publications and matches them against Maastricht University dataset metadata. It is now organized as an API-first backend so a GUI can run, monitor, and reset the pipeline without requiring users to understand the CLI.

## Repository Map

```text
src/idrd/     Python backend package: domain, application, infrastructure, and interfaces
frontend/     Vite React GUI organized into app, feature, shared API, and shared UI packages
migrations/   Alembic migrations for the PostgreSQL/pgvector schema
tests/        Python tests for API, services, storage, CLI, and matching behavior
docs/         Operator, API, architecture, and repository structure guides
data/         Curated source/reference inputs that should survive resets
storage/      Generated runtime files, ignored by git and safe to reset
```

The main backend command is exposed as `idrd` through `pyproject.toml`. `src/main.py` remains as a compatibility entry point for existing scripts.

## What The Backend Does

```text
discover -> download_pdf -> grobid_convert -> render_document -> detect_mentions -> extract_features -> match_um_dataset -> export_insights
```

| Concern | Technology |
|---|---|
| API | FastAPI |
| Background work | Celery + Redis |
| Database | PostgreSQL + pgvector |
| PDF parsing | GROBID |
| Migrations | Alembic |
| Validation | Pydantic |
| Setup | Docker Compose + uv |
| GUI | Vite + React + TypeScript + Tailwind CSS |

## Quick Start

Start the backend stack:

```powershell
docker compose up -d postgres redis grobid
docker compose run --rm migrate
docker compose up api worker
```

Open the API docs:

```text
http://localhost:8000/docs
```

Start the GUI in a second terminal:

```powershell
cd frontend
npm install
npm run dev
```

Open the GUI:

```text
http://localhost:5173
```

The Vite dev server proxies `/api/v1` requests to `http://localhost:8000`, so no FastAPI CORS setup is needed for local development.

Check readiness:

```powershell
curl http://localhost:8000/api/v1/health
```

Start a full queued run:

```powershell
curl -X POST http://localhost:8000/api/v1/runs `
  -H "Content-Type: application/json" `
  -d "{\"query\":\"Maastricht dataset reuse\",\"limit\":25,\"um_datasets_path\":\"data/um_datasets.csv\"}"
```

Poll run status:

```powershell
curl http://localhost:8000/api/v1/runs/1
```

## Storage Layout

Source/reference data stays in `data/`.

Generated runtime files are written to `storage/`:

```text
storage/pdf
storage/xml
storage/markdown
storage/exports
storage/logs
```

The reset endpoint deletes database rows and generated `storage/` files, but it does not delete source code, migrations, `.env`, Docker volumes, or seed/reference files in `data/`.

## Reset

The reset endpoint is intentionally guarded:

```powershell
curl -X POST http://localhost:8000/api/v1/admin/reset `
  -H "Content-Type: application/json" `
  -d "{\"confirm\":\"RESET IDRD\",\"force\":true}"
```

If active runs exist, `force=true` is required.

## CLI

The CLI is still available for operators and development:

```powershell
uv run idrd stages
uv run idrd doctor
uv run idrd import-um-datasets --path data/um_datasets.csv
uv run idrd run-all --query "Maastricht dataset reuse" --limit 25 --um-datasets data/um_datasets.csv --output storage/exports/insights.csv --mode enqueue
```

## Development

Install dependencies and run checks:

```powershell
uv sync
uv run alembic upgrade head
uv run pytest -q
uv run basedpyright
```

More detail:

- [API Guide](docs/API_GUIDE.md)
- [Project Structure](docs/PROJECT_STRUCTURE.md)
- [Architecture Walkthrough](docs/ARCHITECTURE_WALKTHROUGH.md)
- [Setup And CLI Guide](docs/SETUP_AND_CLI_GUIDE.md)
