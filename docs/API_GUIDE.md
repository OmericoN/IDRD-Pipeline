# IDRD API Guide

The FastAPI backend exposes versioned routes under `/api/v1`. OpenAPI documentation is available at `/docs` when the API service is running.

## Run The API

```powershell
docker compose up -d postgres redis grobid
docker compose run --rm migrate
docker compose up api worker
```

Host development:

```powershell
uv run uvicorn idrd.api.main:app --reload --host 0.0.0.0 --port 8000
```

## GUI Polling Flow

1. Call `GET /api/v1/health` and show readiness checks.
2. Call `POST /api/v1/runs` with the user query and run options.
3. Store the returned `pipeline_run_id`.
4. Poll `GET /api/v1/runs/{pipeline_run_id}` every few seconds.
5. Poll `GET /api/v1/runs/{pipeline_run_id}/events` for structured stage messages and errors.
6. Show stage progress from the `stages` array.
7. Read preview rows from `GET /api/v1/insights` after matching/export.

## Run The GUI

The GUI lives in `frontend/` and uses Vite's local proxy to reach the API.

```powershell
cd frontend
npm install
npm run dev
```

Open:

```text
http://localhost:5173
```

## Main Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/health` | Database, Redis, worker, GROBID, and migration readiness |
| `GET` | `/api/v1/stages` | Canonical pipeline stages and descriptions |
| `POST` | `/api/v1/runs` | Enqueue the full pipeline |
| `POST` | `/api/v1/stages/{stage}/runs` | Enqueue one stage |
| `GET` | `/api/v1/runs` | List recent pipeline runs |
| `GET` | `/api/v1/runs/{run_id}` | Get detailed run progress |
| `GET` | `/api/v1/runs/{run_id}/events` | Get chronological structured run events |
| `GET` | `/api/v1/insights` | Preview joined insight rows |
| `POST` | `/api/v1/um-datasets/import` | Import UM dataset CSV/JSON from disk |
| `POST` | `/api/v1/admin/reset` | Wipe database rows and generated storage |

## Full Run Example

```json
POST /api/v1/runs
{
  "query": "Maastricht dataset reuse",
  "limit": 25,
  "open_access_only": true,
  "overwrite": false,
  "um_datasets_path": "data/um_datasets.csv",
  "output_path": "storage/exports/insights.csv"
}
```

Response:

```json
{
  "pipeline_run_id": 1,
  "task_id": "celery-task-id",
  "status": "queued"
}
```

## Reset

Reset requires an exact confirmation string:

```json
POST /api/v1/admin/reset
{
  "confirm": "RESET IDRD",
  "force": true
}
```

The reset keeps schema migrations and source/reference files, then recreates empty generated storage directories.
