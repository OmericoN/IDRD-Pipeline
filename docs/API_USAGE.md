# API Usage

DataSight exposes a versioned FastAPI API under `/api/v1`. Interactive OpenAPI documentation is available at `/docs` while the API service is running.

## Run The API

```powershell
docker compose up -d postgres redis grobid
docker compose run --rm migrate
docker compose up api worker
```

Host development:

```powershell
cd backend
uv sync
uv run alembic upgrade head
uv run uvicorn datasight.interfaces.api.main:app --reload --host 0.0.0.0 --port 8000
```

Base URL:

```text
http://localhost:8000/api/v1
```

## Main Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Check database, Redis, worker, GROBID, and migration readiness. |
| `GET` | `/stages` | Return the canonical pipeline stages and descriptions. |
| `POST` | `/runs` | Enqueue a full pipeline run. |
| `POST` | `/stages/{stage}/runs` | Enqueue one stage. |
| `GET` | `/runs` | List recent pipeline runs. |
| `GET` | `/runs/{run_id}` | Read detailed run progress. |
| `GET` | `/runs/{run_id}/events` | Read chronological run events. |
| `GET` | `/insights` | Preview joined insight rows. |
| `POST` | `/um-datasets/import` | Import UM dataset metadata from a local CSV or JSON file. |
| `GET` | `/um-datasets` | Search and paginate stored UM dataset metadata. |
| `GET` | `/um-datasets/verification` | Compare stored metadata with the configured authoritative UM export. |
| `GET` | `/um-datasets/{um_dataset_id}` | Inspect one normalized UM dataset record and its raw metadata. |
| `POST` | `/admin/reset` | Clear database rows and generated storage files. |

## Full Run

```powershell
curl -X POST http://localhost:8000/api/v1/runs `
  -H "Content-Type: application/json" `
  -d "{\"query\":\"Maastricht dataset reuse\",\"limit\":25,\"open_access_only\":true,\"keyword_terms\":[\"biobank\"],\"topic_ids\":[\"T12345\"],\"from_year\":2020,\"to_year\":2026,\"overwrite\":false,\"output_path\":\"storage/exports/insights.csv\"}"
```

Set `"strategy\":\"high_throughput\"` to stream individual publications through downstream stages in parallel. If omitted, runs use the standard sequential strategy.
Discovery uses OpenAlex. Full runs default to `data/um_dataset` with `use_um_profile=true`. The UM profile searches works that cite or link any known UM dataset in batches of 100, then uses topics, keywords, and related works as secondary signals. Set `use_um_profile=false` to opt out. Optional filters include `topic_ids`, `keyword_terms`, `mesh_terms`, `from_year`, and `to_year`.

Response:

```json
{
  "pipeline_run_id": 1,
  "task_id": "celery-task-id",
  "status": "queued"
}
```

Poll progress:

```powershell
curl http://localhost:8000/api/v1/runs/1
curl http://localhost:8000/api/v1/runs/1/events
```

## Single Stage Run

```powershell
curl -X POST http://localhost:8000/api/v1/stages/discover/runs `
  -H "Content-Type: application/json" `
  -d "{\"query\":\"Maastricht dataset reuse\",\"limit\":10}"
```

Valid stages:

```text
discover
download_pdf
grobid_convert
render_document
detect_mentions
extract_features
match_um_dataset
export_insights
```

## Import UM Dataset Metadata

Matching requires UM dataset records before `match_um_dataset` runs. The importer supports either the original compact CSV/JSON shape or the authoritative `data/um_dataset` directory. Directory exports may be comma- or tab-delimited and UTF-8 or Windows-1252. Both `OPEN_ALEX_AFFILIATION.csv` and the older plural filename are accepted. A validated directory import synchronizes the catalog; compact CSV/JSON imports remain additive.

The GUI's **UM datasets** page is read-only. It uses the catalog endpoints to search stored records and compares every normalized field and raw payload against `UM_DATASETS_PATH`. A mismatch reports missing, unexpected, and changed records without modifying storage; use the import endpoint or CLI to repair the catalog.

Compact CSV files should include at least:

```text
um_dataset_id,title
```

Optional list-like fields such as `aliases`, `creators`, and `keywords` use semicolons.

```powershell
curl -X POST http://localhost:8000/api/v1/um-datasets/import `
  -H "Content-Type: application/json" `
  -d "{\"path\":\"data/um_dataset\"}"
```

PURE export directory example:

```powershell
curl -X POST http://localhost:8000/api/v1/um-datasets/import `
  -H "Content-Type: application/json" `
  -d "{\"path\":\"data/um_dataset\"}"
```

## Reset

Reset is intentionally guarded and requires the exact confirmation string:

```powershell
curl -X POST http://localhost:8000/api/v1/admin/reset `
  -H "Content-Type: application/json" `
  -d "{\"confirm\":\"RESET DATASIGHT\",\"force\":true}"
```

`force=true` is required when active runs exist. Reset removes generated rows and configured `storage/` artifacts, but it does not delete source code, migrations, `.env`, Docker volumes, or curated inputs under `data/`.

## GUI Polling Flow

The frontend uses the same API:

1. `GET /health` to show readiness.
2. `POST /runs` to start work.
3. `GET /runs/{run_id}` for stage status.
4. `GET /runs/{run_id}/events` for structured logs.
5. `GET /insights` to preview results after matching/export.

The Vite dev server proxies `/api/v1` to `http://localhost:8000`, so local GUI development does not require FastAPI CORS changes.
