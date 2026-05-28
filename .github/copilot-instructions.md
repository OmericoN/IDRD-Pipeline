# Copilot Instructions for DataSight

## Build, test, and lint commands

- Install/sync backend dependencies from `backend/`: `uv sync`
- Run backend tests from `backend/`: `uv run pytest -q`
- Run type checking from `backend/`: `uv run basedpyright`
- Run migrations from `backend/`: `uv run alembic upgrade head`
- Install frontend dependencies from `frontend/`: `bun install`
- Run frontend checks from `frontend/`: `bun run test` and `bun run build`
- Validate Compose changes from the repository root: `docker compose config`

There is no dedicated formatter configured in this repo.

## Architecture

- The active Python package is `datasight`.
- The CLI command is `uv run datasight`.
- The FastAPI app is `datasight.interfaces.api.main:app`.
- The Celery app is `datasight.infrastructure.worker.celery_app:celery_app`.
- Pipeline state is persisted through `datasight.infrastructure.persistence.PipelineRepository`.

Canonical stage order:

```text
discover -> download_pdf -> grobid_convert -> render_document -> detect_mentions -> extract_features -> match_um_dataset -> export_insights
```

## Conventions

- Interfaces validate input and call application use cases.
- Application services coordinate stage behavior and persist outcomes through repositories.
- Infrastructure owns SQL, queues, external HTTP clients, GROBID, health checks, and generated files.
- Domain modules hold shared schemas, stage names, and result objects.
- Generated artifacts belong under `storage/`; curated source/reference inputs belong under `data/`.
- Do not add compatibility imports or docs for old package names.
