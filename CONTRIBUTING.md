# Contributing

This repository uses a src-layout Python backend plus a separate Vite frontend. Keep changes within the existing ownership boundaries unless the feature truly crosses them.

## Local Setup

```powershell
uv sync
Copy-Item .env.example .env
docker compose up -d postgres redis grobid
uv run alembic upgrade head
```

For frontend work:

```powershell
cd frontend
npm install
```

## Checks

Run the backend checks before opening a change:

```powershell
uv run pytest -q
uv run basedpyright
```

Run frontend checks when touching `frontend/`:

```powershell
cd frontend
npm run test
npm run build
```

## Boundaries

- `idrd.api` should stay thin: validate HTTP input, call services/orchestrators, and return schemas.
- `idrd.pipeline` owns pipeline stages, Celery tasks, orchestration, and service-level behavior.
- `idrd.storage` owns database access and reset behavior.
- `idrd.ingestion`, `idrd.pubfetcher`, and `idrd.matching` own external data acquisition and domain-specific transforms.
- `frontend/src/lib` owns API/data helpers; reusable UI primitives live in `frontend/src/components/ui`.

## Data And Generated Files

Commit only stable source/reference inputs under `data/`. Generated runtime files belong under `storage/` or `outputs/` and should stay out of git. Keep local secrets in `.env`; update `.env.example` when adding a new setting.
