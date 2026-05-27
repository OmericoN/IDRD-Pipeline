# IDRD Backend

FastAPI, Celery, Alembic, and pipeline services for the IDRD monorepo.

Run local backend commands from this directory:

```powershell
uv sync
uv run alembic upgrade head
uv run pytest -q
uv run basedpyright
```
