FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/datasight-venv

WORKDIR /app/backend

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl build-essential \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && apt-get purge -y --auto-remove curl build-essential \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:${PATH}"

COPY backend/pyproject.toml backend/uv.lock backend/README.md ./
COPY backend/src ./src
COPY backend/alembic.ini ./
COPY backend/migrations ./migrations

RUN uv sync --frozen

CMD ["uv", "run", "uvicorn", "datasight.interfaces.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
