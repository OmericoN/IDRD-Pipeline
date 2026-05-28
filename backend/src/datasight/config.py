"""Global configuration for DataSight."""

from pathlib import Path
from dotenv import load_dotenv
import os

# Load .env from the monorepo root.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
env_path = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv()

# ── Semantic Scholar ───────────────────────────────────────────────────────────
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "")

# ── OpenAlex ──────────────────────────────────────────────────────────────────
OPENALEX_API_URL = os.getenv("OPENALEX_API_URL", "https://api.openalex.org")
OPENALEX_MAILTO = os.getenv("OPENALEX_MAILTO", "")

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB",   "datasight_pipeline")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

POSTGRES_DSN = (
    f"host={POSTGRES_HOST} "
    f"port={POSTGRES_PORT} "
    f"dbname={POSTGRES_DB} "
    f"user={POSTGRES_USER} "
    f"password={POSTGRES_PASSWORD}"
)

POSTGRES_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ── Queue Runtime (free/open-source: Celery + Redis) ──────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", REDIS_URL)
CELERY_TASK_ALWAYS_EAGER = (
    os.getenv("CELERY_TASK_ALWAYS_EAGER", "false").lower() in {"1", "true", "yes"}
)

# ── Embeddings / pgvector ─────────────────────────────────────────────────────
VECTOR_DIMENSIONS = int(os.getenv("VECTOR_DIMENSIONS", "1536"))

# ── LLM (future extraction provider) ──────────────────────────────────────────
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1")
LLM_API_KEY    = os.getenv("LLM_API_KEY",    "")

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data"
STORAGE_DIR = PROJECT_ROOT / "storage"

# Source/reference inputs live in data/. Generated runtime files live in storage/.
PDF_DIR      = STORAGE_DIR / "pdf"
XML_DIR      = STORAGE_DIR / "xml"
MARKDOWN_DIR = STORAGE_DIR / "markdown"
EXPORTS_DIR  = STORAGE_DIR / "exports"
LOGS_DIR     = STORAGE_DIR / "logs"
RUNS_DIR     = LOGS_DIR / "runs"

# ── Pipeline Settings ──────────────────────────────────────────────────────────

# PDF Downloader
DOWNLOAD_TIMEOUT_SEC = 60          # HTTP request timeout for downloading PDFs
DOWNLOAD_CHUNK_SIZE_BYTES = 8192   # Chunk size for streaming downloads
DOWNLOAD_DELAY_SEC = 0.5           # Delay between downloads to avoid rate limiting
DOWNLOAD_MAX_RETRIES = 3           # Maximum retry attempts for failed downloads

# GROBID Converter  
GROBID_BASE_URL = os.getenv("GROBID_BASE_URL", "http://localhost:8070")
GROBID_STARTUP_TIMEOUT_SEC = 30    # Wait time for GROBID server to start
GROBID_ALIVE_CHECK_TIMEOUT_SEC = 2 # Timeout for /api/isalive endpoint
GROBID_CONVERSION_TIMEOUT_SEC = 300  # Timeout for PDF→XML conversion
GROBID_STARTUP_RETRY_TIMEOUT_SEC = 5  # Timeout when checking if GROBID started
CONVERSION_DELAY_SEC = 0.1         # Delay between conversions

# Renderer
RENDER_TIMEOUT_SEC = 30            # Timeout for markdown rendering operations

# Create generated runtime directories on import
for _dir in (PDF_DIR, XML_DIR, MARKDOWN_DIR, EXPORTS_DIR, RUNS_DIR):
    _dir.mkdir(parents=True, exist_ok=True)
