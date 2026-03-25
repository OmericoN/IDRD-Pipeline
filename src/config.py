"""
Global configuration for IDRD Pipeline.
Single source of truth for all settings.
"""
from pathlib import Path
from dotenv import load_dotenv
import os

# Load .env from project root
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
load_dotenv()

# ── Semantic Scholar ───────────────────────────────────────────────────────────
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "")

# ── PostgreSQL ─────────────────────────────────────────────────────────────────
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB",   "idrd_pipeline")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

POSTGRES_DSN = (
    f"host={POSTGRES_HOST} "
    f"port={POSTGRES_PORT} "
    f"dbname={POSTGRES_DB} "
    f"user={POSTGRES_USER} "
    f"password={POSTGRES_PASSWORD}"
)

# ── LLM (Phase 3) ──────────────────────────────────────────────────────────────
LLM_BASE_URL = "https://api.groq.com/openai/v1"
LLM_API_KEY    = os.getenv("LLM_API_KEY",    "")

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
BASE_DIR = PROJECT_ROOT / "data"

# Pipeline DATA — produced and consumed by pipeline steps
DATA_DIR     = BASE_DIR
PDF_DIR      = DATA_DIR / "pdf"
XML_DIR      = DATA_DIR / "xml"
MARKDOWN_DIR = DATA_DIR / "markdown"

# Pipeline LOGS — observational only, never read by the pipeline
LOGS_DIR = PROJECT_ROOT / "logs"
RUNS_DIR = LOGS_DIR / "runs"

# ── Pipeline Settings ──────────────────────────────────────────────────────────

# PDF Downloader
DOWNLOAD_TIMEOUT_SEC = 60          # HTTP request timeout for downloading PDFs
DOWNLOAD_CHUNK_SIZE_BYTES = 8192   # Chunk size for streaming downloads
DOWNLOAD_DELAY_SEC = 0.5           # Delay between downloads to avoid rate limiting
DOWNLOAD_MAX_RETRIES = 3           # Maximum retry attempts for failed downloads

# GROBID Converter  
GROBID_STARTUP_TIMEOUT_SEC = 30    # Wait time for GROBID server to start
GROBID_ALIVE_CHECK_TIMEOUT_SEC = 2 # Timeout for /api/isalive endpoint
GROBID_CONVERSION_TIMEOUT_SEC = 300  # Timeout for PDF→XML conversion
GROBID_STARTUP_RETRY_TIMEOUT_SEC = 5  # Timeout when checking if GROBID started
CONVERSION_DELAY_SEC = 0.1         # Delay between conversions

# Renderer
RENDER_TIMEOUT_SEC = 30            # Timeout for markdown rendering operations

# Create directories on import
for _dir in (PDF_DIR, XML_DIR, MARKDOWN_DIR, RUNS_DIR):
    _dir.mkdir(parents=True, exist_ok=True)