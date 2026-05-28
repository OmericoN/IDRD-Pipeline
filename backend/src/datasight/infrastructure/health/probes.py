"""Runtime health probes for external services."""

from __future__ import annotations

import redis
import requests

from datasight.config import CELERY_BROKER_URL, GROBID_ALIVE_CHECK_TIMEOUT_SEC, GROBID_BASE_URL
from datasight.infrastructure.worker.celery_app import celery_app


def redis_ready() -> bool:
    try:
        client = redis.Redis.from_url(CELERY_BROKER_URL, socket_connect_timeout=1, socket_timeout=1)
        return bool(client.ping())
    except Exception:
        return False


def grobid_ready() -> bool:
    try:
        response = requests.get(
            f"{GROBID_BASE_URL}/api/isalive",
            timeout=GROBID_ALIVE_CHECK_TIMEOUT_SEC,
        )
        return response.ok
    except Exception:
        return False


def celery_worker_available(timeout: float = 1.0) -> bool:
    try:
        replies = celery_app.control.ping(timeout=timeout)
        return bool(replies)
    except Exception:
        return False
