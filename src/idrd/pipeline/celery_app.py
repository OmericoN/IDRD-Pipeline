"""Compatibility export for the Celery app."""

from idrd.infrastructure.worker.celery_app import celery_app

__all__ = ["celery_app"]

