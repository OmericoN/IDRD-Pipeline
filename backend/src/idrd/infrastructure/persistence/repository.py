"""PostgreSQL repository composed from focused persistence concerns."""

from __future__ import annotations

from idrd.infrastructure.persistence.artifacts import ArtifactRepositoryMixin
from idrd.infrastructure.persistence.base import PipelineRepositoryBase
from idrd.infrastructure.persistence.insights import InsightRepositoryMixin
from idrd.infrastructure.persistence.mentions import MentionRepositoryMixin
from idrd.infrastructure.persistence.publications import PublicationRepositoryMixin
from idrd.infrastructure.persistence.runs import RunRepositoryMixin
from idrd.infrastructure.persistence.um_datasets import UMDatasetRepositoryMixin


class PipelineRepository(
    RunRepositoryMixin,
    PublicationRepositoryMixin,
    ArtifactRepositoryMixin,
    MentionRepositoryMixin,
    UMDatasetRepositoryMixin,
    InsightRepositoryMixin,
    PipelineRepositoryBase,
):
    """PostgreSQL gateway for pipeline state and research artifacts."""
