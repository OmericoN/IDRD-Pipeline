"""PostgreSQL repository composed from focused persistence concerns."""

from __future__ import annotations

from datasight.infrastructure.persistence.artifacts import ArtifactRepositoryMixin
from datasight.infrastructure.persistence.base import PipelineRepositoryBase
from datasight.infrastructure.persistence.insights import InsightRepositoryMixin
from datasight.infrastructure.persistence.items import PipelineItemRepositoryMixin
from datasight.infrastructure.persistence.mentions import MentionRepositoryMixin
from datasight.infrastructure.persistence.publications import PublicationRepositoryMixin
from datasight.infrastructure.persistence.runs import RunRepositoryMixin
from datasight.infrastructure.persistence.um_datasets import UMDatasetRepositoryMixin


class PipelineRepository(
    RunRepositoryMixin,
    PipelineItemRepositoryMixin,
    PublicationRepositoryMixin,
    ArtifactRepositoryMixin,
    MentionRepositoryMixin,
    UMDatasetRepositoryMixin,
    InsightRepositoryMixin,
    PipelineRepositoryBase,
):
    """PostgreSQL gateway for pipeline state and research artifacts."""
