"""Canonical stage names for the queue-backed DataSight pipeline."""

from __future__ import annotations

from enum import StrEnum


class PipelineStage(StrEnum):
    DISCOVER = "discover"
    DOWNLOAD_PDF = "download_pdf"
    GROBID_CONVERT = "grobid_convert"
    RENDER_DOCUMENT = "render_document"
    DETECT_MENTIONS = "detect_mentions"
    EXTRACT_FEATURES = "extract_features"
    MATCH_UM_DATASET = "match_um_dataset"
    EXPORT_INSIGHTS = "export_insights"


STAGE_ORDER: tuple[PipelineStage, ...] = (
    PipelineStage.DISCOVER,
    PipelineStage.DOWNLOAD_PDF,
    PipelineStage.GROBID_CONVERT,
    PipelineStage.RENDER_DOCUMENT,
    PipelineStage.DETECT_MENTIONS,
    PipelineStage.EXTRACT_FEATURES,
    PipelineStage.MATCH_UM_DATASET,
    PipelineStage.EXPORT_INSIGHTS,
)

# Stages currently executed by automatic standard and high-throughput runs.
# Matching and export remain registered for direct API/CLI development use.
WORKFLOW_STAGE_ORDER: tuple[PipelineStage, ...] = (
    PipelineStage.DISCOVER,
    PipelineStage.DOWNLOAD_PDF,
    PipelineStage.GROBID_CONVERT,
    PipelineStage.RENDER_DOCUMENT,
    PipelineStage.DETECT_MENTIONS,
    PipelineStage.EXTRACT_FEATURES,
)

ITEM_STAGE_ORDER: tuple[PipelineStage, ...] = WORKFLOW_STAGE_ORDER[1:]


def stage_values() -> list[str]:
    return [stage.value for stage in STAGE_ORDER]
