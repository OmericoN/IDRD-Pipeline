"""Stage service functions used by Celery tasks and local CLI runs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Literal

from idrd.config import MARKDOWN_DIR, PDF_DIR, XML_DIR
from idrd.infrastructure.ingestion.converter import GrobidConverter
from idrd.infrastructure.ingestion.downloader import PDFDownloader
from idrd.infrastructure.ingestion.renderer import render_papers
from idrd.infrastructure.persistence.repository import PipelineRepository
from idrd.infrastructure.pubfetcher.semantic_scholar import SemanticScholarClient
from idrd.matching.um_matcher import match_mention_to_um_dataset
from idrd.pipeline.candidate_detection import detect_dataset_candidates
from idrd.domain.schemas import DatasetMention, ExtractionProvenance, MentionEvidence, StageResult, UMDatasetRecord
from idrd.domain.stages import PipelineStage


def discover_publications(
    query: str,
    limit: int = 100,
    open_access_only: bool = True,
    fields_of_study: str | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    client = SemanticScholarClient()
    papers = client.search_papers(
        query=query,
        limit=limit,
        open_access_pdf=open_access_only,
        fields_of_study=fields_of_study,
    )
    with PipelineRepository() as repo:
        inserted = repo.upsert_publications(papers, source="semantic_scholar")
        result = _stage_result(
            PipelineStage.DISCOVER,
            "successful" if papers else "skipped",
            inserted,
            f"Discovered {len(papers)} publications.",
            {"query": query, "fetched": len(papers)},
        )
        repo.record_stage_result(PipelineStage.DISCOVER, result["status"], result, pipeline_run_id)
        return result


def download_pdf_batch(
    limit: int | None = None,
    overwrite: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        downloader = PDFDownloader(output_dir=PDF_DIR)
        papers = repo.get_papers_needing_download(limit=limit)
        results = downloader.download_papers(papers, overwrite=overwrite)
        repo.persist_download_results(results)
        result = _result_from_operation(PipelineStage.DOWNLOAD_PDF, results)
        repo.record_stage_result(PipelineStage.DOWNLOAD_PDF, result["status"], result, pipeline_run_id)
        return result


def grobid_convert_batch(
    limit: int | None = None,
    overwrite: bool = False,
    delete_pdf: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        converter = GrobidConverter(output_dir=XML_DIR)
        papers = repo.get_papers_needing_conversion(limit=limit)
        results = converter.convert_papers(papers, overwrite=overwrite, delete_pdf=delete_pdf)
        repo.persist_conversion_results(results)
        result = _result_from_operation(PipelineStage.GROBID_CONVERT, results)
        repo.record_stage_result(PipelineStage.GROBID_CONVERT, result["status"], result, pipeline_run_id)
        return result


def render_document_batch(
    limit: int | None = None,
    overwrite: bool = False,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        papers = repo.get_papers_needing_rendering(limit=limit)
        results = render_papers(papers, output_dir=MARKDOWN_DIR, overwrite=overwrite)
        repo.persist_render_results(results)
        result = _result_from_operation(PipelineStage.RENDER_DOCUMENT, results)
        repo.record_stage_result(PipelineStage.RENDER_DOCUMENT, result["status"], result, pipeline_run_id)
        return result


def detect_mentions_batch(
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        artifacts = repo.get_markdown_artifacts_without_candidates(limit=limit)
        candidates = []
        for artifact in artifacts:
            path = Path(artifact["path"])
            markdown = path.read_text(encoding="utf-8")
            candidates.extend(detect_dataset_candidates(artifact["paper_id"], markdown))
        inserted = repo.upsert_mention_candidates(candidates)
        result = _stage_result(
            PipelineStage.DETECT_MENTIONS,
            "successful" if inserted else "skipped",
            inserted,
            f"Detected and persisted {inserted} candidate dataset mentions.",
            {"candidates": inserted, "documents": len(artifacts)},
        )
        repo.record_stage_result(PipelineStage.DETECT_MENTIONS, result["status"], result, pipeline_run_id)
        return result


def extract_features_from_candidates(
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        rows = repo.get_unprocessed_candidates(limit=limit)
        mentions: list[DatasetMention] = []
        candidate_ids: list[int] = []
        for candidate in rows:
            evidence = MentionEvidence(
                body_quote=candidate["evidence_text"],
                section_heading=candidate.get("section_heading"),
                standardized_section=candidate.get("standardized_section"),
            )
            mention = DatasetMention(
                publication_id=candidate["publication_id"],
                dataset_name=candidate["dataset_name"],
                evidence=evidence,
                provenance=ExtractionProvenance(
                    char_start=candidate.get("char_start"),
                    char_end=candidate.get("char_end"),
                    confidence=candidate.get("score", 0.0),
                    prompt_version="rules-v1",
                ),
            )
            mentions.append(mention)
            candidate_ids.append(candidate["candidate_id"])
        inserted = repo.persist_dataset_mentions(mentions, candidate_ids)
        result = _stage_result(
            PipelineStage.EXTRACT_FEATURES,
            "successful" if inserted else "skipped",
            inserted,
            f"Promoted {inserted} candidates into v1 mention records.",
            {"mentions": inserted},
        )
        repo.record_stage_result(PipelineStage.EXTRACT_FEATURES, result["status"], result, pipeline_run_id)
        return result


def import_um_datasets(path: str) -> dict[str, Any]:
    records = load_um_dataset_records(path)
    with PipelineRepository() as repo:
        count = repo.upsert_um_datasets(records)
    return {"status": "successful" if count else "skipped", "count": count, "path": path}


def match_um_dataset_batch(
    limit: int | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        records = repo.list_um_dataset_records()
        if not records:
            raise ValueError("No UM datasets are imported. Run import-um-datasets first.")

        mention_rows = repo.get_unmatched_mentions(limit=limit)
        decisions = []
        for row in mention_rows:
            mention = DatasetMention.model_validate(
                {
                    "publication_id": row["publication_id"],
                    "dataset_name": row["dataset_name"],
                    "aliases": row["aliases"] or [],
                    "dataset_role": row["dataset_role"],
                    "reference_directness": row["reference_directness"],
                    "evidence": row["evidence"],
                    "metadata": row["metadata"],
                    "provenance": row["provenance"],
                }
            )
            decision = match_mention_to_um_dataset(mention, records)
            decision.mention_id = str(row["mention_id"])
            decisions.append(decision)

        inserted = repo.persist_match_decisions(decisions)
        result = _stage_result(
            PipelineStage.MATCH_UM_DATASET,
            "successful" if inserted else "skipped",
            inserted,
            f"Matched {inserted} mentions against UM metadata candidates.",
            {"decisions": inserted},
        )
        repo.record_stage_result(PipelineStage.MATCH_UM_DATASET, result["status"], result, pipeline_run_id)
        return result


def export_insights_csv(
    output_path: str,
    rows: list[dict[str, Any]] | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        export_rows = rows if rows is not None else repo.export_insight_rows()
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = sorted({key for row in export_rows for key in row.keys()})
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([_json_safe(row) for row in export_rows])
        result = _stage_result(
            PipelineStage.EXPORT_INSIGHTS,
            "successful",
            len(export_rows),
            f"Exported insights to {path}.",
            {"output_path": str(path)},
        )
        repo.record_stage_result(PipelineStage.EXPORT_INSIGHTS, result["status"], result, pipeline_run_id)
        return result


def load_um_dataset_records(path: str) -> list[UMDatasetRecord]:
    source = Path(path)
    if source.suffix.lower() == ".csv":
        with source.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        return [UMDatasetRecord.model_validate(_coerce_csv_record(row)) for row in rows]

    data = json.loads(source.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("datasets", [data])
    return [UMDatasetRecord.model_validate(item) for item in data]


def _result_from_operation(stage: PipelineStage, results: list[Any]) -> dict[str, Any]:
    successful = sum(1 for result in results if result.success)
    failed = sum(1 for result in results if not result.success)
    return _stage_result(
        stage,
        "successful" if successful else ("failed" if failed else "skipped"),
        len(results),
        f"{stage.value}: {successful} successful, {failed} failed.",
        {
            "successful": successful,
            "failed": failed,
            "results": [_json_safe(vars(result)) for result in results],
        },
    )


def _stage_result(
    stage: PipelineStage,
    status: Literal["successful", "failed", "skipped"],
    count: int,
    message: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return StageResult(
        stage=stage,
        status=status,
        count=count,
        message=message,
        payload=payload or {},
    ).model_dump(mode="json")


def _coerce_csv_record(row: dict[str, str]) -> dict[str, Any]:
    coerced: dict[str, Any] = dict(row)
    for list_key in ("aliases", "creators", "keywords"):
        value = coerced.get(list_key)
        if isinstance(value, str):
            coerced[list_key] = [part.strip() for part in value.split(";") if part.strip()]
    if coerced.get("year"):
        coerced["year"] = int(coerced["year"])
    coerced["raw"] = dict(row)
    return coerced


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
