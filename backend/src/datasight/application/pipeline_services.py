"""Stage service functions used by Celery tasks and local CLI runs."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Literal

from datasight.config import MARKDOWN_DIR, PDF_DIR, PROJECT_ROOT, XML_DIR
from datasight.domain.discovery import (
    DiscoveryOptions,
    build_discovery_profile,
    build_openalex_discovery_queries,
    dedupe_and_score_publications,
    parse_terms,
)
from datasight.infrastructure.ingestion.converter import GrobidConverter
from datasight.infrastructure.ingestion.downloader import PDFDownloader
from datasight.infrastructure.ingestion.openalex_exports import (
    load_um_openalex_export_bundle,
    load_um_openalex_exports,
    looks_like_openalex_export,
)
from datasight.infrastructure.ingestion.renderer import render_papers, render_to_markdown
from datasight.infrastructure.persistence.repository import PipelineRepository
from datasight.infrastructure.pubfetcher.openalex import OpenAlexClient
from datasight.matching.um_matcher import match_mention_to_um_dataset
from datasight.domain.candidate_detection import detect_dataset_candidates
from datasight.domain.schemas import (
    DatasetMention,
    DatasetMetadata,
    DatasetRole,
    ExtractionProvenance,
    MentionEvidence,
    ReferenceDirectness,
    StageResult,
    UMDatasetRecord,
)
from datasight.domain.stages import PipelineStage


def discover_publications(
    query: str,
    limit: int = 100,
    open_access_only: bool = True,
    topic_ids: str | list[str] | None = None,
    keyword_terms: str | list[str] | None = None,
    mesh_terms: str | list[str] | None = None,
    from_year: int | None = None,
    to_year: int | None = None,
    use_um_profile: bool = True,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    options = DiscoveryOptions(
        topic_ids=parse_terms(topic_ids),
        keyword_terms=parse_terms(keyword_terms),
        mesh_terms=parse_terms(mesh_terms),
        from_year=from_year,
        to_year=to_year,
        use_um_profile=use_um_profile,
    )
    client = OpenAlexClient()
    with PipelineRepository() as repo:
        profile = build_discovery_profile(repo.list_um_dataset_records()) if use_um_profile else None
        discovery_queries = build_openalex_discovery_queries(
            query=query,
            options=options,
            profile=profile,
            open_access_only=open_access_only,
        )
        fetched: list[dict[str, Any]] = []
        query_payloads = []
        for discovery_query in discovery_queries:
            per_query_limit = (
                100
                if discovery_query.reason in {"um_dataset_citation", "um_dataset_link"}
                else min(100, max(10, limit))
            )
            papers = client.search_works(
                query=discovery_query.query,
                limit=per_query_limit,
                filters=discovery_query.filters,
                sort=discovery_query.sort,
            )
            for paper in papers:
                reasons = list(paper.get("discovery_reasons") or [])
                if discovery_query.reason not in reasons:
                    reasons.append(discovery_query.reason)
                paper["discovery_reasons"] = reasons
            fetched.extend(papers)
            query_payloads.append(
                {
                    "reason": discovery_query.reason,
                    "query": discovery_query.query,
                    "filters": discovery_query.filters,
                    "seed_count": len(discovery_query.seed_work_ids),
                    "fetched": len(papers),
                }
            )
        papers = dedupe_and_score_publications(fetched, query, options, profile)[:limit]
        inserted = repo.upsert_publications(papers, source="openalex")
        result = _stage_result(
            PipelineStage.DISCOVER,
            "successful" if papers else "skipped",
            inserted,
            f"Discovered {len(papers)} publications.",
            {
                "query": query,
                "provider": "openalex",
                "fetched": len(papers),
                "openalex_queries": query_payloads,
                "use_um_profile": use_um_profile,
                "paper_ids": [
                    paper.get("paperId") or paper.get("id")
                    for paper in papers
                    if paper.get("paperId") or paper.get("id")
                ],
            },
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
            mentions.append(_mention_from_candidate(candidate))
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
    source = _project_path(path)
    authoritative = looks_like_openalex_export(source)
    if authoritative:
        bundle = load_um_openalex_export_bundle(source)
        records = bundle.records
        warnings = bundle.warnings
        metrics = bundle.metrics
    else:
        records = load_um_dataset_records(path)
        warnings = []
        metrics = {"source_rows": len(records)}
    if not records:
        raise ValueError(f"No UM dataset records were found in {source}.")
    with PipelineRepository() as repo:
        if authoritative:
            count, deleted = repo.sync_um_datasets(records)
        else:
            count = repo.upsert_um_datasets(records)
            deleted = 0
    return {
        "status": "successful" if count else "skipped",
        "count": count,
        "deleted": deleted,
        "path": path,
        "warnings": warnings,
        "metrics": metrics,
    }


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


def download_pipeline_item(
    item_id: int,
    overwrite: bool = False,
    task_id: str | None = None,
    claimed: bool = False,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        if not claimed and not repo.start_item_stage(item_id, PipelineStage.DOWNLOAD_PDF, task_id):
            return _item_result(PipelineStage.DOWNLOAD_PDF, "skipped", item_id, "Item stage was already terminal.")
        context = repo.get_pipeline_item_context(item_id)
        if not context:
            return _missing_item_result(repo, item_id, PipelineStage.DOWNLOAD_PDF)
        downloader = PDFDownloader(output_dir=PDF_DIR, delay=0)
        result = downloader.download_paper(
            paper_id=context["paper_id"],
            url=context.get("open_access_url"),
            title=context.get("title"),
            overwrite=overwrite,
        )
        repo.persist_download_results([result])
        status = "successful" if result.success else "failed"
        metrics = _json_safe(vars(result))
        repo.finish_item_stage(item_id, PipelineStage.DOWNLOAD_PDF, status, metrics, result.error)
        return _item_result(PipelineStage.DOWNLOAD_PDF, status, item_id, result.message, metrics)


def grobid_convert_pipeline_item(
    item_id: int,
    overwrite: bool = False,
    delete_pdf: bool = False,
    task_id: str | None = None,
    claimed: bool = False,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        if not claimed and not repo.start_item_stage(item_id, PipelineStage.GROBID_CONVERT, task_id):
            return _item_result(PipelineStage.GROBID_CONVERT, "skipped", item_id, "Item stage was already terminal.")
        context = repo.get_pipeline_item_context(item_id)
        if not context:
            return _missing_item_result(repo, item_id, PipelineStage.GROBID_CONVERT)
        if not context.get("pdf_path"):
            message = "PDF artifact is missing."
            repo.finish_item_stage(item_id, PipelineStage.GROBID_CONVERT, "failed", {"message": message}, message)
            return _item_result(PipelineStage.GROBID_CONVERT, "failed", item_id, message)
        try:
            converter = GrobidConverter(output_dir=XML_DIR, delay=0)
            converter.ensure_available()
            result = converter.convert_pdf(
                pdf_path=Path(context["pdf_path"]),
                paper_id=context["paper_id"],
                overwrite=overwrite,
                delete_pdf=delete_pdf,
            )
        except Exception as exc:
            message = f"GROBID conversion error: {exc}"
            repo.finish_item_stage(item_id, PipelineStage.GROBID_CONVERT, "failed", {"message": message}, str(exc))
            return _item_result(PipelineStage.GROBID_CONVERT, "failed", item_id, message)
        repo.persist_conversion_results([result])
        status = "successful" if result.success else "failed"
        metrics = _json_safe(vars(result))
        repo.finish_item_stage(item_id, PipelineStage.GROBID_CONVERT, status, metrics, result.error)
        return _item_result(PipelineStage.GROBID_CONVERT, status, item_id, result.message, metrics)


def render_pipeline_item(
    item_id: int,
    overwrite: bool = False,
    task_id: str | None = None,
    claimed: bool = False,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        if not claimed and not repo.start_item_stage(item_id, PipelineStage.RENDER_DOCUMENT, task_id):
            return _item_result(PipelineStage.RENDER_DOCUMENT, "skipped", item_id, "Item stage was already terminal.")
        context = repo.get_pipeline_item_context(item_id)
        if not context:
            return _missing_item_result(repo, item_id, PipelineStage.RENDER_DOCUMENT)
        if not context.get("xml_path"):
            message = "TEI XML artifact is missing."
            repo.finish_item_stage(item_id, PipelineStage.RENDER_DOCUMENT, "failed", {"message": message}, message)
            return _item_result(PipelineStage.RENDER_DOCUMENT, "failed", item_id, message)
        output_path = MARKDOWN_DIR / f"{context['paper_id']}.md"
        try:
            result = render_to_markdown(
                Path(context["xml_path"]),
                output_path=output_path,
                paper_id=context["paper_id"],
                overwrite=overwrite,
            )
        except Exception as exc:
            message = f"Render error: {exc}"
            repo.finish_item_stage(item_id, PipelineStage.RENDER_DOCUMENT, "failed", {"message": message}, str(exc))
            return _item_result(PipelineStage.RENDER_DOCUMENT, "failed", item_id, message)
        repo.persist_render_results([result])
        status = "successful" if result.success else "failed"
        metrics = _json_safe(vars(result))
        repo.finish_item_stage(item_id, PipelineStage.RENDER_DOCUMENT, status, metrics, result.error)
        return _item_result(PipelineStage.RENDER_DOCUMENT, status, item_id, result.message, metrics)


def detect_mentions_pipeline_item(
    item_id: int,
    task_id: str | None = None,
    claimed: bool = False,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        if not claimed and not repo.start_item_stage(item_id, PipelineStage.DETECT_MENTIONS, task_id):
            return _item_result(PipelineStage.DETECT_MENTIONS, "skipped", item_id, "Item stage was already terminal.")
        context = repo.get_pipeline_item_context(item_id)
        if not context:
            return _missing_item_result(repo, item_id, PipelineStage.DETECT_MENTIONS)
        if not context.get("markdown_path"):
            message = "Markdown artifact is missing."
            repo.finish_item_stage(item_id, PipelineStage.DETECT_MENTIONS, "failed", {"message": message}, message)
            return _item_result(PipelineStage.DETECT_MENTIONS, "failed", item_id, message)
        try:
            markdown = Path(context["markdown_path"]).read_text(encoding="utf-8")
        except OSError as exc:
            message = f"Markdown read error: {exc}"
            repo.finish_item_stage(item_id, PipelineStage.DETECT_MENTIONS, "failed", {"message": message}, str(exc))
            return _item_result(PipelineStage.DETECT_MENTIONS, "failed", item_id, message)
        candidates = detect_dataset_candidates(context["paper_id"], markdown)
        inserted = repo.upsert_mention_candidates(candidates)
        metrics = {"candidates": inserted, "paper_id": context["paper_id"]}
        repo.finish_item_stage(item_id, PipelineStage.DETECT_MENTIONS, "successful", metrics)
        return _item_result(
            PipelineStage.DETECT_MENTIONS,
            "successful",
            item_id,
            f"Detected and persisted {inserted} candidate dataset mentions.",
            metrics,
        )


def extract_features_pipeline_item(
    item_id: int,
    task_id: str | None = None,
    claimed: bool = False,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        if not claimed and not repo.start_item_stage(item_id, PipelineStage.EXTRACT_FEATURES, task_id):
            return _item_result(PipelineStage.EXTRACT_FEATURES, "skipped", item_id, "Item stage was already terminal.")
        context = repo.get_pipeline_item_context(item_id)
        if not context:
            return _missing_item_result(repo, item_id, PipelineStage.EXTRACT_FEATURES)
        rows = repo.get_unprocessed_candidates_for_publication(context["publication_row_id"])
        mentions: list[DatasetMention] = []
        candidate_ids: list[int] = []
        for candidate in rows:
            mentions.append(_mention_from_candidate(candidate))
            candidate_ids.append(candidate["candidate_id"])
        inserted = repo.persist_dataset_mentions(mentions, candidate_ids)
        metrics = {"mentions": inserted, "paper_id": context["paper_id"]}
        repo.finish_item_stage(item_id, PipelineStage.EXTRACT_FEATURES, "successful", metrics)
        return _item_result(
            PipelineStage.EXTRACT_FEATURES,
            "successful",
            item_id,
            f"Promoted {inserted} candidates into v1 mention records.",
            metrics,
        )


def match_um_dataset_pipeline_item(
    item_id: int,
    task_id: str | None = None,
    claimed: bool = False,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        if not claimed and not repo.start_item_stage(item_id, PipelineStage.MATCH_UM_DATASET, task_id):
            return _item_result(PipelineStage.MATCH_UM_DATASET, "skipped", item_id, "Item stage was already terminal.")
        context = repo.get_pipeline_item_context(item_id)
        if not context:
            return _missing_item_result(repo, item_id, PipelineStage.MATCH_UM_DATASET)
        records = repo.list_um_dataset_records()
        if not records:
            message = "No UM datasets are imported. Run import-um-datasets first."
            repo.finish_item_stage(item_id, PipelineStage.MATCH_UM_DATASET, "failed", {"message": message}, message)
            return _item_result(PipelineStage.MATCH_UM_DATASET, "failed", item_id, message)

        mention_rows = repo.get_unmatched_mentions_for_publication(context["publication_row_id"])
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
        metrics = {"decisions": inserted, "paper_id": context["paper_id"]}
        repo.finish_item_stage(item_id, PipelineStage.MATCH_UM_DATASET, "successful", metrics)
        return _item_result(
            PipelineStage.MATCH_UM_DATASET,
            "successful",
            item_id,
            f"Matched {inserted} mentions against UM metadata candidates.",
            metrics,
        )


def export_insights_csv(
    output_path: str,
    rows: list[dict[str, Any]] | None = None,
    pipeline_run_id: int | None = None,
) -> dict[str, Any]:
    with PipelineRepository() as repo:
        export_rows = rows if rows is not None else repo.export_insight_rows()
        path = _project_path(output_path)
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
    source = _project_path(path)
    if looks_like_openalex_export(source):
        return load_um_openalex_exports(source)

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
    if successful and failed:
        status = "completed_with_errors"
    elif successful:
        status = "successful"
    elif failed:
        status = "failed"
    else:
        status = "skipped"
    return _stage_result(
        stage,
        status,
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
    status: Literal["successful", "completed_with_errors", "failed", "skipped"],
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


def _item_result(
    stage: PipelineStage,
    status: Literal["successful", "failed", "skipped"],
    item_id: int,
    message: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "stage": stage.value,
        "status": status,
        "item_id": item_id,
        "message": message,
        "payload": payload or {},
    }


def _missing_item_result(repo: PipelineRepository, item_id: int, stage: PipelineStage) -> dict[str, Any]:
    message = f"Pipeline item {item_id} was not found."
    repo.finish_item_stage(item_id, stage, "failed", {"message": message}, message)
    return _item_result(stage, "failed", item_id, message)


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


DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.I)
URL_RE = re.compile(r"https?://[^\s<>{}\[\]]+", re.I)
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")


def _mention_from_candidate(candidate: dict[str, Any]) -> DatasetMention:
    text = str(candidate["evidence_text"])
    doi_match = DOI_RE.search(text)
    url_match = URL_RE.search(text)
    years = {int(value) for value in YEAR_RE.findall(text)}
    lowered = text.casefold()

    if any(
        phrase in lowered
        for phrase in ("we collected", "we gathered", "data were collected", "data was collected")
    ):
        role = DatasetRole.CREATED
    elif re.search(r"\b(used|using|analysed|analyzed)\b", lowered):
        role = DatasetRole.USED
    else:
        role = DatasetRole.UNCLEAR

    dataset_name = str(candidate["dataset_name"])
    directness = (
        ReferenceDirectness.DIRECT
        if doi_match or url_match or dataset_name != "Author-described dataset"
        else ReferenceDirectness.INFORMAL
    )
    return DatasetMention(
        publication_id=str(candidate["publication_id"]),
        dataset_name=dataset_name,
        dataset_role=role,
        reference_directness=directness,
        evidence=MentionEvidence(
            body_quote=text,
            section_heading=candidate.get("section_heading"),
            standardized_section=candidate.get("standardized_section"),
        ),
        metadata=DatasetMetadata(
            persistent_identifier=doi_match.group(0).rstrip(".,;:") if doi_match else None,
            dataset_url=url_match.group(0).rstrip(".,;:)") if url_match else None,
            dataset_year=next(iter(years)) if len(years) == 1 else None,
        ),
        provenance=ExtractionProvenance(
            char_start=candidate.get("char_start"),
            char_end=candidate.get("char_end"),
            confidence=candidate.get("score", 0.0),
            prompt_version="rules-v2",
        ),
    )


def _project_path(path: str | Path) -> Path:
    resolved = Path(path)
    if resolved.is_absolute():
        return resolved
    return PROJECT_ROOT / resolved


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
