"""Load UM/PURE OpenAlex CSV exports into DataSight records."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

from datasight.domain.schemas import UMDatasetRecord
from datasight.infrastructure.pubfetcher.openalex import openalex_work_id


EXPORT_FILENAMES = {
    "data": "OPEN_ALEX_DATA.csv",
    "topics": "OPEN_ALEX_DATA_TOPICS.csv",
    "keywords": "OPEN_ALEX_DATA_KEYWORDS.csv",
    "concepts": "OPEN_ALEX_DATA_CONCEPTS.csv",
    "mesh": "OPEN_ALEX_DATA_MESHS.csv",
    "affiliations": "OPEN_ALEX_AFFILIATIONS.csv",
    "related_works": "OPEN_ALEX_DATA_RELATED_WORKS.csv",
}


def looks_like_openalex_export(path: Path) -> bool:
    if path.is_dir():
        return (path / EXPORT_FILENAMES["data"]).exists()
    if path.suffix.lower() != ".csv":
        return False
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fields = {field.upper() for field in reader.fieldnames or []}
    except OSError:
        return False
    return {"OPAL_ID", "OPAL_TITLE"} <= fields or {"OPENALEX_ID", "OPAL_TITLE"} <= fields


def load_um_openalex_exports(path: Path) -> list[UMDatasetRecord]:
    base = path if path.is_dir() else path.parent
    data_path = base / EXPORT_FILENAMES["data"] if path.is_dir() else path
    data_rows = _read_csv(data_path)
    child_rows = {
        key: _group_by_openalex_id(_read_csv(base / filename))
        for key, filename in EXPORT_FILENAMES.items()
        if key != "data" and (base / filename).exists()
    }

    records = []
    for row in data_rows:
        normalized = {_key(key): value for key, value in row.items()}
        openalex_id = normalized.get("OPAL_ID") or normalized.get("OPENALEX_ID") or ""
        bare_id = openalex_work_id(openalex_id)
        doi = _first_value(
            normalized.get("OPAL_DOI"),
            normalized.get("OPAL_IDS_DOI"),
            normalized.get("DOI"),
        )
        title = _first_value(normalized.get("OPAL_TITLE"), normalized.get("TITLE"), bare_id)
        keywords = _split_terms(normalized.get("OPAL_KEYWORDS"))
        raw_openalex = _raw_openalex(row, child_rows, bare_id or openalex_id)
        keywords = list(dict.fromkeys([*keywords, *_child_terms(raw_openalex.get("keywords"), "keyword")]))
        records.append(
            UMDatasetRecord(
                um_dataset_id=bare_id or openalex_id or str(normalized.get("PURE_ID") or title),
                title=title,
                aliases=_split_terms(normalized.get("OPAL_PRIMARY_RAW_SOURCE_NAME")),
                creators=_split_terms(normalized.get("OPAL_AUTHORS")),
                doi=doi,
                url=_first_value(normalized.get("URL"), normalized.get("OPAL_OA_URL")),
                year=_int(normalized.get("OPAL_YEAR")),
                repository=normalized.get("OPAL_PRIMARY_SOURCE_DISPLAY_NAME"),
                keywords=keywords,
                raw={"source": "openalex_pure_export", "openalex": raw_openalex, "row": row},
            )
        )
    return records


def _raw_openalex(
    row: dict[str, str],
    child_rows: dict[str, dict[str, list[dict[str, str]]]],
    openalex_id: str,
) -> dict[str, Any]:
    normalized = {_key(key): value for key, value in row.items()}
    raw = {
        "id": normalized.get("OPAL_ID") or normalized.get("OPENALEX_ID") or openalex_id,
        "doi": normalized.get("OPAL_DOI") or normalized.get("OPAL_IDS_DOI"),
        "display_name": normalized.get("OPAL_TITLE"),
        "publication_year": _int(normalized.get("OPAL_YEAR")),
        "publication_date": normalized.get("OPAL_DATE"),
        "language": normalized.get("OPAL_LANGUAGE"),
        "type": normalized.get("OPAL_TYPE"),
        "open_access": {
            "is_oa": _bool(normalized.get("OPAL_IS_OA")),
            "oa_status": normalized.get("OPAL_OA_STATUS"),
            "oa_url": normalized.get("OPAL_OA_URL"),
            "any_repository_has_fulltext": _bool(normalized.get("OPAL_HAS_FULLTEXT")),
        },
        "primary_location": {
            "id": normalized.get("OPAL_PRIMARY_ID"),
            "is_oa": _bool(normalized.get("OPAL_PRIMARY_IS_OA")),
            "landing_page_url": normalized.get("OPAL_PRIMARY_IS_LAND_URL"),
            "pdf_url": normalized.get("OPAL_PRIMARY_IS_PDF_URL"),
            "source": {
                "id": normalized.get("OPAL_PRIMARY_SOURCE_ID"),
                "display_name": normalized.get("OPAL_PRIMARY_SOURCE_DISPLAY_NAME"),
                "host_organization": normalized.get("OPAL_PRIMARY_SOURCE_HOST_ORGANIZATION"),
                "host_organization_name": normalized.get("OPAL_PRIMARY_SOURCE_HOST_ORGANIZATION_NAME"),
                "type": normalized.get("OPAL_PRIMARY_SOURCE_TYPE"),
            },
            "version": normalized.get("OPAL_PRIMARY_VERSION"),
            "raw_source_name": normalized.get("OPAL_PRIMARY_RAW_SOURCE_NAME"),
        },
        "best_oa_location": {
            "landing_page_url": normalized.get("OPAL_BEST_OA_IS_LAND_URL"),
            "pdf_url": normalized.get("OPAL_BEST_OA_IS_PDF_URL"),
            "license": normalized.get("OPAL_BEST_OA_LICENSE"),
            "version": normalized.get("OPAL_BEST_OA_VERSION"),
            "source": {"display_name": normalized.get("OPAL_BEST_OA_SOURCE_DISPLAY_NAME")},
        },
        "authorships": child_rows.get("affiliations", {}).get(openalex_id, []),
        "topics": child_rows.get("topics", {}).get(openalex_id, []),
        "keywords": child_rows.get("keywords", {}).get(openalex_id, []),
        "concepts": child_rows.get("concepts", {}).get(openalex_id, []),
        "mesh": child_rows.get("mesh", {}).get(openalex_id, []),
        "related_works": [
            related.get("related_work_id") or related.get("RELATED_WORK_ID")
            for related in child_rows.get("related_works", {}).get(openalex_id, [])
            if related.get("related_work_id") or related.get("RELATED_WORK_ID")
        ],
        "abstract": normalized.get("OPAL_ABSTRACT"),
        "cited_by_count": _int(normalized.get("OPAL_CITED_BY_COUNT")),
        "is_retracted": _bool(normalized.get("OPAL_IS_RETRACTED")),
        "has_fulltext": _bool(normalized.get("OPAL_HAS_FULLTEXT")),
        "updated_date": normalized.get("OPAL_UPDATED"),
        "created_date": normalized.get("OPAL_CREATED"),
    }
    return raw


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _group_by_openalex_id(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        normalized = {_key(key): value for key, value in row.items()}
        openalex_id = openalex_work_id(
            normalized.get("OPENALEX_ID")
            or normalized.get("OPAL_ID")
            or normalized.get("OPENALEX_WORK_ID")
            or ""
        )
        if openalex_id:
            grouped[openalex_id].append(_normalize_child_row(normalized, row))
    return grouped


def _child_terms(value: Any, key: str) -> list[str]:
    return [
        str(row[key]).strip()
        for row in value or []
        if isinstance(row, dict) and row.get(key) and str(row[key]).strip()
    ]


def _normalize_child_row(normalized: dict[str, str], original: dict[str, str]) -> dict[str, str]:
    row = dict(original)
    aliases = {
        "keyword": normalized.get("KEYWORD"),
        "score": normalized.get("SCORE"),
        "concept": normalized.get("CONCEPT"),
        "level": normalized.get("LEVEL"),
        "mesh": normalized.get("MESH"),
        "qualifier": normalized.get("QUALIFIER"),
        "domain": normalized.get("DOMAIN"),
        "domain_id": normalized.get("DOMAIN_ID"),
        "field": normalized.get("FIELD"),
        "field_id": normalized.get("FIELD_ID"),
        "subfield": normalized.get("SUBFIELD"),
        "subfield_id": normalized.get("SUBFIELD_ID"),
        "topic": normalized.get("TOPIC"),
        "topic_id": normalized.get("TOPIC_ID"),
        "topic_primary": normalized.get("TOPIC_PRIMARY"),
        "topic_index": normalized.get("TOPIC_INDEX"),
        "related_work_id": normalized.get("RELATED_WORK_ID"),
        "openalex_author_id": normalized.get("OPENALEX_AUTHOR_ID"),
        "openalex_author": normalized.get("OPENALEX_AUTHOR"),
        "openalex_orcid": normalized.get("OPENALEX_ORCID"),
        "openalex_is_corresponding": normalized.get("OPENALEX_IS_CORRESPONDING"),
        "openalex_inst_id": normalized.get("OPENALEX_INST_ID"),
        "openalex_inst_name": normalized.get("OPENALEX_INST_NAME"),
        "openalex_inst_ror": normalized.get("OPENALEX_INST_ROR"),
        "openalex_inst_country_code": normalized.get("OPENALEX_INST_COUNTRY_CODE"),
        "openalex_inst_type": normalized.get("OPENALEX_INST_TYPE"),
    }
    row.update({key: value for key, value in aliases.items() if value not in (None, "")})
    return row


def _split_terms(value: str | None) -> list[str]:
    if not value:
        return []
    parts = value.replace("|", ";").replace(",", ";").split(";")
    return [part.strip() for part in parts if part.strip()]


def _first_value(*values: Any) -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _key(value: str | None) -> str:
    return (value or "").strip().upper()


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value)))
    except ValueError:
        return None


def _bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
