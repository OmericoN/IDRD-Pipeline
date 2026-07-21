"""Load and validate UM/PURE OpenAlex exports into DataSight records."""

from __future__ import annotations

import csv
import io
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datasight.domain.schemas import UMDatasetRecord
from datasight.infrastructure.pubfetcher.openalex import openalex_work_id


EXPORT_FILENAMES: dict[str, tuple[str, ...]] = {
    "data": ("OPEN_ALEX_DATA.csv",),
    "topics": ("OPEN_ALEX_DATA_TOPICS.csv",),
    "keywords": ("OPEN_ALEX_DATA_KEYWORDS.csv",),
    "concepts": ("OPEN_ALEX_DATA_CONCEPTS.csv",),
    "mesh": ("OPEN_ALEX_DATA_MESHS.csv",),
    "affiliations": ("OPEN_ALEX_AFFILIATION.csv", "OPEN_ALEX_AFFILIATIONS.csv"),
    "related_works": ("OPEN_ALEX_DATA_RELATED_WORKS.csv",),
}

REQUIRED_CHILD_FIELDS: dict[str, set[str]] = {
    "topics": {"OPENALEX_ID", "TOPIC"},
    "keywords": {"OPENALEX_ID", "KEYWORD"},
    "affiliations": {"OPENALEX_ID", "AUTH_INDEX", "OPENALEX_AUTHOR"},
    "related_works": {"OPENALEX_ID", "RELATED_WORK_ID"},
}

NULL_SENTINELS = {"", "null", "none", "n/a", "na", "unknown"}
DOI_PATTERN = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)
STRICT_UM_INSTITUTION_IDS = {"I34352273", "I2800191616"}
STRICT_UM_RORS = {"02jz4aj89", "02d9ce178"}


@dataclass(frozen=True)
class CSVTable:
    rows: list[dict[str, str]]
    fields: tuple[str, ...]
    encoding: str
    delimiter: str


@dataclass(frozen=True)
class UMOpenAlexImportBundle:
    records: list[UMDatasetRecord]
    warnings: list[str]
    metrics: dict[str, Any]


def looks_like_openalex_export(path: Path) -> bool:
    if path.is_dir():
        return _find_export_file(path, "data") is not None
    if path.suffix.lower() != ".csv":
        return False
    try:
        table = _read_csv(path)
    except (OSError, UnicodeError, ValueError):
        return False
    fields = set(table.fields)
    return "OPAL_TITLE" in fields and bool({"OPAL_ID", "OPENALEX_ID"} & fields)


def load_um_openalex_exports(path: Path) -> list[UMDatasetRecord]:
    """Compatibility wrapper returning only records."""
    return load_um_openalex_export_bundle(path).records


def load_um_openalex_export_bundle(path: Path) -> UMOpenAlexImportBundle:
    base = path if path.is_dir() else path.parent
    data_path = _find_export_file(base, "data") if path.is_dir() else path
    if data_path is None:
        raise ValueError(f"Missing required {EXPORT_FILENAMES['data'][0]} export in {base}.")

    warnings: list[str] = []
    tables: dict[str, CSVTable] = {"data": _read_csv(data_path)}
    _validate_main_headers(tables["data"].fields, data_path)

    for key in EXPORT_FILENAMES:
        if key == "data":
            continue
        child_path = _find_export_file(base, key)
        if child_path is None:
            warnings.append(f"Optional {key} export is missing.")
            tables[key] = CSVTable([], (), "missing", ",")
            continue
        table = _read_csv(child_path)
        fields = set(table.fields)
        if key in {"concepts", "mesh"} and "OPENALEX_ID" not in fields:
            warnings.append(f"{child_path.name} declares no usable {key} rows.")
            table = CSVTable([], table.fields, table.encoding, table.delimiter)
        elif key in REQUIRED_CHILD_FIELDS:
            missing = REQUIRED_CHILD_FIELDS[key] - fields
            if missing:
                raise ValueError(
                    f"{child_path.name} is missing required columns: {', '.join(sorted(missing))}."
                )
        tables[key] = table

    main_rows = tables["data"].rows
    main_ids: list[str] = []
    for index, row in enumerate(main_rows, start=2):
        normalized = _normalized_row(row)
        value = normalized.get("OPAL_ID") or normalized.get("OPENALEX_ID")
        work_id = openalex_work_id(value)
        if not work_id:
            raise ValueError(f"{data_path.name} row {index} has a blank OpenAlex ID.")
        main_ids.append(work_id)
    duplicate_ids = [work_id for work_id, count in Counter(main_ids).items() if count > 1]
    if duplicate_ids:
        raise ValueError(
            f"{data_path.name} contains duplicate OpenAlex IDs: {', '.join(sorted(duplicate_ids)[:10])}."
        )

    main_id_set = set(main_ids)
    child_rows: dict[str, dict[str, list[dict[str, Any]]]] = {}
    child_counts: dict[str, int] = {}
    coverage: dict[str, float] = {}
    for key, table in tables.items():
        if key == "data":
            continue
        grouped = _group_by_openalex_id(table.rows)
        orphan_ids = set(grouped) - main_id_set
        if orphan_ids:
            raise ValueError(
                f"{key} export contains {len(orphan_ids)} OpenAlex IDs absent from the main export."
            )
        child_rows[key] = grouped
        child_counts[key] = len(table.rows)
        coverage[key] = round(100 * len(grouped) / len(main_id_set), 2) if main_id_set else 0.0

    records: list[UMDatasetRecord] = []
    fallback_title_count = 0
    invalid_doi_count = 0
    strict_um_count = 0
    original_titles: list[str] = []
    for row in main_rows:
        normalized = _normalized_row(row)
        source_id = normalized.get("OPAL_ID") or normalized.get("OPENALEX_ID")
        bare_id = openalex_work_id(source_id)
        raw_doi = _first_value(
            normalized.get("OPAL_DOI"), normalized.get("OPAL_IDS_DOI"), normalized.get("DOI")
        )
        doi, invalid_doi = _normalize_doi(raw_doi)
        invalid_doi_count += int(invalid_doi)

        source_title = _first_value(normalized.get("OPAL_TITLE"), normalized.get("TITLE"))
        missing_title = source_title is None
        title = source_title or f"Untitled dataset ({doi or bare_id})"
        fallback_title_count += int(missing_title)
        if source_title:
            original_titles.append(_normalize_title(source_title))

        raw_openalex = _raw_openalex(
            row=row,
            child_rows=child_rows,
            openalex_id=bare_id,
            title=title,
            doi=doi,
        )
        strict_um_count += int(raw_openalex["um_affiliation"]["strict_match"])
        keywords = _split_terms(normalized.get("OPAL_KEYWORDS"))
        keywords = list(
            dict.fromkeys([*keywords, *_child_terms(raw_openalex.get("keywords"), "keyword")])
        )
        creators = _creators(raw_openalex.get("authorships"))
        if not creators:
            creators = _split_terms(normalized.get("OPAL_AUTHORS"))

        records.append(
            UMDatasetRecord(
                um_dataset_id=bare_id,
                title=title,
                aliases=[],
                creators=creators,
                doi=doi,
                url=_dataset_url(normalized, doi, source_id),
                year=_int(normalized.get("OPAL_YEAR")),
                repository=_first_value(normalized.get("OPAL_PRIMARY_SOURCE_DISPLAY_NAME")),
                keywords=keywords,
                raw={
                    "source": "openalex_pure_export",
                    "openalex": raw_openalex,
                    "row": row,
                    "quality": {
                        "missing_title": missing_title,
                        "invalid_doi": raw_doi if invalid_doi else None,
                    },
                },
            )
        )

    title_counts = Counter(original_titles)
    duplicate_title_groups = sum(count > 1 for count in title_counts.values())
    duplicate_title_rows = sum(count for count in title_counts.values() if count > 1)
    if fallback_title_count:
        warnings.append(
            f"Used identifier-based fallback titles for {fallback_title_count} records."
        )
    if invalid_doi_count:
        warnings.append(f"Discarded {invalid_doi_count} invalid DOI values.")
    if duplicate_title_groups:
        warnings.append(
            f"Preserved {duplicate_title_rows} records across {duplicate_title_groups} repeated-title groups."
        )

    metrics: dict[str, Any] = {
        "source_rows": len(main_rows),
        "child_rows": child_counts,
        "coverage_percent": coverage,
        "orphan_child_rows": 0,
        "fallback_title_count": fallback_title_count,
        "invalid_doi_count": invalid_doi_count,
        "duplicate_title_groups": duplicate_title_groups,
        "duplicate_title_rows": duplicate_title_rows,
        "strict_um_affiliation_records": strict_um_count,
        "encodings": {key: table.encoding for key, table in tables.items()},
        "delimiters": {key: table.delimiter for key, table in tables.items()},
    }
    return UMOpenAlexImportBundle(records=records, warnings=warnings, metrics=metrics)


def _raw_openalex(
    row: dict[str, str],
    child_rows: dict[str, dict[str, list[dict[str, Any]]]],
    openalex_id: str,
    title: str,
    doi: str | None,
) -> dict[str, Any]:
    normalized = _normalized_row(row)
    authorships = _canonical_authorships(child_rows.get("affiliations", {}).get(openalex_id, []))
    strict_ids, strict_rors = _strict_um_identifiers(authorships)
    return {
        "id": normalized.get("OPAL_ID") or normalized.get("OPENALEX_ID") or openalex_id,
        "doi": doi,
        "display_name": title,
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
                "host_organization_name": normalized.get(
                    "OPAL_PRIMARY_SOURCE_HOST_ORGANIZATION_NAME"
                ),
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
        "authorships": authorships,
        "topics": child_rows.get("topics", {}).get(openalex_id, []),
        "keywords": child_rows.get("keywords", {}).get(openalex_id, []),
        "concepts": child_rows.get("concepts", {}).get(openalex_id, []),
        "mesh": child_rows.get("mesh", {}).get(openalex_id, []),
        "related_works": [
            related_id
            for related in child_rows.get("related_works", {}).get(openalex_id, [])
            if (related_id := _first_value(related.get("related_work_id"), related.get("RELATED_WORK_ID")))
        ],
        "abstract": normalized.get("OPAL_ABSTRACT"),
        "cited_by_count": _int(normalized.get("OPAL_CITED_BY_COUNT")),
        "is_retracted": _bool(normalized.get("OPAL_IS_RETRACTED")),
        "has_fulltext": _bool(normalized.get("OPAL_HAS_FULLTEXT")),
        "updated_date": normalized.get("OPAL_UPDATED"),
        "created_date": normalized.get("OPAL_CREATED"),
        "um_affiliation": {
            "strict_match": bool(strict_ids or strict_rors),
            "institution_ids": strict_ids,
            "rors": strict_rors,
        },
    }


def _read_csv(path: Path) -> CSVTable:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        text = raw.decode("cp1252")
        encoding = "cp1252"
    first_line = text.splitlines()[0] if text.splitlines() else ""
    delimiter = "\t" if first_line.count("\t") > first_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=delimiter)
    fields = tuple(_key(field) for field in reader.fieldnames or ())
    rows: list[dict[str, str]] = []
    for row_number, row in enumerate(reader, start=2):
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f"{path.name} row {row_number} does not match its header shape.")
        rows.append({_key(key): str(value) for key, value in row.items()})
    return CSVTable(rows=rows, fields=fields, encoding=encoding, delimiter="tab" if delimiter == "\t" else "comma")


def _find_export_file(base: Path, key: str) -> Path | None:
    for filename in EXPORT_FILENAMES[key]:
        candidate = base / filename
        if candidate.exists():
            return candidate
    return None


def _validate_main_headers(fields: tuple[str, ...], path: Path) -> None:
    field_set = set(fields)
    if "OPAL_TITLE" not in field_set or not ({"OPAL_ID", "OPENALEX_ID"} & field_set):
        raise ValueError(
            f"{path.name} must contain OPAL_TITLE and either OPAL_ID or OPENALEX_ID."
        )


def _group_by_openalex_id(rows: list[dict[str, str]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        normalized = _normalized_row(row)
        work_id = openalex_work_id(
            normalized.get("OPENALEX_ID")
            or normalized.get("OPAL_ID")
            or normalized.get("OPENALEX_WORK_ID")
        )
        if not work_id:
            raise ValueError("Child export contains a blank OpenAlex ID.")
        grouped[work_id].append(_normalize_child_row(normalized, row))
    return grouped


def _normalize_child_row(normalized: dict[str, str | None], original: dict[str, str]) -> dict[str, Any]:
    row: dict[str, Any] = dict(original)
    aliases = {
        "auth_index": normalized.get("AUTH_INDEX"),
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
    row.update({key: value for key, value in aliases.items() if value is not None})
    return row


def _canonical_authorships(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_int(row.get("auth_index")) or 0].append(row)
    authorships: list[dict[str, Any]] = []
    for auth_index in sorted(grouped):
        author_rows = grouped[auth_index]
        first = author_rows[0]
        institutions: list[dict[str, Any]] = []
        seen_institutions: set[tuple[Any, ...]] = set()
        for row in author_rows:
            institution = {
                "id": _clean(row.get("openalex_inst_id")),
                "display_name": _clean(row.get("openalex_inst_name")),
                "ror": _clean(row.get("openalex_inst_ror")),
                "country_code": _clean(row.get("openalex_inst_country_code")),
                "type": _clean(row.get("openalex_inst_type")),
            }
            identity = tuple(institution.values())
            if any(identity) and identity not in seen_institutions:
                seen_institutions.add(identity)
                institutions.append(institution)
        authorships.append(
            {
                "author_index": auth_index,
                "author": {
                    "id": _clean(first.get("openalex_author_id")),
                    "display_name": _clean(first.get("openalex_author")),
                    "orcid": _clean(first.get("openalex_orcid")),
                },
                "is_corresponding": _bool(first.get("openalex_is_corresponding")),
                "institutions": institutions,
                "raw_rows": author_rows,
            }
        )
    return authorships


def _strict_um_identifiers(authorships: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    institution_ids: set[str] = set()
    rors: set[str] = set()
    for authorship in authorships:
        for institution in authorship.get("institutions") or []:
            institution_id = _id_tail(institution.get("id"))
            ror = _id_tail(institution.get("ror"))
            if institution_id in STRICT_UM_INSTITUTION_IDS:
                institution_ids.add(institution_id)
            if ror in STRICT_UM_RORS:
                rors.add(ror)
    return sorted(institution_ids), sorted(rors)


def _creators(value: Any) -> list[str]:
    creators: list[str] = []
    for authorship in value or []:
        author = authorship.get("author") if isinstance(authorship, dict) else None
        name = _clean(author.get("display_name")) if isinstance(author, dict) else None
        if name and name not in creators:
            creators.append(name)
    return creators


def _child_terms(value: Any, key: str) -> list[str]:
    return [
        text
        for row in value or []
        if isinstance(row, dict) and (text := _clean(row.get(key)))
    ]


def _dataset_url(normalized: dict[str, str | None], doi: str | None, source_id: str | None) -> str | None:
    return _first_value(
        normalized.get("OPAL_BEST_OA_IS_LAND_URL"),
        normalized.get("OPAL_PRIMARY_IS_LAND_URL"),
        normalized.get("OPAL_OA_URL"),
        f"https://doi.org/{doi}" if doi else None,
        source_id,
    )


def _normalize_doi(value: str | None) -> tuple[str | None, bool]:
    text = _clean(value)
    if not text:
        return None, False
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if text.lower().startswith(prefix):
            text = text[len(prefix) :]
            break
    if "*" in text or not DOI_PATTERN.match(text):
        return None, True
    return text, False


def _normalized_row(row: dict[str, Any]) -> dict[str, str | None]:
    return {_key(key): _clean(value) for key, value in row.items()}


def _split_terms(value: str | None) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    parts = text.replace("|", ";").replace(",", ";").split(";")
    return [part.strip() for part in parts if part.strip()]


def _first_value(*values: Any) -> str | None:
    for value in values:
        if (cleaned := _clean(value)) is not None:
            return cleaned
    return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if text.casefold() in NULL_SENTINELS else text


def _key(value: str | None) -> str:
    return (value or "").strip().upper()


def _int(value: Any) -> int | None:
    text = _clean(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _bool(value: Any) -> bool | None:
    text = _clean(value)
    if text is None:
        return None
    return text.casefold() in {"1", "true", "yes", "y"}


def _id_tail(value: Any) -> str:
    text = _clean(value)
    return text.rstrip("/").rsplit("/", 1)[-1] if text else ""


def _normalize_title(value: str) -> str:
    return " ".join(value.casefold().split())
