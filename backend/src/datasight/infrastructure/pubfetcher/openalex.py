"""OpenAlex API client and work normalization helpers."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import requests

from datasight.config import OPENALEX_API_KEY, OPENALEX_API_URL, OPENALEX_MAILTO

logger = logging.getLogger(__name__)

OPENALEX_WORK_SELECT_FIELDS = [
    "id",
    "doi",
    "title",
    "display_name",
    "publication_year",
    "publication_date",
    "ids",
    "language",
    "type",
    "primary_location",
    "best_oa_location",
    "open_access",
    "authorships",
    "topics",
    "keywords",
    "concepts",
    "mesh",
    "referenced_works",
    "datasets",
    "related_works",
    "abstract_inverted_index",
    "cited_by_count",
    "is_retracted",
    "has_fulltext",
    "updated_date",
    "created_date",
]


class OpenAlexApiError(RuntimeError):
    """Raised when OpenAlex returns a permanent error or retries are exhausted."""


class OpenAlexClient:
    """Small synchronous client for the OpenAlex REST API."""

    _REQUEST_DELAY = 0.15
    _MAX_PER_PAGE = 100

    def __init__(
        self,
        api_key: str | None = None,
        mailto: str | None = None,
        base_url: str = OPENALEX_API_URL,
        session: Any | None = None,
        request_delay: float = _REQUEST_DELAY,
    ) -> None:
        self.api_key = api_key if api_key is not None else OPENALEX_API_KEY
        self.mailto = mailto if mailto is not None else OPENALEX_MAILTO
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.request_delay = request_delay
        self.headers = {"User-Agent": "DataSight/0.1 OpenAlex client"}

    def search_works(
        self,
        query: str | None = None,
        limit: int = 100,
        filters: Mapping[str, str | int | bool | Sequence[str | int]] | None = None,
        select: Sequence[str] | None = None,
        sort: str | None = None,
        cursor: str = "*",
    ) -> list[dict[str, Any]]:
        """Search/list works and return DataSight-normalized publication dicts."""
        raw_works = self.list_entities(
            "works",
            query=query,
            limit=limit,
            filters=filters,
            select=select or OPENALEX_WORK_SELECT_FIELDS,
            sort=sort,
            cursor=cursor,
        )
        return [normalize_openalex_work(work) for work in raw_works]

    def get_work(
        self,
        work_id: str,
        select: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        data = self.get_entity("works", work_id, select=select or OPENALEX_WORK_SELECT_FIELDS)
        return normalize_openalex_work(data)

    def bulk_get_works_by_doi(
        self,
        dois: Iterable[str],
        select: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_dois = [_normalize_doi_for_filter(doi) for doi in dois if doi]
        works: list[dict[str, Any]] = []
        for batch in _chunks(normalized_dois, 50):
            works.extend(
                self.search_works(
                    limit=len(batch),
                    filters={"doi": batch},
                    select=select or OPENALEX_WORK_SELECT_FIELDS,
                )
            )
        return works

    def get_entity(
        self,
        entity: str,
        entity_id: str,
        select: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if select:
            params["select"] = ",".join(select)
        return self._request_json("GET", f"/{entity}/{entity_id}", params)

    def list_entities(
        self,
        entity: str,
        query: str | None = None,
        limit: int = 100,
        filters: Mapping[str, str | int | bool | Sequence[str | int]] | None = None,
        select: Sequence[str] | None = None,
        sort: str | None = None,
        cursor: str = "*",
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []

        results: list[dict[str, Any]] = []
        next_cursor: str | None = cursor
        while next_cursor and len(results) < limit:
            per_page = min(self._MAX_PER_PAGE, limit - len(results))
            params: dict[str, Any] = {
                "per_page": per_page,
                "cursor": next_cursor,
            }
            if query:
                params["search"] = query
            if filters:
                params["filter"] = _encode_filters(filters)
            if select:
                params["select"] = ",".join(select)
            if sort:
                params["sort"] = sort

            data = self._request_json("GET", f"/{entity}", params)
            page_results = list(data.get("results") or [])
            results.extend(page_results)
            next_cursor = (data.get("meta") or {}).get("next_cursor")
            if not page_results:
                break
            if len(results) < limit and self.request_delay > 0:
                time.sleep(self.request_delay)

        return results[:limit]

    def _request_json(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        max_retries: int = 5,
    ) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        request_params = dict(params or {})
        if self.api_key:
            request_params["api_key"] = self.api_key
        if self.mailto:
            request_params["mailto"] = self.mailto

        backoff = 2.0
        for attempt in range(max_retries):
            try:
                response = self.session.request(
                    method,
                    url,
                    params=request_params,
                    headers=self.headers,
                    timeout=30,
                )
            except requests.exceptions.RequestException as exc:
                if attempt == max_retries - 1:
                    raise OpenAlexApiError(f"OpenAlex network error: {exc}") from exc
                time.sleep(backoff * (2**attempt))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt == max_retries - 1:
                    raise OpenAlexApiError(
                        f"OpenAlex transient error {response.status_code}: {response.text[:200]}"
                    )
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff * (2**attempt)
                logger.warning("OpenAlex returned %s; retrying in %.1fs", response.status_code, wait)
                time.sleep(wait)
                continue

            if 400 <= response.status_code < 500:
                raise OpenAlexApiError(
                    f"OpenAlex client error {response.status_code}: {response.text[:300]}"
                )

            try:
                data = response.json()
            except json.JSONDecodeError as exc:
                raise OpenAlexApiError("OpenAlex returned invalid JSON") from exc
            if not isinstance(data, dict):
                raise OpenAlexApiError("OpenAlex returned a non-object JSON response")
            return data

        raise OpenAlexApiError("OpenAlex retries exhausted")


def normalize_openalex_work(work: Mapping[str, Any]) -> dict[str, Any]:
    """Convert an OpenAlex work into the publication shape used by DataSight."""
    raw = dict(work)
    openalex_id = str(raw.get("id") or "")
    primary_location = _dict(raw.get("primary_location"))
    best_oa_location = _dict(raw.get("best_oa_location"))
    open_access = _dict(raw.get("open_access"))
    source = _dict(primary_location.get("source"))

    normalized = {
        "paperId": openalex_work_id(openalex_id),
        "id": openalex_id,
        "doi": _strip_doi_url(raw.get("doi")),
        "title": raw.get("display_name") or raw.get("title"),
        "abstract": reconstruct_abstract(raw.get("abstract_inverted_index")),
        "year": raw.get("publication_year"),
        "publication_date": raw.get("publication_date"),
        "language": raw.get("language"),
        "publication_type": raw.get("type"),
        "url": openalex_id,
        "source_url": openalex_id,
        "open_access_url": _best_pdf_url(best_oa_location, primary_location, open_access),
        "oa_status": open_access.get("oa_status"),
        "cited_by_count": raw.get("cited_by_count"),
        "is_retracted": raw.get("is_retracted"),
        "has_fulltext": raw.get("has_fulltext") or open_access.get("any_repository_has_fulltext"),
        "primary_source_name": source.get("display_name") or primary_location.get("raw_source_name"),
        "raw": raw,
    }
    return normalized


def reconstruct_abstract(abstract_inverted_index: Any) -> str | None:
    if not isinstance(abstract_inverted_index, Mapping) or not abstract_inverted_index:
        return None
    positions: dict[int, str] = {}
    for token, indexes in abstract_inverted_index.items():
        if not isinstance(indexes, list):
            continue
        for index in indexes:
            if isinstance(index, int):
                positions[index] = str(token)
    if not positions:
        return None
    return " ".join(positions[index] for index in sorted(positions))


def openalex_work_id(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).rstrip("/")
    return text.rsplit("/", 1)[-1]


def _encode_filters(filters: Mapping[str, str | int | bool | Sequence[str | int]]) -> str:
    parts = []
    for key, value in filters.items():
        if isinstance(value, bool):
            encoded_value = str(value).lower()
        elif isinstance(value, Sequence) and not isinstance(value, str):
            encoded_value = "|".join(str(item) for item in value if item is not None)
        else:
            encoded_value = str(value)
        if encoded_value:
            parts.append(f"{key}:{encoded_value}")
    return ",".join(parts)


def _best_pdf_url(
    best_oa_location: Mapping[str, Any],
    primary_location: Mapping[str, Any],
    open_access: Mapping[str, Any],
) -> str | None:
    for location in (best_oa_location, primary_location):
        pdf_url = location.get("pdf_url")
        if pdf_url:
            return str(pdf_url)
    oa_url = open_access.get("oa_url")
    if oa_url and str(oa_url).lower().endswith(".pdf"):
        return str(oa_url)
    return None


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _strip_doi_url(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if text.lower().startswith(prefix):
            return text[len(prefix) :]
    return text


def _normalize_doi_for_filter(value: str) -> str:
    return _strip_doi_url(value) or value


def _chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]
