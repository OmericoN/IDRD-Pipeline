"""OpenAlex discovery option and candidate scoring helpers."""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from datasight.domain.schemas import UMDatasetRecord

@dataclass(frozen=True)
class DiscoveryOptions:
    topic_ids: tuple[str, ...] = ()
    keyword_terms: tuple[str, ...] = ()
    mesh_terms: tuple[str, ...] = ()
    from_year: int | None = None
    to_year: int | None = None
    use_um_profile: bool = False


@dataclass(frozen=True)
class DiscoveryProfile:
    topic_ids: tuple[str, ...] = ()
    keyword_terms: tuple[str, ...] = ()
    mesh_terms: tuple[str, ...] = ()
    concept_terms: tuple[str, ...] = ()
    source_names: tuple[str, ...] = ()
    openalex_work_ids: tuple[str, ...] = ()
    related_work_ids: tuple[str, ...] = ()

    @property
    def search_terms(self) -> tuple[str, ...]:
        return _dedupe(
            [
                *self.keyword_terms,
                *self.mesh_terms,
                *self.concept_terms,
                *self.source_names,
            ]
        )


@dataclass(frozen=True)
class DiscoveryQuery:
    query: str | None = None
    filters: dict[str, str | int | bool | Sequence[str | int]] = field(default_factory=dict)
    sort: str = "relevance_score:desc"
    reason: str = "query"


def build_discovery_profile(records: Iterable[UMDatasetRecord], max_terms: int = 20) -> DiscoveryProfile:
    topic_ids: Counter[str] = Counter()
    keyword_terms: Counter[str] = Counter()
    mesh_terms: Counter[str] = Counter()
    concept_terms: Counter[str] = Counter()
    source_names: Counter[str] = Counter()
    work_ids: set[str] = set()
    related_work_ids: Counter[str] = Counter()

    for record in records:
        openalex = _openalex_raw(record.raw)
        work_id = _id_tail(str(openalex.get("id") or record.um_dataset_id))
        if work_id.startswith("W"):
            work_ids.add(work_id)

        for keyword in [*record.keywords, *_terms_from_rows(openalex.get("keywords"), "keyword")]:
            _count_term(keyword_terms, keyword)
        for mesh in _terms_from_rows(openalex.get("mesh"), "descriptor_name", fallback_key="mesh"):
            _count_term(mesh_terms, mesh)
        for concept in _terms_from_rows(openalex.get("concepts"), "display_name", fallback_key="concept"):
            _count_term(concept_terms, concept)
        for topic in _rows(openalex.get("topics")):
            topic_id = _id_tail(topic.get("id") or topic.get("topic_id"))
            if topic_id:
                topic_ids[topic_id] += 1
            _count_term(keyword_terms, topic.get("display_name") or topic.get("topic"))
        primary_source = _source_name(openalex)
        _count_term(source_names, primary_source)
        for related_id in openalex.get("related_works") or []:
            related_work_id = _id_tail(str(related_id))
            if related_work_id:
                related_work_ids[related_work_id] += 1

    return DiscoveryProfile(
        topic_ids=tuple(topic for topic, _ in topic_ids.most_common(max_terms)),
        keyword_terms=tuple(term for term, _ in keyword_terms.most_common(max_terms)),
        mesh_terms=tuple(term for term, _ in mesh_terms.most_common(max_terms)),
        concept_terms=tuple(term for term, _ in concept_terms.most_common(max_terms)),
        source_names=tuple(term for term, _ in source_names.most_common(max_terms // 2)),
        openalex_work_ids=tuple(sorted(work_ids)),
        related_work_ids=tuple(work for work, _ in related_work_ids.most_common(max_terms)),
    )


def build_openalex_discovery_queries(
    query: str,
    options: DiscoveryOptions,
    profile: DiscoveryProfile | None = None,
    open_access_only: bool = True,
) -> list[DiscoveryQuery]:
    base_filters = _base_filters(options, open_access_only)
    queries = [DiscoveryQuery(query=query, filters=dict(base_filters), reason="user_query")]
    merged_topic_ids = _dedupe([*options.topic_ids, *((profile.topic_ids if profile else ()) or ())])
    if merged_topic_ids:
        queries.append(
            DiscoveryQuery(
                query=query,
                filters={**base_filters, "topics.id": merged_topic_ids[:100]},
                reason="topic_profile",
            )
        )

    terms = _dedupe(
        [
            *options.keyword_terms,
            *options.mesh_terms,
            *((profile.search_terms if profile else ()) or ()),
        ]
    )
    for term in terms[:6]:
        combined = f"{query} {term}".strip()
        queries.append(DiscoveryQuery(query=combined, filters=dict(base_filters), reason="term_profile"))

    if profile and profile.related_work_ids:
        queries.append(
            DiscoveryQuery(
                query=None,
                filters={**base_filters, "openalex": profile.related_work_ids[:100]},
                sort="cited_by_count:desc",
                reason="related_work_seed",
            )
        )
    return queries


def score_openalex_candidate(
    publication: Mapping[str, Any],
    query: str,
    options: DiscoveryOptions,
    profile: DiscoveryProfile | None = None,
) -> float:
    raw = publication.get("raw") if isinstance(publication.get("raw"), Mapping) else publication
    raw_mapping = raw if isinstance(raw, Mapping) else {}
    score = 0.1
    title = str(publication.get("title") or raw_mapping.get("display_name") or "").casefold()
    abstract = str(publication.get("abstract") or "").casefold()
    for token in query.casefold().split():
        if len(token) > 2 and (token in title or token in abstract):
            score += 0.04

    if publication.get("open_access_url"):
        score += 0.18
    if publication.get("is_retracted"):
        score -= 0.35
    if publication.get("has_fulltext"):
        score += 0.08

    cited_by = publication.get("cited_by_count") or raw_mapping.get("cited_by_count") or 0
    if isinstance(cited_by, int) and cited_by > 0:
        score += min(0.12, math.log10(cited_by + 1) / 25)

    candidate_topics = {_id_tail(topic.get("id")) for topic in _rows(raw_mapping.get("topics"))}
    expected_topics = set(options.topic_ids) | set(profile.topic_ids if profile else ())
    if candidate_topics & expected_topics:
        score += 0.25

    candidate_terms = {
        term.casefold()
        for term in [
            *_terms_from_rows(raw_mapping.get("keywords"), "display_name", fallback_key="keyword"),
            *_terms_from_rows(raw_mapping.get("mesh"), "descriptor_name", fallback_key="mesh"),
            *_terms_from_rows(raw_mapping.get("concepts"), "display_name", fallback_key="concept"),
        ]
    }
    expected_terms = {term.casefold() for term in [*options.keyword_terms, *options.mesh_terms]}
    if profile:
        expected_terms |= {term.casefold() for term in profile.search_terms}
    if candidate_terms & expected_terms:
        score += 0.22

    year = publication.get("year") or raw_mapping.get("publication_year")
    if isinstance(year, int) and year >= 2020:
        score += 0.05
    return max(0.0, min(score, 1.0))


def dedupe_and_score_publications(
    publications: Iterable[dict[str, Any]],
    query: str,
    options: DiscoveryOptions,
    profile: DiscoveryProfile | None = None,
) -> list[dict[str, Any]]:
    best_by_id: dict[str, dict[str, Any]] = {}
    for publication in publications:
        paper_id = str(publication.get("paperId") or publication.get("id") or "")
        if not paper_id:
            continue
        scored = dict(publication)
        scored["discovery_score"] = score_openalex_candidate(scored, query, options, profile)
        existing = best_by_id.get(paper_id)
        if not existing or scored["discovery_score"] > existing.get("discovery_score", 0):
            best_by_id[paper_id] = scored
    return sorted(
        best_by_id.values(),
        key=lambda item: (
            float(item.get("discovery_score") or 0),
            int(item.get("cited_by_count") or 0),
        ),
        reverse=True,
    )


def parse_terms(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        parts = value.replace("|", ",").replace(";", ",").split(",")
    else:
        parts = [str(item) for item in value]
    return _dedupe(part.strip() for part in parts if part.strip())


def _base_filters(
    options: DiscoveryOptions,
    open_access_only: bool,
) -> dict[str, str | int | bool | Sequence[str | int]]:
    filters: dict[str, str | int | bool | Sequence[str | int]] = {}
    if open_access_only:
        filters["open_access.is_oa"] = True
    if options.from_year and options.to_year:
        filters["publication_year"] = f"{options.from_year}-{options.to_year}"
    elif options.from_year:
        filters["publication_year"] = f">{options.from_year - 1}"
    elif options.to_year:
        filters["publication_year"] = f"<{options.to_year + 1}"
    return filters


def _openalex_raw(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    value = raw.get("openalex")
    if isinstance(value, Mapping):
        return value
    return raw


def _rows(value: Any) -> list[Mapping[str, Any]]:
    return [row for row in value or [] if isinstance(row, Mapping)]


def _terms_from_rows(value: Any, key: str, fallback_key: str | None = None) -> list[str]:
    terms = []
    for row in _rows(value):
        item = row.get(key)
        if item is None and fallback_key:
            item = row.get(fallback_key)
        if item:
            terms.append(str(item))
    return terms


def _source_name(openalex: Mapping[str, Any]) -> str | None:
    primary = openalex.get("primary_location")
    if isinstance(primary, Mapping):
        source = primary.get("source")
        if isinstance(source, Mapping) and source.get("display_name"):
            return str(source["display_name"])
    return None


def _count_term(counter: Counter[str], value: Any) -> None:
    if value:
        text = str(value).strip()
        if len(text) > 2:
            counter[text] += 1


def _id_tail(value: Any) -> str:
    if not value:
        return ""
    return str(value).rstrip("/").rsplit("/", 1)[-1]


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item.strip() for item in items if item and item.strip()))
