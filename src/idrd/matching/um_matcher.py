"""Layered UM dataset matching without paid vector services."""

from __future__ import annotations

from collections.abc import Iterable

from idrd.matching.normalization import jaccard, normalize_identifier, normalize_text
from idrd.pipeline.schemas import DatasetMention, MatchStatus, UMDatasetRecord, UMMatchDecision


def match_mention_to_um_dataset(
    mention: DatasetMention,
    candidates: Iterable[UMDatasetRecord],
    possible_threshold: float = 0.72,
) -> UMMatchDecision:
    """Match one extracted mention against UM dataset metadata.

    This is the deterministic part of the resolver. pgvector similarity slots into the
    same decision model later by providing pre-ranked candidates with vector scores.
    """
    best_record: UMDatasetRecord | None = None
    best_score = 0.0
    best_fields: list[str] = []
    best_method = "no_match"

    mention_pid = normalize_identifier(mention.metadata.persistent_identifier)
    mention_url = normalize_identifier(str(mention.metadata.dataset_url or ""))

    for record in candidates:
        fields: list[str] = []
        score = 0.0
        method = "metadata_similarity"

        record_doi = normalize_identifier(record.doi)
        record_url = normalize_identifier(record.url)
        if mention_pid and record_doi and mention_pid == record_doi:
            return _decision(mention, record, MatchStatus.MATCHED, 1.0, "exact_pid", ["doi"])
        if mention_url and record_url and mention_url == record_url:
            return _decision(mention, record, MatchStatus.MATCHED, 1.0, "exact_url", ["url"])

        title_score = max(
            [jaccard(mention.dataset_name, record.title)]
            + [jaccard(mention.dataset_name, alias) for alias in record.aliases]
        )
        if title_score:
            fields.append("title_or_alias")
            score += title_score * 0.6

        mention_authors = {normalize_text(author) for author in mention.metadata.dataset_authors}
        record_creators = {normalize_text(creator) for creator in record.creators}
        if mention_authors and record_creators:
            author_overlap = len(mention_authors & record_creators) / len(mention_authors | record_creators)
            if author_overlap:
                fields.append("creators")
                score += author_overlap * 0.25

        if mention.metadata.dataset_year and record.year:
            if mention.metadata.dataset_year == record.year:
                fields.append("year")
                score += 0.15
            elif abs(mention.metadata.dataset_year - record.year) == 1:
                fields.append("near_year")
                score += 0.05

        if score > best_score:
            best_record = record
            best_score = min(score, 0.99)
            best_fields = fields
            best_method = method

    if best_record and best_score >= possible_threshold:
        return _decision(
            mention,
            best_record,
            MatchStatus.POSSIBLE,
            best_score,
            best_method,
            best_fields,
            review_required=True,
        )

    return UMMatchDecision(
        publication_id=mention.publication_id,
        dataset_name=mention.dataset_name,
        status=MatchStatus.NO_MATCH,
        match_method="no_match",
        match_score=best_score,
        matched_fields=best_fields,
        review_required=False,
    )


def _decision(
    mention: DatasetMention,
    record: UMDatasetRecord,
    status: MatchStatus,
    score: float,
    method: str,
    fields: list[str],
    review_required: bool = False,
) -> UMMatchDecision:
    return UMMatchDecision(
        publication_id=mention.publication_id,
        dataset_name=mention.dataset_name,
        status=status,
        um_dataset_id=record.um_dataset_id,
        match_method=method,
        match_score=score,
        matched_fields=fields,
        review_required=review_required,
    )
