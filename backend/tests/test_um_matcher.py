from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.matching.um_matcher import match_mention_to_um_dataset
from datasight.domain.schemas import DatasetMention, DatasetMetadata, MentionEvidence, MatchStatus, UMDatasetRecord


def test_match_exact_doi():
    mention = DatasetMention(
        publication_id="p1",
        dataset_name="Maastricht Cohort Data",
        evidence=MentionEvidence(body_quote="We used Maastricht Cohort Data."),
        metadata=DatasetMetadata(persistent_identifier="https://doi.org/10.123/example"),
    )
    record = UMDatasetRecord(
        um_dataset_id="um-1",
        title="Different title",
        doi="10.123/example",
    )

    decision = match_mention_to_um_dataset(mention, [record])

    assert decision.status == MatchStatus.MATCHED
    assert decision.um_dataset_id == "um-1"
    assert decision.match_method == "exact_pid"


def test_match_possible_title_creator_year():
    mention = DatasetMention(
        publication_id="p2",
        dataset_name="Maastricht Health Survey",
        evidence=MentionEvidence(body_quote="The Maastricht Health Survey dataset was used."),
        metadata=DatasetMetadata(dataset_authors=["Jane Doe"], dataset_year=2024),
    )
    record = UMDatasetRecord(
        um_dataset_id="um-2",
        title="Maastricht Health Survey Data",
        creators=["Jane Doe"],
        year=2024,
    )

    decision = match_mention_to_um_dataset(mention, [record])

    assert decision.status == MatchStatus.POSSIBLE
    assert decision.review_required is True
    assert "title_or_alias" in decision.matched_fields


def test_exact_title_is_reviewable_without_fabricated_metadata():
    mention = DatasetMention(
        publication_id="p2",
        dataset_name="Maastricht Health Survey",
        evidence=MentionEvidence(body_quote="We used the Maastricht Health Survey dataset."),
    )
    record = UMDatasetRecord(
        um_dataset_id="um-2",
        title="Maastricht Health Survey",
    )

    decision = match_mention_to_um_dataset(mention, [record])

    assert decision.status == MatchStatus.POSSIBLE
    assert decision.match_method == "exact_title_or_alias"
    assert decision.review_required is True


def test_ambiguous_metadata_match_returns_all_candidate_ids_without_selection():
    mention = DatasetMention(
        publication_id="p3",
        dataset_name="Versioned Dataset",
        evidence=MentionEvidence(body_quote="We used the Versioned Dataset."),
        metadata=DatasetMetadata(dataset_authors=["Jane Doe"], dataset_year=2024),
    )
    records = [
        UMDatasetRecord(
            um_dataset_id="W1", title="Versioned Dataset", creators=["Jane Doe"], year=2024
        ),
        UMDatasetRecord(
            um_dataset_id="W2", title="Versioned Dataset", creators=["Jane Doe"], year=2024
        ),
    ]

    decision = match_mention_to_um_dataset(mention, records)

    assert decision.status == MatchStatus.REVIEW_REQUIRED
    assert decision.um_dataset_id is None
    assert decision.candidate_um_dataset_ids == ["W1", "W2"]
    assert decision.match_method == "ambiguous_exact_title_or_alias"
