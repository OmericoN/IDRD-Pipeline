from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.domain.discovery import (
    DiscoveryOptions,
    build_discovery_profile,
    build_openalex_discovery_queries,
    dedupe_and_score_publications,
)
from datasight.domain.schemas import UMDatasetRecord


def test_build_discovery_profile_from_um_openalex_metadata():
    record = UMDatasetRecord(
        um_dataset_id="W123",
        title="Demo Dataset",
        keywords=["cohort"],
        raw={
            "openalex": {
                "id": "https://openalex.org/W123",
                "topics": [{"id": "https://openalex.org/T42", "display_name": "Health data"}],
                "keywords": [{"keyword": "biobank"}],
                "mesh": [{"mesh": "Humans"}],
                "concepts": [{"concept": "Epidemiology"}],
                "related_works": ["https://openalex.org/W999"],
            }
        },
    )

    profile = build_discovery_profile([record])

    assert profile.topic_ids == ("T42",)
    assert "cohort" in profile.keyword_terms
    assert "biobank" in profile.keyword_terms
    assert profile.mesh_terms == ("Humans",)
    assert profile.related_work_ids == ("W999",)


def test_build_openalex_discovery_queries_expands_topics_and_terms():
    profile = build_discovery_profile(
        [
            UMDatasetRecord(
                um_dataset_id="W123",
                title="Demo Dataset",
                keywords=["cohort"],
                raw={"openalex": {"topics": [{"topic_id": "T42"}]}},
            )
        ]
    )
    options = DiscoveryOptions(keyword_terms=("survey",), from_year=2020, to_year=2024)

    queries = build_openalex_discovery_queries("Maastricht data", options, profile, open_access_only=True)

    assert queries[0].filters == {"open_access.is_oa": True, "publication_year": "2020-2024"}
    assert any(query.reason == "topic_profile" and query.filters["topics.id"] == ("T42",) for query in queries)
    assert any(query.reason == "term_profile" and "survey" in str(query.query) for query in queries)


def test_dedupe_and_score_publications_prefers_profile_overlap():
    options = DiscoveryOptions(topic_ids=("T42",), keyword_terms=("cohort",))
    publications = [
        {
            "paperId": "W1",
            "title": "Unrelated",
            "raw": {"topics": [], "keywords": []},
            "cited_by_count": 1,
        },
        {
            "paperId": "W1",
            "title": "Maastricht cohort reuse",
            "open_access_url": "https://example.org/a.pdf",
            "has_fulltext": True,
            "raw": {
                "topics": [{"id": "https://openalex.org/T42"}],
                "keywords": [{"display_name": "cohort"}],
            },
            "cited_by_count": 100,
        },
    ]

    scored = dedupe_and_score_publications(publications, "Maastricht cohort", options)

    assert len(scored) == 1
    assert scored[0]["title"] == "Maastricht cohort reuse"
    assert scored[0]["discovery_score"] > 0.7

