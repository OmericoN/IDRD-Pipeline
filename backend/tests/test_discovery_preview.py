from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.application import discovery_preview
from datasight.domain.schemas import UMDatasetRecord
from datasight.infrastructure.pubfetcher.openalex import OpenAlexSearchResult


class FakeRepository:
    saved_preview: dict | None = None
    records: list[UMDatasetRecord] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return None

    def list_um_dataset_records(self):
        return self.records

    def list_topic_resolution_cache(self, catalog_fingerprint):
        return {}

    def save_topic_resolutions(self, catalog_fingerprint, requested_names, resolved):
        return None

    def save_discovery_preview(self, **kwargs):
        type(self).saved_preview = kwargs


class FakeOpenAlexClient:
    api_key = "test-key"
    calls: list[dict] = []
    work_count = 125

    def search_works_with_meta(self, **kwargs):
        type(self).calls.append(kwargs)
        works = [
            {
                "paperId": f"W{900 + index}",
                "title": f"Candidate {index}",
                "open_access_url": f"https://example.org/{index}.pdf",
                "raw": {},
            }
            for index in range(self.work_count)
        ]
        return OpenAlexSearchResult(
            works=works,
            total_count=len(works),
            cost_usd=0.001 if kwargs.get("query") else 0.0001,
            calls=1,
            rate_limit={},
        )

    def list_entities_with_meta(self, *args, **kwargs):
        raise AssertionError("Topic resolution should be lazy")


def test_manual_preview_returns_every_retained_candidate(monkeypatch):
    FakeRepository.records = []
    FakeRepository.saved_preview = None
    FakeOpenAlexClient.calls = []
    FakeOpenAlexClient.work_count = 125
    monkeypatch.setattr(discovery_preview, "PipelineRepository", FakeRepository)
    monkeypatch.setattr(discovery_preview, "OpenAlexClient", FakeOpenAlexClient)

    preview = discovery_preview.create_discovery_preview(
        {
            "strategy_version": 2,
            "mode": "manual",
            "manual_query": "dataset reuse",
            "focus_query": "",
            "discovery_limit": 125,
            "processing_limit": 125,
            "publication_types": [],
            "max_cost_usd": 0.25,
        }
    )

    assert preview["candidate_count"] == 125
    assert preview["ready_count"] == 125
    assert preview["watchlist_count"] == 0
    assert preview["stop_reason"] == "ready_target_met"
    assert preview["completed_phases"] == ["manual"]
    assert all(candidate["evidence_tier"] == "expanded" for candidate in preview["candidates"])
    assert FakeRepository.saved_preview is not None


def test_random_preview_uses_seeded_sample_without_catalog(monkeypatch):
    FakeRepository.records = []
    FakeRepository.saved_preview = None
    FakeOpenAlexClient.calls = []
    FakeOpenAlexClient.work_count = 125
    monkeypatch.setattr(discovery_preview, "PipelineRepository", FakeRepository)
    monkeypatch.setattr(discovery_preview, "OpenAlexClient", FakeOpenAlexClient)

    preview = discovery_preview.create_discovery_preview(
        {
            "strategy_version": 2,
            "mode": "random",
            "focus_query": "",
            "manual_query": None,
            "random_seed": 42,
            "discovery_limit": 125,
            "processing_limit": 50,
            "from_year": 2020,
            "to_year": 2025,
            "publication_types": ["article"],
            "max_cost_usd": 0.25,
        }
    )

    assert preview["random_seed"] == 42
    assert preview["completed_phases"] == ["random"]
    assert preview["stop_reason"] == "ready_target_met"
    assert preview["candidate_count"] == 125
    assert all(candidate["evidence_reasons"] == ["random_sample"] for candidate in preview["candidates"])
    assert FakeOpenAlexClient.calls == [
        {
            "query": None,
            "limit": 125,
            "filters": {"publication_year": "2020-2025", "type": ["article"]},
            "sort": None,
            "search_mode": "search",
            "sample_size": 125,
            "sample_seed": 42,
            "max_cost_usd": 0.25,
        }
    ]


def test_catalog_funnel_completes_direct_phase_then_stops_before_exact(monkeypatch):
    FakeRepository.records = [
        UMDatasetRecord(
            um_dataset_id="W1",
            title="Distinctive Maastricht longitudinal research dataset",
            doi="10.1234/um.1",
            raw={"openalex": {"id": "W1", "related_works": ["W2"]}},
        )
    ]
    FakeOpenAlexClient.calls = []
    FakeOpenAlexClient.work_count = 1
    monkeypatch.setattr(discovery_preview, "PipelineRepository", FakeRepository)
    monkeypatch.setattr(discovery_preview, "OpenAlexClient", FakeOpenAlexClient)

    preview = discovery_preview.create_discovery_preview(
        {
            "strategy_version": 2,
            "mode": "catalog_funnel",
            "focus_query": "must not narrow direct",
            "manual_query": None,
            "discovery_limit": 10,
            "processing_limit": 1,
            "publication_types": [],
            "max_cost_usd": 0.25,
        }
    )

    assert len(FakeOpenAlexClient.calls) == 2
    assert all(call["query"] is None for call in FakeOpenAlexClient.calls)
    assert preview["completed_phases"] == ["direct"]
    assert preview["stop_reason"] == "ready_target_met"
    assert set(preview["phase_results"]) == {"direct"}


def test_cost_ceiling_retains_partial_direct_results(monkeypatch):
    FakeRepository.records = [
        UMDatasetRecord(
            um_dataset_id="W1",
            title="Distinctive Maastricht longitudinal research dataset",
            raw={"openalex": {"id": "W1"}},
        )
    ]
    FakeOpenAlexClient.calls = []
    FakeOpenAlexClient.work_count = 1
    monkeypatch.setattr(discovery_preview, "PipelineRepository", FakeRepository)
    monkeypatch.setattr(discovery_preview, "OpenAlexClient", FakeOpenAlexClient)

    preview = discovery_preview.create_discovery_preview(
        {
            "strategy_version": 2,
            "mode": "catalog_funnel",
            "focus_query": "",
            "manual_query": None,
            "discovery_limit": 10,
            "processing_limit": 10,
            "publication_types": [],
            "max_cost_usd": 0.00015,
        }
    )

    assert len(FakeOpenAlexClient.calls) == 1
    assert preview["candidate_count"] == 1
    assert preview["partial"] is True
    assert preview["stop_reason"] == "cost_ceiling"
    assert preview["phase_results"]["direct"]["status"] == "partial"
