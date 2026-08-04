from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.application import um_catalog
from datasight.domain.schemas import UMDatasetRecord
from datasight.infrastructure.ingestion.openalex_exports import UMOpenAlexImportBundle


class FakeRepo:
    records: list[UMDatasetRecord] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def list_um_dataset_records(self):
        return self.records


def test_verification_reports_exact_catalog_match(monkeypatch):
    record = UMDatasetRecord(um_dataset_id="W1", title="Dataset", keywords=["health"], raw={"a": 1})
    FakeRepo.records = [record]
    monkeypatch.setattr(um_catalog, "PipelineRepository", FakeRepo)
    monkeypatch.setattr(um_catalog, "looks_like_openalex_export", lambda path: True)
    monkeypatch.setattr(
        um_catalog,
        "load_um_openalex_export_bundle",
        lambda path: UMOpenAlexImportBundle([record], ["source warning"], {"source_rows": 1}),
    )

    result = um_catalog.verify_um_dataset_catalog("data/um_dataset")

    assert result["status"] == "verified"
    assert result["verified_count"] == 1
    assert result["issues"] == []
    assert result["warnings"] == ["source warning"]


def test_verification_reports_missing_unexpected_and_changed_records(monkeypatch):
    source_records = [
        UMDatasetRecord(um_dataset_id="W1", title="Expected title"),
        UMDatasetRecord(um_dataset_id="W2", title="Missing dataset"),
    ]
    FakeRepo.records = [
        UMDatasetRecord(um_dataset_id="W1", title="Stored title"),
        UMDatasetRecord(um_dataset_id="W3", title="Unexpected dataset"),
    ]
    monkeypatch.setattr(um_catalog, "PipelineRepository", FakeRepo)
    monkeypatch.setattr(um_catalog, "looks_like_openalex_export", lambda path: True)
    monkeypatch.setattr(
        um_catalog,
        "load_um_openalex_export_bundle",
        lambda path: UMOpenAlexImportBundle(source_records, [], {"source_rows": 2}),
    )

    result = um_catalog.verify_um_dataset_catalog()

    assert result["status"] == "mismatch"
    assert result["verified_count"] == 0
    issues = {issue["um_dataset_id"]: issue for issue in result["issues"]}
    assert issues["W1"]["status"] == "changed"
    assert issues["W1"]["changed_fields"] == ["title"]
    assert issues["W2"]["status"] == "missing"
    assert issues["W3"]["status"] == "unexpected"


def test_verification_keeps_stored_catalog_available_when_source_is_invalid(monkeypatch):
    FakeRepo.records = [UMDatasetRecord(um_dataset_id="W1", title="Stored dataset")]
    monkeypatch.setattr(um_catalog, "PipelineRepository", FakeRepo)
    monkeypatch.setattr(um_catalog, "looks_like_openalex_export", lambda path: True)
    monkeypatch.setattr(
        um_catalog,
        "load_um_openalex_export_bundle",
        lambda path: (_ for _ in ()).throw(ValueError("Invalid source export")),
    )

    result = um_catalog.verify_um_dataset_catalog()

    assert result["status"] == "unavailable"
    assert result["stored_count"] == 1
    assert result["message"] == "Invalid source export"
