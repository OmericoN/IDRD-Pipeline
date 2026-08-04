from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.application.pipeline_services import (
    _mention_from_candidate,
    _result_from_operation,
    load_um_dataset_records,
)
from datasight.domain.schemas import DatasetRole, ReferenceDirectness
from datasight.domain.stages import PipelineStage


def test_load_um_dataset_records_from_csv(tmp_path):
    csv_path = tmp_path / "um.csv"
    csv_path.write_text(
        "um_dataset_id,title,aliases,creators,year\n"
        "um-1,Maastricht Health Survey,MHS;Health Survey,Jane Doe;John Doe,2024\n",
        encoding="utf-8",
    )

    records = load_um_dataset_records(str(csv_path))

    assert len(records) == 1
    assert records[0].um_dataset_id == "um-1"
    assert records[0].aliases == ["MHS", "Health Survey"]
    assert records[0].creators == ["Jane Doe", "John Doe"]


def test_batch_result_reports_partial_failures():
    result = _result_from_operation(
        PipelineStage.DOWNLOAD_PDF,
        [SimpleNamespace(success=True), SimpleNamespace(success=False)],
    )

    assert result["status"] == "completed_with_errors"
    assert result["payload"]["successful"] == 1
    assert result["payload"]["failed"] == 1


def test_rule_feature_extraction_populates_matchable_metadata():
    mention = _mention_from_candidate(
        {
            "publication_id": "p1",
            "dataset_name": "Maastricht Health Survey",
            "evidence_text": (
                "We used the Maastricht Health Survey dataset (2024), "
                "available at https://doi.org/10.1234/example."
            ),
            "char_start": 10,
            "char_end": 120,
            "score": 0.8,
        }
    )

    assert mention.metadata.persistent_identifier == "10.1234/example"
    assert str(mention.metadata.dataset_url) == "https://doi.org/10.1234/example"
    assert mention.metadata.dataset_year == 2024
    assert mention.dataset_role == DatasetRole.USED
    assert mention.reference_directness == ReferenceDirectness.DIRECT
    assert mention.provenance.prompt_version == "rules-v2"
