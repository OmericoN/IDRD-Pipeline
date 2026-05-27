from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from idrd.pipeline.services import load_um_dataset_records


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
