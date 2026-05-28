from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.application import reset as reset_module


def test_generated_storage_reset_only_allows_storage_paths(tmp_path, monkeypatch):
    storage = tmp_path / "storage"
    generated = storage / "pdf"
    generated.mkdir(parents=True)
    (generated / "paper.pdf").write_text("content", encoding="utf-8")
    seed_file = tmp_path / "data" / "um_datasets.csv"
    seed_file.parent.mkdir()
    seed_file.write_text("um_dataset_id,title\n", encoding="utf-8")
    monkeypatch.setattr(reset_module, "STORAGE_DIR", storage)

    deleted = reset_module.reset_generated_storage((generated,))

    assert str(generated.resolve()) in deleted
    assert generated.exists()
    assert list(generated.iterdir()) == []
    assert seed_file.exists()


def test_generated_storage_reset_refuses_non_storage_paths(tmp_path, monkeypatch):
    storage = tmp_path / "storage"
    outside = tmp_path / "data"
    outside.mkdir()
    monkeypatch.setattr(reset_module, "STORAGE_DIR", storage)

    try:
        reset_module.reset_generated_storage((outside,))
    except ValueError as exc:
        assert "outside storage" in str(exc)
    else:
        raise AssertionError("Expected reset to reject paths outside storage.")
