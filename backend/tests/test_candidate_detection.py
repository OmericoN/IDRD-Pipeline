from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.domain.candidate_detection import detect_dataset_candidates, standardize_section


def test_detect_named_dataset_candidate_in_methodology():
    markdown = """# Paper

## 2. Materials and Methods

The UK Biobank dataset was used to validate the model [12].
"""
    candidates = detect_dataset_candidates("p1", markdown)

    assert len(candidates) == 1
    assert candidates[0].dataset_name == "UK Biobank"
    assert candidates[0].standardized_section == "Methodology"
    assert candidates[0].score >= 0.5


def test_detect_implicit_author_described_dataset():
    markdown = """## Data collection

We collected survey responses from 500 participants during 2024.
"""
    candidates = detect_dataset_candidates("p2", markdown)

    assert len(candidates) == 1
    assert candidates[0].dataset_name == "Author-described dataset"


def test_standardize_section_fallbacks():
    assert standardize_section("Data Availability Statement") == "Data Availability"
    assert standardize_section("Results") == "Results"
    assert standardize_section(None) is None
