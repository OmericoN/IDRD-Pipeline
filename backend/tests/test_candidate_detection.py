from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.domain.candidate_detection import detect_dataset_candidates, standardize_section
from datasight.domain.schemas import UMDatasetRecord


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


def test_ignores_generic_discussion_of_survey_responses():
    markdown = "Survey responses can be influenced by question wording."

    assert detect_dataset_candidates("p3", markdown) == []


def test_standardize_section_fallbacks():
    assert standardize_section("Data Availability Statement") == "Data Availability"
    assert standardize_section("Results") == "Results"
    assert standardize_section(None) is None


def test_accessions_repository_and_availability_phrases_are_transparent():
    markdown = """## Data Availability

The sequencing data are available in the Sequence Read Archive under accession PRJNA123456.
"""
    candidate = detect_dataset_candidates("p4", markdown, render_sha256="abc")[0]
    assert candidate.evidence_tier == "strong"
    assert candidate.dataset_name == "PRJNA123456"
    assert candidate.detector_version == "rules-v3"
    assert candidate.render_sha256 == "abc"
    assert {trigger["type"] for trigger in candidate.triggers} >= {
        "accession",
        "repository_name",
    }
    assert markdown[candidate.char_start:candidate.char_end] == candidate.evidence_text


def test_catalog_title_alias_identifier_and_url_are_strong_exact_triggers():
    record = UMDatasetRecord(
        um_dataset_id="W-DATA",
        title="Maastricht Study Dataset",
        aliases=["TMS cohort"],
        doi="10.1234/um.dataset",
        url="https://data.example/um-study",
    )
    markdown = "The TMS cohort was accessed for secondary analysis."
    candidate = detect_dataset_candidates("p5", markdown, catalog_records=[record])[0]
    assert candidate.dataset_name == "Maastricht Study Dataset"
    assert candidate.trigger_type == "um_catalog_exact"
    assert candidate.evidence_tier == "strong"


def test_bare_dataset_terms_remain_broad_and_unresolved():
    candidate = detect_dataset_candidates("p6", "A benchmark was constructed for evaluation.")[0]
    assert candidate.evidence_tier == "broad"
    assert candidate.dataset_name == "Unresolved dataset mention"


def test_literature_database_and_database_server_are_not_generic_triggers():
    text = "The literature database server was restarted before the search."
    assert detect_dataset_candidates("p7", text) == []


def test_long_paragraph_uses_trigger_centered_merged_windows():
    prefix = "Background material without evidence. " * 40
    middle = "The cohort data were retrieved from Dryad at https://datadryad.org/stash/dataset/doi:10.1/example. "
    suffix = "Unrelated interpretation follows. " * 40
    markdown = f"## Methods\n\n{prefix}{middle}{suffix}"
    candidates = detect_dataset_candidates("p8", markdown)
    assert len(candidates) == 1
    candidate = candidates[0]
    assert len(candidate.evidence_text) < len(markdown) * 0.6
    assert "Dryad" in candidate.evidence_text
    assert markdown[candidate.char_start:candidate.char_end] == candidate.evidence_text


def test_multiline_table_passage_and_overlapping_triggers_merge():
    markdown = """## Data

| Repository | Accession |
| --- | --- |
| GEO | GSE123456 |
"""
    candidates = detect_dataset_candidates("p9", markdown)
    assert len(candidates) == 1
    assert candidates[0].evidence_tier == "strong"
