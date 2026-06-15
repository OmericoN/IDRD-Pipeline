from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.application.pipeline_services import load_um_dataset_records


def test_load_um_openalex_export_directory(tmp_path):
    (tmp_path / "OPEN_ALEX_DATA.csv").write_text(
        "PURE_ID,OPAL_ID,URL,OPAL_DOI,OPAL_TITLE,OPAL_YEAR,OPAL_KEYWORDS,OPAL_PRIMARY_SOURCE_DISPLAY_NAME\n"
        "7,https://openalex.org/W123,https://example.org,https://doi.org/10.123/demo,Demo Dataset Work,2024,cohort;biobank,Dataverse\n",
        encoding="utf-8",
    )
    (tmp_path / "OPEN_ALEX_DATA_KEYWORDS.csv").write_text(
        "OPENALEX_ID,KEYWORD,SCORE\n"
        "https://openalex.org/W123,genomics,0.8\n",
        encoding="utf-8",
    )
    (tmp_path / "OPEN_ALEX_DATA_TOPICS.csv").write_text(
        "OPENALEX_ID,TOPIC,TOPIC_ID,SCORE,TOPIC_PRIMARY\n"
        "https://openalex.org/W123,Health data,T42,0.9,1\n",
        encoding="utf-8",
    )
    (tmp_path / "OPEN_ALEX_DATA_MESHS.csv").write_text(
        "OPENALEX_ID,MESH,QUALIFIER\n"
        "https://openalex.org/W123,Humans,analysis\n",
        encoding="utf-8",
    )
    (tmp_path / "OPEN_ALEX_DATA_RELATED_WORKS.csv").write_text(
        "OPENALEX_ID,RELATED_WORK_ID\n"
        "https://openalex.org/W123,https://openalex.org/W999\n",
        encoding="utf-8",
    )

    records = load_um_dataset_records(str(tmp_path))

    assert len(records) == 1
    record = records[0]
    assert record.um_dataset_id == "W123"
    assert record.title == "Demo Dataset Work"
    assert record.doi == "https://doi.org/10.123/demo"
    assert record.year == 2024
    assert record.repository == "Dataverse"
    assert record.keywords == ["cohort", "biobank", "genomics"]
    assert record.raw["openalex"]["topics"][0]["topic_id"] == "T42"
    assert record.raw["openalex"]["mesh"][0]["mesh"] == "Humans"
    assert record.raw["openalex"]["related_works"] == ["https://openalex.org/W999"]

