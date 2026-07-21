from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.domain.stages import PipelineStage, stage_values
from datasight.interfaces.cli.main import build_parser


def test_stage_values_are_canonical_order():
    assert stage_values() == [
        "discover",
        "download_pdf",
        "grobid_convert",
        "render_document",
        "detect_mentions",
        "extract_features",
        "match_um_dataset",
        "export_insights",
    ]


def test_cli_parses_enqueue_discover():
    args = build_parser().parse_args(
        [
            "enqueue",
            PipelineStage.DISCOVER,
            "--query",
            "dataset reuse",
            "--topic-id",
            "T123",
            "--keyword-term",
            "biobank",
            "--use-um-profile",
        ]
    )
    assert args.command == "enqueue"
    assert args.stage == "discover"
    assert args.query == "dataset reuse"
    assert args.topic_ids == ["T123"]
    assert args.keyword_terms == ["biobank"]
    assert args.use_um_profile is True


def test_cli_parses_doctor():
    args = build_parser().parse_args(["doctor"])
    assert args.command == "doctor"


def test_cli_parses_import_um_datasets():
    args = build_parser().parse_args(["import-um-datasets", "--path", "um.json"])
    assert args.command == "import-um-datasets"
    assert args.path == "um.json"


def test_cli_defaults_to_catalog_profile_and_supports_opt_out():
    defaults = build_parser().parse_args(
        ["run-all", "--query", "reuse", "--output", "storage/exports/insights.csv"]
    )
    opted_out = build_parser().parse_args(
        [
            "run-all",
            "--query",
            "reuse",
            "--output",
            "storage/exports/insights.csv",
            "--no-use-um-profile",
        ]
    )

    assert defaults.um_datasets == "data/um_dataset"
    assert defaults.use_um_profile is True
    assert opted_out.use_um_profile is False


def test_cli_parses_guided_run_all():
    args = build_parser().parse_args(
        [
            "run-all",
            "--query",
            "Maastricht dataset reuse",
            "--limit",
            "5",
            "--um-datasets",
            "um.csv",
            "--output",
            "storage/exports/insights.csv",
            "--use-um-profile",
        ]
    )
    assert args.command == "run-all"
    assert args.mode == "guided"
    assert args.limit == 5
    assert args.um_datasets == "um.csv"
    assert args.output == "storage/exports/insights.csv"
    assert args.use_um_profile is True
