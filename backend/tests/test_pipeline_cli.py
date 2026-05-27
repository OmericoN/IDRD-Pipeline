from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from idrd.domain.stages import PipelineStage, stage_values
from idrd.interfaces.cli.main import build_parser


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
    args = build_parser().parse_args(["enqueue", PipelineStage.DISCOVER, "--query", "dataset reuse"])
    assert args.command == "enqueue"
    assert args.stage == "discover"
    assert args.query == "dataset reuse"


def test_cli_parses_doctor():
    args = build_parser().parse_args(["doctor"])
    assert args.command == "doctor"


def test_cli_parses_import_um_datasets():
    args = build_parser().parse_args(["import-um-datasets", "--path", "um.json"])
    assert args.command == "import-um-datasets"
    assert args.path == "um.json"


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
        ]
    )
    assert args.command == "run-all"
    assert args.mode == "guided"
    assert args.limit == 5
    assert args.um_datasets == "um.csv"
    assert args.output == "storage/exports/insights.csv"
