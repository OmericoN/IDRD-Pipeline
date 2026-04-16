from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from main import IDRDPipeline
from models.results import DownloadResult, ConversionResult


def test_download_result_status_classification():
    ok = DownloadResult(paper_id="p1", success=True, message="Downloaded: p1.pdf")
    skipped = DownloadResult(paper_id="p2", success=True, message="Already exists: p2.pdf")
    failed = DownloadResult(paper_id="p3", success=False, message="Request error", error="Request error")

    assert IDRDPipeline._download_result_status(ok) == "successful"
    assert IDRDPipeline._download_result_status(skipped) == "skipped"
    assert IDRDPipeline._download_result_status(failed) == "failed"


def test_convert_result_status_classification():
    ok = ConversionResult(paper_id="p1", success=True, message="Converted: p1.tei.xml")
    skipped = ConversionResult(paper_id="p2", success=True, message="Already converted: p2.tei.xml")
    failed = ConversionResult(paper_id="p3", success=False, message="GROBID error", error="boom")

    assert IDRDPipeline._convert_result_status(ok) == "successful"
    assert IDRDPipeline._convert_result_status(skipped) == "skipped"
    assert IDRDPipeline._convert_result_status(failed) == "failed"


def test_render_result_status_classification():
    assert IDRDPipeline._render_result_status(True, "Rendered: p1.md") == "successful"
    assert IDRDPipeline._render_result_status(True, "Already exists: p2.md") == "skipped"
    assert IDRDPipeline._render_result_status(False, "Error") == "failed"
