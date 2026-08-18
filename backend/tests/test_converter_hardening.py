from __future__ import annotations

from pathlib import Path

import pytest
import requests

from datasight.infrastructure.ingestion.converter import GrobidConverter, TEIValidationError, validate_tei


VALID_TEI = b"""<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0"><text><body><div><p>Body evidence.</p></div></body></text></TEI>"""


class FakeResponse:
    def __init__(self, content: bytes = VALID_TEI, status_code: int = 200):
        self.content = content
        self.status_code = status_code


def _pdf(tmp_path: Path) -> Path:
    path = tmp_path / "paper.pdf"
    path.write_bytes(b"source pdf")
    return path


def test_validates_and_atomically_publishes_tei(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: FakeResponse())
    result = GrobidConverter(output_dir=tmp_path).convert_pdf(_pdf(tmp_path), "W1", delete_pdf=True)
    assert result.success
    assert result.sha256 and result.source_sha256
    assert result.quality_metrics and result.quality_metrics["body_characters"] > 0
    assert result.warnings and "retain" in result.warnings[0]
    assert (tmp_path / "paper.pdf").exists()
    assert list(tmp_path.glob("*.part")) == []


@pytest.mark.parametrize(
    "content",
    [
        b"<html><body>not TEI</body></html>",
        b"<TEI",
        b'<TEI xmlns="http://www.tei-c.org/ns/1.0"><text><body/></text></TEI>',
    ],
)
def test_rejects_non_tei_malformed_and_empty_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, content: bytes
):
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: FakeResponse(content))
    result = GrobidConverter(output_dir=tmp_path).convert_pdf(_pdf(tmp_path), "bad")
    assert not result.success
    assert result.failure_category == "invalid_tei"
    assert not (tmp_path / "bad.tei.xml").exists()
    assert list(tmp_path.glob("*.part")) == []


def test_timeout_is_stable_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def timeout(*args, **kwargs):
        raise requests.Timeout("slow")

    monkeypatch.setattr(requests, "post", timeout)
    result = GrobidConverter(output_dir=tmp_path).convert_pdf(_pdf(tmp_path), "timeout")
    assert not result.success
    assert result.failure_category == "timeout"


def test_cache_reuse_requires_hash_source_and_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    calls = 0

    def post(*args, **kwargs):
        nonlocal calls
        calls += 1
        return FakeResponse()

    monkeypatch.setattr(requests, "post", post)
    converter = GrobidConverter(output_dir=tmp_path)
    first = converter.convert_pdf(_pdf(tmp_path), "cached")
    second = converter.convert_pdf(tmp_path / "paper.pdf", "cached")
    assert first.success and second.success
    assert calls == 1
    assert second.quality_metrics and second.quality_metrics["cache_reused"]

    (tmp_path / "paper.pdf").write_bytes(b"changed source")
    third = converter.convert_pdf(tmp_path / "paper.pdf", "cached")
    assert third.success
    assert calls == 2


def test_validate_tei_rejects_malformed_file(tmp_path: Path):
    path = tmp_path / "broken.xml"
    path.write_text("<TEI", encoding="utf-8")
    with pytest.raises(TEIValidationError):
        validate_tei(path)


def test_availability_missing_pdf_and_http_failure_are_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResponse(status_code=200))
    converter = GrobidConverter(output_dir=tmp_path)
    converter.ensure_available(timeout=1)

    missing = converter.convert_pdf(tmp_path / "missing.pdf", "missing")
    assert missing.failure_category == "missing_pdf"

    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: FakeResponse(status_code=503))
    http_failure = converter.convert_pdf(_pdf(tmp_path), "http")
    assert http_failure.failure_category == "grobid_http"
