"""Atomic PDF-to-TEI conversion through an already-running GROBID service."""

from __future__ import annotations

import logging
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import lxml.etree as etree
import requests

from datasight.config import (
    CONVERSION_DELAY_SEC,
    GROBID_ALIVE_CHECK_TIMEOUT_SEC,
    GROBID_BASE_URL,
    GROBID_CONVERSION_TIMEOUT_SEC,
    GROBID_STARTUP_TIMEOUT_SEC,
    PDF_DIR,
    XML_DIR,
)
from datasight.domain.results import ConversionResult
from datasight.infrastructure.ingestion.file_integrity import sha256_file

logger = logging.getLogger(__name__)
TEI_NS = "http://www.tei-c.org/ns/1.0"
GROBID_PRODUCER_VERSION = "grobid-tei-v2"


class TEIValidationError(ValueError):
    pass


def validate_tei(path: str | Path) -> dict[str, int]:
    try:
        tree = etree.parse(str(path), parser=etree.XMLParser(resolve_entities=False, no_network=True))
    except (OSError, etree.XMLSyntaxError) as exc:
        raise TEIValidationError(f"Malformed TEI XML: {exc}") from exc
    root = tree.getroot()
    if etree.QName(root).localname != "TEI":
        raise TEIValidationError("GROBID response is not a TEI document")
    body = root.find(f".//{{{TEI_NS}}}text/{{{TEI_NS}}}body")
    body_text = "" if body is None else "".join(body.itertext()).strip()
    if not body_text:
        raise TEIValidationError("TEI document has an empty body")
    return {
        "body_characters": len(body_text),
        "figures": len(root.findall(f".//{{{TEI_NS}}}figure")),
        "tables": len(root.findall(f".//{{{TEI_NS}}}figure[@type='table']")),
    }


class GrobidConverter:
    def __init__(
        self,
        pdf_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        grobid_base_url: str = GROBID_BASE_URL,
        delay: float = CONVERSION_DELAY_SEC,
    ):
        self.pdf_dir = Path(pdf_dir) if pdf_dir else PDF_DIR
        self.output_dir = Path(output_dir) if output_dir else XML_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.grobid_base_url = grobid_base_url.rstrip("/")
        self.delay = delay

    def ensure_available(self, timeout: int = GROBID_STARTUP_TIMEOUT_SEC) -> None:
        start = time.time()
        while time.time() - start < timeout:
            try:
                response = requests.get(f"{self.grobid_base_url}/api/isalive", timeout=GROBID_ALIVE_CHECK_TIMEOUT_SEC)
                if response.status_code == 200:
                    return
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)
        raise RuntimeError(f"GROBID is not reachable at {self.grobid_base_url}")

    def _failure(self, paper_id: str, pdf_path: Path, category: str, message: str) -> ConversionResult:
        return ConversionResult(
            paper_id=paper_id,
            success=False,
            message=message,
            pdf_path=pdf_path,
            error=message,
            failure_category=category,
            producer_version=GROBID_PRODUCER_VERSION,
            warnings=[],
            quality_metrics={},
        )

    def convert_pdf(
        self,
        pdf_path: Path,
        paper_id: str | None = None,
        overwrite: bool = False,
        delete_pdf: bool = False,
    ) -> ConversionResult:
        pdf_path = Path(pdf_path)
        paper_id = paper_id or pdf_path.stem
        output_path = self.output_dir / f"{paper_id}.tei.xml"
        metadata_path = output_path.with_suffix(output_path.suffix + ".meta.json")
        if not pdf_path.exists():
            return self._failure(paper_id, pdf_path, "missing_pdf", f"PDF not found: {pdf_path}")

        source_hash = sha256_file(pdf_path)
        if output_path.exists() and not overwrite:
            try:
                metrics = validate_tei(output_path)
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                artifact_hash = sha256_file(output_path)
                if not (
                    isinstance(metadata, dict)
                    and metadata.get("producer_version") == GROBID_PRODUCER_VERSION
                    and metadata.get("source_sha256") == source_hash
                    and metadata.get("sha256") == artifact_hash
                ):
                    raise TEIValidationError("Cached TEI lineage does not match its source/version")
                return ConversionResult(
                    paper_id=paper_id,
                    success=True,
                    message=f"Reused validated TEI: {output_path.name}",
                    xml_path=output_path,
                    pdf_path=pdf_path,
                    xml_size_bytes=output_path.stat().st_size,
                    sha256=artifact_hash,
                    source_sha256=source_hash,
                    producer_version=GROBID_PRODUCER_VERSION,
                    warnings=[],
                    quality_metrics={**metrics, "cache_reused": True},
                )
            except (TEIValidationError, OSError, json.JSONDecodeError):
                pass

        temp_path: Path | None = None
        try:
            with pdf_path.open("rb") as handle:
                response = requests.post(
                    f"{self.grobid_base_url}/api/processFulltextDocument",
                    files={"input": handle},
                    timeout=GROBID_CONVERSION_TIMEOUT_SEC,
                )
            if response.status_code != 200:
                return self._failure(paper_id, pdf_path, "grobid_http", f"GROBID HTTP {response.status_code}")

            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=f".{output_path.name}.", suffix=".part", dir=self.output_dir, delete=False
            ) as temp:
                temp_path = Path(temp.name)
                content = getattr(response, "content", None)
                if content is None:
                    content = str(getattr(response, "text", "")).encode("utf-8")
                temp.write(content)
                temp.flush()
                os.fsync(temp.fileno())
            metrics = validate_tei(temp_path)
            digest = sha256_file(temp_path)
            os.replace(temp_path, output_path)
            temp_path = None
            metadata_bytes = json.dumps(
                {
                    "sha256": digest,
                    "source_sha256": source_hash,
                    "producer_version": GROBID_PRODUCER_VERSION,
                    "quality_metrics": metrics,
                },
                indent=2,
                sort_keys=True,
            ).encode("utf-8")
            metadata_temp: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb", prefix=f".{metadata_path.name}.", suffix=".part",
                    dir=self.output_dir, delete=False,
                ) as handle:
                    metadata_temp = Path(handle.name)
                    handle.write(metadata_bytes)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(metadata_temp, metadata_path)
                metadata_temp = None
            finally:
                if metadata_temp is not None:
                    metadata_temp.unlink(missing_ok=True)
            # Source PDFs are deliberately retained for experiment auditability.
            warnings = ["delete_pdf ignored: experimental runs retain source PDFs"] if delete_pdf else []
            return ConversionResult(
                paper_id=paper_id,
                success=True,
                message=f"Converted and validated: {output_path.name}",
                xml_path=output_path,
                pdf_path=pdf_path,
                xml_size_bytes=output_path.stat().st_size,
                sha256=digest,
                source_sha256=source_hash,
                producer_version=GROBID_PRODUCER_VERSION,
                warnings=warnings,
                quality_metrics={**metrics, "cache_reused": False},
            )
        except TEIValidationError as exc:
            return self._failure(paper_id, pdf_path, "invalid_tei", str(exc))
        except requests.exceptions.Timeout as exc:
            return self._failure(paper_id, pdf_path, "timeout", f"GROBID timeout: {exc}")
        except requests.exceptions.RequestException as exc:
            return self._failure(paper_id, pdf_path, "request_error", f"GROBID request error: {exc}")
        except OSError as exc:
            return self._failure(paper_id, pdf_path, "file_error", f"TEI file error: {exc}")
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    def convert_papers(
        self,
        papers: list[dict[str, Any]],
        paper_id_key: str = "paperId",
        pdf_path_key: str = "pdf_path",
        overwrite: bool = False,
        delete_pdf: bool = False,
    ) -> list[ConversionResult]:
        if not papers:
            return []
        self.ensure_available()
        results: list[ConversionResult] = []
        for paper in papers:
            pdf_path = paper.get(pdf_path_key)
            if not pdf_path:
                results.append(self._failure(paper.get(paper_id_key, "unknown"), Path(), "missing_pdf", "Missing PDF path"))
                continue
            results.append(
                self.convert_pdf(
                    Path(pdf_path),
                    paper_id=paper.get(paper_id_key),
                    overwrite=overwrite,
                    delete_pdf=delete_pdf,
                )
            )
            if self.delay > 0:
                time.sleep(self.delay)
        logger.info("Converted %s/%s PDFs", sum(result.success for result in results), len(results))
        return results
