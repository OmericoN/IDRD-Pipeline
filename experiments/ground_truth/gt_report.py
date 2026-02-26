"""
GT Report
=========
Prints and saves a structured report of a ground-truth pipeline run.
Covers fetch, download, conversion, and extraction stages.

Saved to:  data/gt_experiment/report_<timestamp>.json
"""

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
from config import DATA_DIR

GT_EXPERIMENT_DIR = DATA_DIR / "gt_experiment"
GT_EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)


def build_report(
    found_with_pdf:     list[dict],
    found_no_pdf:       list[dict],
    not_found:          list[dict],
    download_results:   dict,
    conversion_results: dict | None,
    extraction_results: dict | None,        # ← added
    total_in_csv:       int,
    elapsed_seconds:    float,
) -> dict:
    """Assemble a structured report dict from all pipeline stage outputs."""

    dl_stats = download_results.get("stats", {})
    cv_stats = (conversion_results or {}).get("stats", {})
    ex_stats = (extraction_results or {}).get("stats", {})   # ← added

    report = {
        "run_timestamp":   datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed_seconds, 1),
        "totals": {
            "in_csv":         total_in_csv,
            "found_with_pdf": len(found_with_pdf),
            "found_no_pdf":   len(found_no_pdf),
            "not_found":      len(not_found),
            "pdf_downloaded": dl_stats.get("successful", 0),
            "pdf_failed":     dl_stats.get("failed",     0),
            "xml_converted":  cv_stats.get("successful", 0),
            "xml_failed":     cv_stats.get("failed",     0),
            "md_extracted":   ex_stats.get("successful", 0),  # ← added
            "md_failed":      ex_stats.get("failed",     0),  # ← added
        },
        "coverage": {},
        "found_with_pdf": [
            {
                "gt_id":    p.get("_gt_id"),
                "paper_id": p.get("paperId"),
                "title":    p.get("title"),
                "year":     p.get("year"),
                "pdf_url":  (p.get("openAccessPdf") or {}).get("url"),
            }
            for p in found_with_pdf
        ],
        "found_no_pdf": [
            {
                "gt_id":    p.get("_gt_id"),
                "paper_id": p.get("paperId"),
                "title":    p.get("title"),
                "year":     p.get("year"),
            }
            for p in found_no_pdf
        ],
        "not_found":          [{"gt_id": r["id"], "title": r["title"]} for r in not_found],
        "download_failures":  download_results.get("failed", []),
        "conversion_results": (conversion_results or {}).get("results", []),
        "extraction_results": (extraction_results or {}).get("results", []),  # ← added
    }

    # ── coverage percentages ──────────────────────────────────────────
    t = total_in_csv or 1
    report["coverage"] = {
        "ss_found_pct":      round((len(found_with_pdf) + len(found_no_pdf)) / t * 100, 1),
        "ss_with_pdf_pct":   round(len(found_with_pdf)                       / t * 100, 1),
        "pdf_download_pct":  round(dl_stats.get("successful", 0)             / t * 100, 1),
        "xml_converted_pct": round(cv_stats.get("successful", 0)             / t * 100, 1),
        "md_extracted_pct":  round(ex_stats.get("successful", 0)             / t * 100, 1),  # ← added
    }

    return report


def print_report(report: dict):
    t = report["totals"]
    c = report["coverage"]

    print(f"\n{'='*70}")
    print("GROUND TRUTH EXPERIMENT REPORT")
    print(f"{'='*70}")
    print(f"  Run at              : {report['run_timestamp']}")
    print(f"  Elapsed             : {report['elapsed_seconds']}s")
    print(f"{'─'*70}")
    print(f"  Papers in CSV       : {t['in_csv']}")
    print(f"  Found in SS         : {t['found_with_pdf'] + t['found_no_pdf']}  ({c['ss_found_pct']}%)")
    print(f"    ↳ with PDF URL    : {t['found_with_pdf']}  ({c['ss_with_pdf_pct']}%)")
    print(f"    ↳ no PDF URL      : {t['found_no_pdf']}")
    print(f"  Not in SS           : {t['not_found']}")
    print(f"{'─'*70}")
    print(f"  PDFs downloaded     : {t['pdf_downloaded']}  ({c['pdf_download_pct']}%)")
    print(f"  PDFs failed         : {t['pdf_failed']}")
    print(f"{'─'*70}")
    print(f"  XML converted       : {t['xml_converted']}  ({c['xml_converted_pct']}%)")
    print(f"  XML failed          : {t['xml_failed']}")
    print(f"{'─'*70}")
    print(f"  Markdown extracted  : {t['md_extracted']}  ({c['md_extracted_pct']}%)")   # ← added
    print(f"  Markdown failed     : {t['md_failed']}")                                   # ← added

    if report["not_found"]:
        print(f"\n  Not found in Semantic Scholar ({len(report['not_found'])}):")
        for r in report["not_found"]:
            print(f"    [{r['gt_id']}] {r['title'][:65]}")

    if report["found_no_pdf"]:
        print(f"\n  Found but no PDF ({len(report['found_no_pdf'])}):")
        for r in report["found_no_pdf"]:
            print(f"    [{r['gt_id']}] {r['title'][:65]}")

    if report["download_failures"]:
        print(f"\n  Download failures ({len(report['download_failures'])}):")
        for r in report["download_failures"]:
            print(f"    [{r.get('gt_id')}] {r.get('reason')} — {r.get('title','')[:50]}")

    # ← added: show extraction failures
    extraction_failures = [r for r in report.get("extraction_results", []) if not r.get("success")]
    if extraction_failures:
        print(f"\n  Extraction failures ({len(extraction_failures)}):")
        for r in extraction_failures:
            print(f"    {r.get('xml')} — {r.get('message','')[:60]}")

    print(f"{'='*70}")


def save_report(report: dict, output_dir: Path = GT_EXPERIMENT_DIR) -> Path:
    ts   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = output_dir / f"report_{ts}.json"
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"\n  Report saved → {path}")
    return path