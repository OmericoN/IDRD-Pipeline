"""
GT Fetcher
==========
Reads the ground truth CSV and attempts to fetch each paper
from Semantic Scholar by title, then by DOI as fallback.

Returns three buckets:
    - found_with_pdf   : SS has the paper AND an open-access PDF URL
    - found_no_pdf     : SS has the paper but no PDF URL
    - not_found        : SS has no record of the paper
"""

import csv
import sys
import time
import requests
from pathlib import Path
from typing import Optional

# ── resolve src/ on the path ─────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from config import (
    SEMANTIC_SCHOLAR_API_URL,
    SEMANTIC_SCHOLAR_API_KEY,
)

# ── constants ─────────────────────────────────────────────────────────
GT_CSV = Path(__file__).parent.parent.parent / "data" / "ground_truth" / "ground_truth.csv"

SS_FIELDS = ",".join([
    "paperId", "title", "abstract", "year",
    "authors", "citationCount", "referenceCount",
    "influentialCitationCount", "venue", "publicationDate",
    "publicationTypes", "journal", "fieldsOfStudy",
    "url", "externalIds", "isOpenAccess", "openAccessPdf", "tldr",
])

_HEADERS = {"x-api-key": SEMANTIC_SCHOLAR_API_KEY} if SEMANTIC_SCHOLAR_API_KEY else {}


# ── helpers ───────────────────────────────────────────────────────────

def _ss_get(endpoint: str, params: dict) -> Optional[dict]:
    """Single GET to Semantic Scholar with basic retry on 429."""
    url = f"{SEMANTIC_SCHOLAR_API_URL}/{endpoint}"
    for attempt in range(5):
        try:
            r = requests.get(url, params=params, headers=_HEADERS, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = 10 * (2 ** attempt)
                print(f"    rate-limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            print(f"    HTTP {r.status_code} — {r.text[:120]}")
            return None
        except requests.RequestException as e:
            print(f"    network error: {e}")
            time.sleep(5)
    return None


def _has_pdf(paper: dict) -> bool:
    oa = paper.get("openAccessPdf") or {}
    return bool(oa.get("url"))


def _fetch_by_title(title: str) -> Optional[dict]:
    """Search SS by title, return the best exact-match hit."""
    data = _ss_get("paper/search", {
        "query":  title,
        "fields": SS_FIELDS,
        "limit":  5,
    })
    if not data:
        return None

    for hit in data.get("data", []):
        if (hit.get("title") or "").strip().lower() == title.strip().lower():
            return hit

    # accept the top result as a fuzzy fall back if only one returned
    hits = data.get("data", [])
    return hits[0] if len(hits) == 1 else None


def _fetch_by_doi(doi: str) -> Optional[dict]:
    """Direct DOI lookup via /paper/{doi}."""
    return _ss_get(f"paper/{doi}", {"fields": SS_FIELDS})


# ── public API ────────────────────────────────────────────────────────

def load_csv(csv_path: Path = GT_CSV) -> list[dict]:
    """Return list of ground-truth row dicts."""
    rows = []
    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            pid   = row.get("Publication ID",    "").strip()
            title = row.get("Publication Title", "").strip()
            if pid and title:
                rows.append({
                    "id":     pid,
                    "title":  title,
                    "doi":    row.get("Publication DOI", "").strip() or None,
                    "type":   row.get("Publication Type",   "").strip(),
                    "year":   row.get("Publication Year",   "").strip(),
                    "authors":row.get("Publication Author", "").strip(),
                    "url":    row.get("Publication URL",    "").strip() or None,
                })
    return rows


def fetch_all(
    csv_path: Path = GT_CSV,
    delay: float = 0.2,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Fetch every ground-truth paper from Semantic Scholar.

    Returns:
        found_with_pdf  — paper found AND has open-access PDF URL
        found_no_pdf    — paper found BUT no PDF URL
        not_found       — paper not in Semantic Scholar at all
    """
    rows = load_csv(csv_path)
    found_with_pdf: list[dict] = []
    found_no_pdf:   list[dict] = []
    not_found:      list[dict] = []

    print(f"\n{'='*70}")
    print(f"GT FETCHER — {len(rows)} papers")
    print(f"{'='*70}")

    for i, gt in enumerate(rows, 1):
        print(f"  [{i:>3}/{len(rows)}] {gt['id']} — {gt['title'][:55]}...")

        # strategy 1 — title search
        paper = _fetch_by_title(gt["title"])

        # strategy 2 — DOI lookup
        if not paper and gt["doi"]:
            print(f"          title miss — trying DOI: {gt['doi']}")
            paper = _fetch_by_doi(gt["doi"])

        if not paper:
            print(f"          ✗ not found")
            not_found.append(gt)
        elif _has_pdf(paper):
            print(f"          ✓ found + PDF  [{paper.get('paperId')}]")
            paper["_gt_id"] = gt["id"]
            found_with_pdf.append(paper)
        else:
            print(f"          ~ found, no PDF [{paper.get('paperId')}]")
            paper["_gt_id"] = gt["id"]
            found_no_pdf.append(paper)

        time.sleep(delay)

    # ── summary ───────────────────────────────────────────────────────
    total = len(rows)
    print(f"\n{'─'*70}")
    print(f"  Total              : {total}")
    print(f"  Found + PDF        : {len(found_with_pdf)}  ({len(found_with_pdf)/total*100:.1f}%)")
    print(f"  Found, no PDF      : {len(found_no_pdf)}  ({len(found_no_pdf)/total*100:.1f}%)")
    print(f"  Not found          : {len(not_found)}  ({len(not_found)/total*100:.1f}%)")
    print(f"{'─'*70}")

    return found_with_pdf, found_no_pdf, not_found