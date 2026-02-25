import os
import sys
import json
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm

sys.path.append(str(Path(__file__).parent.parent))

from config import SEMANTIC_SCHOLAR_API_KEY, SEMANTIC_SCHOLAR_API_URL
from utils.dict_parser import PaperDictParser


class SemanticScholarClient:
    """Client for fetching publications from the Semantic Scholar API."""

    # Semantic Scholar free tier: 1 req/s  |  with key: 10 req/s
    _REQUEST_DELAY = 0.15

    def __init__(self, api_key: str = None):
        self.api_key  = api_key or SEMANTIC_SCHOLAR_API_KEY
        self.base_url = SEMANTIC_SCHOLAR_API_URL
        self.headers  = {}

        if self.api_key:
            self.headers["x-api-key"] = self.api_key
            print("  API key loaded ✓")
        else:
            print("  ⚠ No API key — using anonymous access (stricter rate limits)")

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def search_papers(
        self,
        query: str,
        limit: int = 100,
        offset: int = 0,
        fields: List[str] = None,
        fields_of_study: str = None,
        open_access_pdf: bool = False,
    ) -> List[Dict]:
        """
        Search Semantic Scholar for papers matching a query.

        Args:
            query:           Search string.
            limit:           Max number of papers to return.
            offset:          Pagination offset.
            fields:          API fields to request (uses sensible defaults if None).
            fields_of_study: Filter e.g. "Computer Science".
            open_access_pdf: Only return papers that have a free PDF.

        Returns:
            List of paper dicts.
        """
        print(f"Fetching from Semantic Scholar (limit={limit}) papers about '{query}'...")
        if fields_of_study:
            print(f"  Fields of study : {fields_of_study}")
        if open_access_pdf:
            print(f"  Filter          : Open Access PDFs only")

        default_fields = fields or [
            "paperId", "title", "abstract", "year",
            "authors", "citationCount", "referenceCount",
            "influentialCitationCount", "venue", "publicationDate",
            "publicationTypes", "journal", "fieldsOfStudy",
            "url", "externalIds", "isOpenAccess", "openAccessPdf", "tldr",
        ]

        all_papers    = []
        batch_size    = min(100, limit)  # API max is 100
        total_batches = (limit + batch_size - 1) // batch_size
        actual_total  = None

        with tqdm(total=limit, desc="Fetching papers", unit="paper") as pbar:
            for batch_idx in range(total_batches):
                current_offset = offset + batch_idx * batch_size
                current_limit  = min(batch_size, limit - batch_idx * batch_size)

                if actual_total is not None and current_offset >= actual_total:
                    pbar.write(f"Reached actual total papers ({actual_total:,}) — stopping.")
                    break

                papers, total, error = self._fetch_batch(
                    query           = query,
                    limit           = current_limit,
                    offset          = current_offset,
                    fields          = default_fields,
                    fields_of_study = fields_of_study,
                    open_access_pdf = open_access_pdf,
                )

                if error:
                    pbar.write(f"  ⚠ Batch {batch_idx + 1} failed: {error} — skipping.")
                    continue

                if batch_idx == 0:
                    pbar.write("API call successful!")
                    pbar.write(f"Total papers: {total:,}")
                    actual_total = total
                    if actual_total < limit:
                        pbar.total = actual_total
                        pbar.refresh()

                if not papers:
                    break

                all_papers.extend(papers)
                pbar.update(len(papers))

                if batch_idx < total_batches - 1:
                    time.sleep(self._REQUEST_DELAY)

        print(f"Fetched {len(all_papers)} papers.")
        return all_papers

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _fetch_batch(
        self,
        query: str,
        limit: int,
        offset: int,
        fields: List[str],
        fields_of_study: str = None,
        open_access_pdf: bool = False,
        max_retries: int = 10,
    ) -> Tuple[List[Dict], int, Optional[str]]:
        """
        Fetch one batch of papers with exponential-backoff retry.

        Returns:
            (papers, total, error_message) — error_message is None on success.
        """
        url    = f"{self.base_url}/paper/search"
        params = {
            "query":  query,
            "fields": ",".join(fields),
            "limit":  limit,
            "offset": offset,
        }

        if fields_of_study:
            params["fieldsOfStudy"] = (
                ",".join(fields_of_study) if isinstance(fields_of_study, list)
                else fields_of_study.strip()
            )

        if open_access_pdf:
            # Send both params — isOpenAccess broadens results,
            # openAccessPdf ensures a direct PDF URL is returned
            params["isOpenAccess"]  = ""
            params["openAccessPdf"] = ""

        backoff = 5  # initial wait (s); doubles on each retry

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url, params=params, headers=self.headers, timeout=30
                )

                # Rate limited — exponential backoff
                if response.status_code == 429:
                    wait = backoff * (2 ** attempt)
                    if attempt < max_retries - 1:
                        print(f"\n  Rate limited — waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(wait)
                        continue
                    return [], 0, "Rate limited — max retries reached"

                # Server error — transient, retry with backoff
                if response.status_code >= 500:
                    wait = backoff * (2 ** attempt)
                    if attempt < max_retries - 1:
                        print(f"\n  Server error {response.status_code} — waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(wait)
                        continue
                    return [], 0, f"Server error {response.status_code} after {max_retries} attempts"

                # Client error — permanent, don't retry
                if 400 <= response.status_code < 500:
                    return [], 0, f"Client error {response.status_code}: {response.text[:200]}"

                # Success
                data = response.json()
                return data.get("data", []), data.get("total", 0), None

            except requests.exceptions.Timeout:
                wait = backoff * (2 ** attempt)
                if attempt < max_retries - 1:
                    print(f"\n  Timeout — waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait)
                    continue
                return [], 0, f"Timeout after {max_retries} attempts"

            except requests.exceptions.RequestException as e:
                wait = backoff * (2 ** attempt)
                if attempt < max_retries - 1:
                    print(f"\n  Network error: {e} — waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait)
                    continue
                return [], 0, f"Network error: {e}"

            except json.JSONDecodeError as e:
                return [], 0, f"JSON parse error: {e}"

        return [], 0, "Max retries exhausted"


# ── Standalone usage ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    from db.db import PublicationDatabase

    client = SemanticScholarClient()
    papers = client.search_papers(query="Transformers", open_access_pdf=True, limit=10)

    parser = PaperDictParser()
    parser.parse_papers(papers)

    out = Path(__file__).parent.parent.parent / 'outputs' / 'metadata' / 'retrieved_results.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    parser.to_json(str(out))

    db    = PublicationDatabase()
    count = db.insert_publications(papers)
    print(f"✓ Saved {count} papers to database")
    db.close()