import sys
sys.path.append('../../')
# Import directly from config
from src.config import SEMANTIC_SCHOLAR_API_KEY, SEMANTIC_SCHOLAR_API_URL
from src.utils.dict_parser import PaperDictParser
import requests
import time
import json
from typing import Dict, List, Optional
import pandas as pd

class SemanticScholarClient:
    """
    Client for fetching publications and snippets from the Semantic Scholar API

    Usage:
    """

    def __init__(self, api_key: str=None):
        """
        Initialize the  client.

        Args:
            api_key : Allows to specify an API key. By default, the environment API key is used
        """
        self.api_key = api_key or SEMANTIC_SCHOLAR_API_KEY
        self.base_url = SEMANTIC_SCHOLAR_API_URL
        self.headers = {}

        if self.api_key:
            self.headers["x-api-key"] = self.api_key
            print("API key successfully loaded")
        else:
            print("API is invalid/not found - proceeding to anonymous access")

    def search_papers(
        self,
        query: str,
        limit: int = 100,
        offset: int = 0,
        fields: List[str] = None,
        fields_of_study: str = None
    )-> List[Dict]:
        """
        Search for papers matching a query.
        
        :param self: 
        :param query: String query
        :type query: str
        :param limit: Amount of papers per search request
        :type limit: int
        :param offset: Specifies current starting page (1-101)
        :type offset: int
        :param fields: A comma-separated list of the fields to be returned. e.g. papers.year, papers.authors, affiliations, name, etc
        :type fields: List[str]
        :param fields_of_study: Restricts results to papers in the given fields of study, formatted as a comma-separated string (e.g., "Computer Science,Medicine") or list
        :type fields_of_study: str or List[str]
        :return: List of Publication Dictionaries
        :rtype: List[Dict]
        """

        print(f"Fetching from Semantic Scholar (with API limit of {limit}) papers about '{query}'...")
        if fields_of_study:
            print(f"Field of studies are: {fields_of_study}")

        all_papers = []
        batch_size = 100 # The API Limit
        total_batches = (limit + batch_size -1) // batch_size
        actual_total = None

        default_fields = fields or ["title", "abstract", "year", "tldr"]#, "venue", "externalIds.DOI", "tldr"]  # Fixed typo: "tite" -> "title"

        for batch in range(total_batches):
            current_offset = offset + batch * batch_size
            current_limit = min(batch_size, limit - batch * batch_size)

            # Stop if actual total is exceeded
            if actual_total is not None and current_offset >= actual_total:
                print(f"Reached actual total papers ({actual_total}); stopping fetch")
                break
            print(f"Fetching batch {batch+1}/{total_batches} papers (offset: {current_offset}, limit: {current_limit})...")
            
            papers, total = self._fetch_batch(
                query, current_limit, current_offset, default_fields, fields_of_study
            )

            if batch == 0:
                print("API call successful!")
                print(f"Total papers: {total:,}")
                actual_total = total
                
            all_papers.extend(papers)
            print(f"Batch {batch+1}: fetched {len(papers)} papers")
            return all_papers


    def _fetch_batch(
        self,
        query: str,
        limit: int,
        offset: int,
        fields: List[str],
        fields_of_study: str = None,
        max_retries: int = 10,
    ) -> tuple:
        """Fetch a single batch of papers with retry logic."""
        url = f"{self.base_url}/paper/search"
        params = {
            "query": query,
            "fields": ",".join(fields),
            "limit": limit,
            "offset": offset,
        }

        if fields_of_study:
            params["fieldsOfStudy"] = fields_of_study.strip()

        for retry in range(max_retries):
            try:
                response = requests.get(
                    url, params=params, headers=self.headers, timeout=30
                )

                # Handle rate limiting (retryable)
                if response.status_code == 429:
                    if retry < max_retries - 1:
                        print(
                            f"  Rate limited (429); waiting 5s before retry... (attempt {retry + 1})"
                        )
                        time.sleep(5)
                        continue
                    else:
                        print("  Reached max retries for rate limiting")
                        return [], 0

                # Handle client errors (NOT retryable - bad request)
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    print(f"  Client error {response.status_code}: {response.text}")
                    return [], 0  # Don't retry - fix the request instead

                # Handle server errors (retryable)
                if response.status_code >= 500:
                    if retry < max_retries - 1:
                        print(
                            f"  Server error {response.status_code}; waiting 5s before retry... (attempt {retry + 1})"
                        )
                        time.sleep(5)
                        continue
                    else:
                        print(f"  Reached max retries for server error")
                        return [], 0

                response.raise_for_status()
                data = response.json()

                return data.get("data", []), data.get("total", 0)

            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    print(
                        f"  Network error: {e}; waiting 5s before retry... (attempt {retry + 1})"
                    )
                    time.sleep(5)
                else:
                    print(f"  Reached max retries: {e}")
                    return [], 0
            except json.JSONDecodeError as e:
                if retry < max_retries - 1:
                    print(f"  JSON parse failed: {e}; waiting 5s before retry...")
                    time.sleep(5)
                else:
                    print(f"  Reached max retries: {e}")
                    return [], 0

        return [], 0
    
# Example Usage 
parser = PaperDictParser()
client = SemanticScholarClient()
result = client.search_papers(query="Transformers")
parser.parse_papers(result)
parser.to_json(filename="retrieved_results") 