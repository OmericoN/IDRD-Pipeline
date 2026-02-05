import os
from config import SEMANTIC_SCHOLAR_API_KEY, SEMANTIC_SCHOLAR_API_URL
from utils.dict_parser import PaperDictParser
import requests
import time
import json
from typing import Dict, List, Optional
import pandas as pd
from tqdm import tqdm
import sys
from pathlib import Path

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
        fields_of_study: str = None,
        open_access_pdf: bool = False
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
        :param open_access_pdf: If True, only return papers with open access PDFs
        :type open_access_pdf: bool
        :return: List of Publication Dictionaries
        :rtype: List[Dict]
        """

        print(f"Fetching from Semantic Scholar (with API limit of {limit}) papers about '{query}'...")
        if fields_of_study:
            print(f"  Field of studies: {fields_of_study}")
        if open_access_pdf:
            print(f"  Filter: Open Access PDFs only")

        all_papers = []
        batch_size = 100 # The API Limit
        total_batches = (limit + batch_size -1) // batch_size
        actual_total = None

        # Include DOI and open access fields
        default_fields = fields or [
            "paperId",
            "title", 
            "abstract", 
            "year", 
            "authors",
            "citationCount",
            "referenceCount",
            "influentialCitationCount",
            "venue",
            "publicationDate",
            "publicationTypes",
            "journal",
            "fieldsOfStudy",
            "url",
            "externalIds",  # This includes DOI
            "isOpenAccess",
            "openAccessPdf",  # PDF URL and status
            "tldr"
        ]

        with tqdm(total=limit, desc="Fetching papers", unit="papers") as pbar:
            for batch in range(total_batches):
                current_offset = offset + batch * batch_size
                current_limit = min(batch_size, limit - batch * batch_size)

                # Stop if actual total is exceeded
                if actual_total is not None and current_offset >= actual_total:
                    pbar.write(f"Reached actual total papers ({actual_total}); stopping fetch")
                    break
                
                papers, total = self._fetch_batch(
                    query, current_limit, current_offset, default_fields, fields_of_study, open_access_pdf
                )

                if batch == 0:
                    pbar.write("API call successful!")
                    pbar.write(f"Total papers: {total:,}")
                    actual_total = total
                    # Update progress bar total if actual total is less than requested
                    if actual_total < limit:
                        pbar.total = actual_total
                        pbar.refresh()
                    
                all_papers.extend(papers)
                pbar.update(len(papers))
                
        return all_papers

    def _fetch_batch(
        self,
        query: str,
        limit: int,
        offset: int,
        fields: List[str],
        fields_of_study: str = None,
        open_access_pdf: bool = False,
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

        # Convert fields_of_study list to string if needed
        if fields_of_study:
            if isinstance(fields_of_study, list):
                fields_of_study = ",".join(fields_of_study)
            params["fieldsOfStudy"] = fields_of_study.strip()
        
        # Add BOTH open access filters
        if open_access_pdf:
            params["isOpenAccess"] = ""  # Filter for open access papers
            params["openAccessPdf"] = ""  # Filter for papers with PDF URLs

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
    

    def get_paper_citations(
        self,
        paper_id: str,
        fields: List[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict]:
        """
        Get citations for a specific paper with contexts and intents.
        
        :param paper_id: Semantic Scholar paper ID
        :type paper_id: str
        :param fields: Fields to retrieve for each citation. Default includes contexts and intents
        :type fields: List[str]
        :param limit: Number of citations to retrieve per request (max 1000)
        :type limit: int
        :param offset: Starting offset for pagination
        :type offset: int
        :return: List of citation dictionaries
        :rtype: List[Dict]
        """
        print(f"Fetching citations for paper {paper_id}...")
        
        # Default fields - use nested syntax for citingPaper fields
        default_fields = fields or [
            "contexts",
            "intents",
            "isInfluential",
            "citingPaper.paperId",
            "citingPaper.title",
            "citingPaper.year",
            "citingPaper.authors"
        ]
        
        all_citations = []
        batch_size = min(limit, 1000)  # API max is 1000
        total_batches = (limit + batch_size - 1) // batch_size
        
        with tqdm(total=limit, desc="Fetching citations", unit="citations") as pbar:
            for batch in range(total_batches):
                current_offset = offset + batch * batch_size
                current_limit = min(batch_size, limit - batch * batch_size)
                
                citations = self._fetch_citations_batch(
                    paper_id, current_limit, current_offset, default_fields
                )
                
                if not citations:
                    pbar.write("No more citations available")
                    break
                
                all_citations.extend(citations)
                pbar.update(len(citations))
                
                # If we got fewer citations than requested, we've reached the end
                if len(citations) < current_limit:
                    break
        
        print(f"Total citations fetched: {len(all_citations)}")
        return all_citations
    
    def _fetch_citations_batch(
        self,
        paper_id: str,
        limit: int,
        offset: int,
        fields: List[str],
        max_retries: int = 10
    ) -> List[Dict]:
        """Fetch a single batch of citations with retry logic."""
        url = f"{self.base_url}/paper/{paper_id}/citations"
        params = {
            "fields": ",".join(fields),
            "limit": limit,
            "offset": offset
        }
        
        for retry in range(max_retries):
            try:
                response = requests.get(
                    url, params=params, headers=self.headers, timeout=30
                )
                
                # Handle rate limiting
                if response.status_code == 429:
                    if retry < max_retries - 1:
                        print(f"  Rate limited; waiting 5s... (attempt {retry + 1})")
                        time.sleep(5)
                        continue
                    else:
                        print("  Reached max retries for rate limiting")
                        return []
                
                # Handle client errors
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    print(f"  Client error {response.status_code}: {response.text}")
                    return []
                
                # Handle server errors
                if response.status_code >= 500:
                    if retry < max_retries - 1:
                        print(f"  Server error {response.status_code}; waiting 5s...")
                        time.sleep(5)
                        continue
                    else:
                        print("  Reached max retries for server error")
                        return []
                
                response.raise_for_status()
                data = response.json()
                
                return data.get("data", [])
                
            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    print(f"  Network error: {e}; waiting 5s...")
                    time.sleep(5)
                else:
                    print(f"  Reached max retries: {e}")
                    return []
            except json.JSONDecodeError as e:
                if retry < max_retries - 1:
                    print(f"  JSON parse failed: {e}; waiting 5s...")
                    time.sleep(5)
                else:
                    print(f"  Reached max retries: {e}")
                    return []
        
        return []
    
    def enrich_papers_with_citations(
        self,
        papers: List[Dict],
        max_citations_per_paper: int = 100
    ) -> List[Dict]:
        """
        Enrich papers with their citation contexts and intents.
        
        :param papers: List of paper dictionaries
        :type papers: List[Dict]
        :param max_citations_per_paper: Maximum citations to fetch per paper
        :type max_citations_per_paper: int
        :return: Papers enriched with citation data
        :rtype: List[Dict]
        """
        print(f"\nEnriching {len(papers)} papers with citation contexts...")
        
        with tqdm(total=len(papers), desc="Enriching papers", unit="papers") as pbar:
            for paper in papers:
                paper_id = paper.get('paperId')
                if not paper_id:
                    paper['citations'] = []
                    pbar.update(1)
                    continue
                
                citations = self.get_paper_citations(
                    paper_id=paper_id,
                    limit=max_citations_per_paper
                )
                
                paper['citations'] = citations
                pbar.update(1)
                time.sleep(0.1)  # Small delay to avoid rate limiting
        
        return papers

# Example Usage 
if __name__ == "__main__":
    # Add parent directory to path for database imports
    sys.path.append(str(Path(__file__).parent.parent))
    from db.db import PublicationDatabase
    
    parser = PaperDictParser()
    client = SemanticScholarClient()
    
    # Example 1: Basic paper search
    result = client.search_papers(
        query="Transformers", 
        open_access_pdf=True,
        limit=10  # Reduced for testing
    )
    
    # Example 3: Enrich papers with citation contexts
    result = client.enrich_papers_with_citations(result, max_citations_per_paper=50)
    
    parser.parse_papers(result)
    
    # Save to JSON (check the actual method signature - likely just needs filename)
    json_path = Path(__file__).parent.parent.parent / 'outputs' / 'metadata' / 'retrieved_results.json'
    json_path.parent.mkdir(parents=True, exist_ok=True)
    parser.to_json(str(json_path))  # Pass full path as single argument
    
    # Also save to database
    print("\nSaving to database...")
    db = PublicationDatabase()
    count = db.insert_publications(result)
    print(f"✓ Saved {count} papers to database")
    db.close()
    
    # Print summary
    open_access_count = sum(1 for p in result if p.get('openAccessPdf', {}).get('url'))
    papers_with_doi = sum(1 for p in result if p.get('externalIds', {}).get('DOI'))
    papers_with_citations = sum(1 for p in result if p.get('citations'))
    
    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"Total papers fetched: {len(result)}")
    print(f"Papers with PDF URLs: {open_access_count}")
    print(f"Papers with DOI: {papers_with_doi}")
    print(f"Papers with citation data: {papers_with_citations}")
    print(f"Saved to: {json_path}")
    
    # Show first paper's details
    if result:
        first_paper = result[0]
        print(f"\nFirst paper example:")
        print(f"  Title: {first_paper.get('title')}")
        print(f"  DOI: {first_paper.get('externalIds', {}).get('DOI', 'N/A')}")
        print(f"  PDF: {first_paper.get('openAccessPdf', {}).get('url', 'N/A')}")
        print(f"  Citations: {first_paper.get('citationCount', 0)}")
        if first_paper.get('citations'):
            print(f"  Citation contexts fetched: {len(first_paper['citations'])}")
            if first_paper['citations']:
                first_citation = first_paper['citations'][0]
                print(f"  First citation contexts: {first_citation.get('contexts', [])[:2]}")
                print(f"  First citation intents: {first_citation.get('intents', [])}")