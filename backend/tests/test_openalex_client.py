from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.infrastructure.pubfetcher.openalex import (
    OpenAlexClient,
    normalize_openalex_work,
    reconstruct_abstract,
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return self.responses.pop(0)


def test_reconstruct_abstract_from_inverted_index():
    assert reconstruct_abstract({"Data": [0], "reuse": [1], "matters": [2]}) == "Data reuse matters"


def test_normalize_openalex_work_prefers_best_oa_pdf_and_bare_work_id():
    publication = normalize_openalex_work(
        {
            "id": "https://openalex.org/W2527848138",
            "doi": "https://doi.org/10.123/example",
            "display_name": "Example work",
            "publication_year": 2024,
            "best_oa_location": {"pdf_url": "https://repo.example/work.pdf"},
            "primary_location": {
                "pdf_url": "https://publisher.example/work.pdf",
                "source": {"display_name": "Journal"},
            },
            "open_access": {"oa_status": "gold", "oa_url": "https://repo.example/work"},
            "abstract_inverted_index": {"Hello": [0], "world": [1]},
        }
    )

    assert publication["paperId"] == "W2527848138"
    assert publication["doi"] == "10.123/example"
    assert publication["open_access_url"] == "https://repo.example/work.pdf"
    assert publication["primary_source_name"] == "Journal"
    assert publication["abstract"] == "Hello world"


def test_search_works_uses_cursor_pagination_and_query_options():
    session = FakeSession(
        [
            FakeResponse(
                payload={
                    "meta": {"next_cursor": "next"},
                    "results": [{"id": "https://openalex.org/W1", "display_name": "One"}],
                }
            ),
            FakeResponse(
                payload={
                    "meta": {"next_cursor": None},
                    "results": [{"id": "https://openalex.org/W2", "display_name": "Two"}],
                }
            ),
        ]
    )
    client = OpenAlexClient(api_key="key", mailto="ops@example.org", session=session, request_delay=0)

    works = client.search_works(
        query="dataset reuse",
        limit=2,
        filters={"open_access.is_oa": True, "topics.id": ["T1", "T2"]},
        select=["id", "display_name"],
        sort="relevance_score:desc",
    )

    assert [work["paperId"] for work in works] == ["W1", "W2"]
    first_params = session.calls[0]["params"]
    assert first_params["search"] == "dataset reuse"
    assert first_params["filter"] == "open_access.is_oa:true,topics.id:T1|T2"
    assert first_params["select"] == "id,display_name"
    assert first_params["api_key"] == "key"
    assert first_params["mailto"] == "ops@example.org"
    assert session.calls[1]["params"]["cursor"] == "next"


def test_bulk_get_works_by_doi_uses_doi_filter():
    session = FakeSession(
        [
            FakeResponse(
                payload={
                    "meta": {"next_cursor": None},
                    "results": [{"id": "https://openalex.org/W1", "doi": "https://doi.org/10.1/a"}],
                }
            )
        ]
    )
    client = OpenAlexClient(api_key="", mailto="", session=session, request_delay=0)

    works = client.bulk_get_works_by_doi(["https://doi.org/10.1/a"])

    assert works[0]["paperId"] == "W1"
    assert session.calls[0]["params"]["filter"] == "doi:10.1/a"


def test_transient_errors_are_retried(monkeypatch):
    monkeypatch.setattr("datasight.infrastructure.pubfetcher.openalex.time.sleep", lambda _: None)
    session = FakeSession(
        [
            FakeResponse(status_code=429, text="rate limited"),
            FakeResponse(payload={"id": "https://openalex.org/W1"}),
        ]
    )
    client = OpenAlexClient(api_key="", mailto="", session=session, request_delay=0)

    work = client.get_work("W1")

    assert work["paperId"] == "W1"
    assert len(session.calls) == 2

