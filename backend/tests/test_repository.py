from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from datasight.domain.schemas import UMDatasetRecord
from datasight.infrastructure.persistence.repository import PipelineRepository


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.fetchone_results = []
        self.fetchall_results = []
        self.rowcount = 0

    def execute(self, query, params=None):
        self.executed.append((str(query), params))

    def executemany(self, query, params):
        for values in params:
            self.execute(query, values)

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def cursor(self, cursor_factory=None):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closes += 1


def make_repo():
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    repo = PipelineRepository.__new__(PipelineRepository)
    repo.cursor = cursor
    repo.conn = conn
    return repo, cursor, conn


def test_upsert_um_datasets_uses_canonical_table():
    repo, cursor, conn = make_repo()
    record = UMDatasetRecord(
        um_dataset_id="um-1",
        title="Maastricht Health Survey",
        aliases=["MHS"],
        creators=["Jane Doe"],
        year=2024,
    )

    assert repo.upsert_um_datasets([record]) == 1

    query, params = cursor.executed[-1]
    assert "INSERT INTO um_datasets" in query
    assert params[0] == "um-1"
    assert params[2] == ["MHS"]
    assert conn.commits == 1


def test_sync_um_datasets_upserts_then_removes_stale_records():
    repo, cursor, conn = make_repo()
    cursor.rowcount = 1
    record = UMDatasetRecord(um_dataset_id="W123", title="Dataset")

    assert repo.sync_um_datasets([record]) == (1, 1)

    assert any("INSERT INTO um_datasets" in query for query, _ in cursor.executed)
    delete_query, delete_params = cursor.executed[-1]
    assert "DELETE FROM um_datasets" in delete_query
    assert delete_params == (["W123"],)
    assert conn.commits == 1


def test_upsert_openalex_publications_replaces_child_rows():
    repo, cursor, conn = make_repo()
    cursor.fetchone_results = [{"id": 11}]
    paper = {
        "paperId": "W123",
        "id": "https://openalex.org/W123",
        "doi": "10.123/example",
        "title": "OpenAlex work",
        "abstract": "A study using data.",
        "year": 2024,
        "publication_date": "2024-01-02",
        "language": "en",
        "publication_type": "article",
        "source_url": "https://openalex.org/W123",
        "open_access_url": "https://example.org/work.pdf",
        "oa_status": "gold",
        "cited_by_count": 12,
        "is_retracted": False,
        "has_fulltext": True,
        "primary_source_name": "Journal",
        "raw": {
            "authorships": [
                {
                    "author": {"id": "https://openalex.org/A1", "display_name": "Jane Doe"},
                    "is_corresponding": True,
                    "institutions": [{"id": "https://openalex.org/I1", "display_name": "UM"}],
                }
            ],
            "topics": [{"id": "https://openalex.org/T1", "display_name": "Health data", "score": 0.9}],
            "keywords": [{"display_name": "biobank", "score": 0.8}],
            "concepts": [{"display_name": "Epidemiology", "level": 1, "score": 0.7}],
            "mesh": [{"descriptor_name": "Humans", "qualifier_name": "analysis"}],
            "related_works": ["https://openalex.org/W999"],
        },
    }

    assert repo.upsert_publications([paper], source="openalex") == 1

    queries = [query for query, _ in cursor.executed]
    assert "RETURNING id" in queries[0]
    assert any("DELETE FROM publication_openalex_topics" in query for query in queries)
    assert any("INSERT INTO publication_openalex_affiliations" in query for query in queries)
    assert any("INSERT INTO publication_openalex_topics" in query for query in queries)
    assert any("INSERT INTO publication_openalex_keywords" in query for query in queries)
    assert any("INSERT INTO publication_openalex_concepts" in query for query in queries)
    assert any("INSERT INTO publication_openalex_mesh" in query for query in queries)
    assert any("INSERT INTO publication_openalex_related_works" in query for query in queries)
    assert conn.commits == 1


def test_healthcheck_reports_candidate_table_readiness():
    repo, cursor, _ = make_repo()
    cursor.fetchone_results = [
        {"database": "datasight_pipeline", "user": "postgres"},
        {"relation": "publications"},
        {"relation": "mention_candidates"},
        {"relation": "alembic_version"},
        {"version_num": "20260527_0002"},
        {"installed": True},
    ]

    health = repo.healthcheck()

    assert health["ready"] is True
    assert health["mention_candidates_table"] is True
    assert health["vector_search_ready"] is True


def test_active_run_count_reads_running_statuses():
    repo, cursor, _ = make_repo()
    cursor.fetchone_results = [{"count": 2}]

    assert repo.active_run_count() == 2
    query, _ = cursor.executed[-1]
    assert "status IN ('queued', 'running', 'started')" in query


def test_create_pipeline_run_records_creation_event():
    repo, cursor, conn = make_repo()
    cursor.fetchone_results = [{"id": 9}]

    run_id = repo.create_pipeline_run("dataset reuse", {"limit": 5})

    assert run_id == 9
    event_query, event_params = cursor.executed[-1]
    assert "INSERT INTO pipeline_run_events" in event_query
    assert event_params[:4] == (9, None, "info", 'Pipeline run created for "dataset reuse".')
    assert conn.commits == 1


def test_record_stage_result_records_stage_event():
    repo, cursor, conn = make_repo()

    repo.record_stage_result(
        stage="discover",
        status="successful",
        metrics={"message": "Discovered 3 publications.", "count": 3},
        pipeline_run_id=9,
    )

    assert "SELECT id, attempt_count" in cursor.executed[0][0]
    assert "INSERT INTO stage_runs" in cursor.executed[1][0]
    event_query, event_params = cursor.executed[2]
    assert "INSERT INTO pipeline_run_events" in event_query
    assert event_params[:4] == (9, "discover", "info", "Discovered 3 publications.")
    assert conn.commits == 1


def test_create_pipeline_items_initializes_item_stages():
    repo, cursor, conn = make_repo()
    cursor.fetchone_results = [{"id": 100}, {"id": 200}]

    item_ids = repo.create_pipeline_items(9, ["paper-1"])

    assert item_ids == [200]
    queries = [query for query, _ in cursor.executed]
    assert any("INSERT INTO pipeline_items" in query for query in queries)
    stage_inserts = [params for query, params in cursor.executed if "INSERT INTO pipeline_item_stages" in query]
    assert len(stage_inserts) == 6
    assert stage_inserts[0] == (200, "download_pdf")
    assert conn.commits == 1


def test_start_item_stage_marks_stage_and_item_running():
    repo, cursor, conn = make_repo()
    cursor.fetchone_results = [{"id": 10}, {"pipeline_run_id": 9}, {"total": 1, "successful": 0, "failed": 0, "skipped": 0, "active": 1, "pending": 0}, None]

    assert repo.start_item_stage(200, "download_pdf", "task-1") is True

    assert "UPDATE pipeline_item_stages" in cursor.executed[0][0]
    assert "UPDATE pipeline_items SET status = 'running'" in cursor.executed[1][0]
    assert conn.commits >= 1


def test_claim_queued_item_stages_uses_skip_locked():
    repo, cursor, _ = make_repo()
    cursor.fetchall_results = [[{"pipeline_item_id": 200}, {"pipeline_item_id": 201}]]
    cursor.fetchone_results = [
        {"total": 2, "successful": 0, "failed": 0, "skipped": 0, "queued": 0, "running": 2, "pending": 0},
        None,
    ]

    item_ids = repo.claim_queued_item_stages(9, "download_pdf", 2, "task-1")

    assert item_ids == [200, 201]
    claim_query, claim_params = cursor.executed[0]
    assert "FOR UPDATE SKIP LOCKED" in claim_query
    assert "pis.status = 'queued'" in claim_query
    assert claim_params == (9, "download_pdf", 2, "task-1")


def test_high_throughput_outcome_reports_partial_errors():
    repo, cursor, _ = make_repo()
    cursor.fetchone_results = [{"failed": 1, "skipped": 3}]

    assert repo.high_throughput_outcome(9) == "completed_with_errors"


def test_fail_pipeline_run_records_error_event():
    repo, cursor, conn = make_repo()

    repo.fail_pipeline_run(9, "No UM datasets are imported.")

    assert "UPDATE pipeline_runs" in cursor.executed[0][0]
    event_query, event_params = cursor.executed[1]
    assert "INSERT INTO pipeline_run_events" in event_query
    assert event_params[:4] == (9, None, "error", "Pipeline run failed.")
    assert conn.commits == 1


def test_list_pipeline_run_events_reads_chronological_events():
    repo, cursor, _ = make_repo()
    cursor.fetchall_results = [
        [
            {
                "id": 1,
                "pipeline_run_id": 9,
                "stage": "discover",
                "level": "info",
                "message": "Discovered 3 publications.",
                "payload": {},
            }
        ]
    ]

    events = repo.list_pipeline_run_events(9, limit=10)

    assert events[0]["stage"] == "discover"
    query, params = cursor.executed[-1]
    assert "FROM pipeline_run_events" in query
    assert params == (9, 10)


def test_reset_database_truncates_pipeline_tables_and_preserves_schema():
    repo, cursor, conn = make_repo()

    tables = repo.reset_database()

    query, _ = cursor.executed[-1]
    assert "TRUNCATE" in query
    assert "alembic_version" not in query
    assert "pipeline_item_stages" in tables
    assert "pipeline_items" in tables
    assert "pipeline_run_events" in tables
    assert "publications" in tables
    assert conn.commits == 1
