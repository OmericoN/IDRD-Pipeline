from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from idrd.pipeline.schemas import UMDatasetRecord
from idrd.storage.repository import PipelineRepository


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.fetchone_results = []
        self.fetchall_results = []

    def execute(self, query, params=None):
        self.executed.append((str(query), params))

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


def test_healthcheck_reports_candidate_table_readiness():
    repo, cursor, _ = make_repo()
    cursor.fetchone_results = [
        {"database": "idrd_pipeline", "user": "postgres"},
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

    assert "INSERT INTO stage_runs" in cursor.executed[0][0]
    event_query, event_params = cursor.executed[1]
    assert "INSERT INTO pipeline_run_events" in event_query
    assert event_params[:4] == (9, "discover", "info", "Discovered 3 publications.")
    assert conn.commits == 1


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
    assert "pipeline_run_events" in tables
    assert "publications" in tables
    assert conn.commits == 1
