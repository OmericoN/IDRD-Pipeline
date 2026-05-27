from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pipeline.repository import PipelineRepository
from pipeline.schemas import UMDatasetRecord


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
