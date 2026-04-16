import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from db import db as db_module
from db.db import IDRDDatabase


class FakeCursor:
    def __init__(self):
        self.executed = []
        self.fetchone_results = []
        self.fetchall_results = []
        self.execute_side_effects = []
        self.rowcount = 0

    def execute(self, query, params=None):
        self.executed.append((str(query), params))
        if self.execute_side_effects:
            effect = self.execute_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            if callable(effect):
                effect(query, params)

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
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self, cursor_factory=None):
        return self._cursor

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1


@pytest.fixture
def db_obj():
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    db = IDRDDatabase.__new__(IDRDDatabase)
    db.cursor = cursor
    db.conn = conn
    return db, cursor, conn


def test_init_and_double_instantiation_warning(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    monkeypatch.setattr(db_module.psycopg2, "connect", lambda **kwargs: conn)

    calls = {"stack": 0}
    import traceback

    monkeypatch.setattr(traceback, "print_stack", lambda limit=8: calls.__setitem__("stack", calls["stack"] + 1))
    IDRDDatabase._init_count = 0

    first = IDRDDatabase()
    second = IDRDDatabase()

    assert first.conn is conn
    assert second.cursor is cursor
    assert conn.autocommit is False
    assert calls["stack"] == 1
    assert conn.commit_calls >= 2


def test_insert_publication_success_paths(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchone_results = [{"id": 10}]
    paper = {
        "paperId": "p1",
        "title": "t",
        "abstract": "a",
        "year": 2020,
        "url": "u",
        "venue": "v",
        "publicationDate": "2020-01-01",
        "citationCount": 2,
        "referenceCount": 1,
        "influentialCitationCount": 1,
        "tldr": {"text": "summary"},
        "openAccessPdf": {"url": "pdf", "status": "ok", "license": "x", "disclaimer": "d"},
        "authors": [{"authorId": "a1", "name": "Alice", "url": "au"}],
        "externalIds": {"DOI": "d", "ArXiv": "x", "PubMed": "p", "DBLP": "b", "CorpusId": "c"},
        "journal": {"name": "jn", "volume": "1", "pages": "1-2"},
        "publicationTypes": ["JournalArticle"],
        "fieldsOfStudy": ["Computer Science"],
        "citations": [
            {
                "isInfluential": True,
                "citingPaper": {
                    "paperId": "cp1",
                    "title": "ct",
                    "year": 2019,
                    "authors": [{"authorId": "ca1", "name": "Carl"}],
                },
                "contexts": ["ctx"],
                "intents": ["Background"],
            }
        ],
    }
    assert db.insert_publication(paper) is True
    assert len(cursor.executed) > 8


def test_insert_publication_open_access_truthy_and_none_citations(db_obj):
    db, _, _ = db_obj
    paper = {"paperId": "p2", "openAccessPdf": "truthy", "citations": None}
    assert db.insert_publication(paper) is True


def test_insert_publication_error_rolls_back(db_obj):
    db, cursor, conn = db_obj
    cursor.execute_side_effects = [RuntimeError("boom")]
    assert db.insert_publication({"paperId": "err"}) is False
    assert conn.rollback_calls == 1


def test_insert_publications_branches(db_obj):
    db, cursor, conn = db_obj
    cursor.fetchone_results = [{"id": 1}, {"id": 2}, None]
    papers = [
        {},
        {"paperId": "p1", "authors": [{"authorId": "a1", "name": "A1"}, {"name": "Anon"}]},
        {"paperId": "p2", "authors": [{"authorId": "a2"}]},
    ]
    inserted = db.insert_publications(papers)
    assert inserted == 2
    assert conn.commit_calls == 2


def test_insert_publications_exception_path(db_obj):
    db, cursor, conn = db_obj
    cursor.execute_side_effects = [RuntimeError("bad")]
    inserted = db.insert_publications([{"paperId": "x"}])
    assert inserted == 0
    assert conn.rollback_calls == 1


def test_load_from_json_dict_and_list(db_obj, tmp_path):
    db, _, _ = db_obj
    calls = []
    db.insert_publications = lambda papers: calls.append(papers) or len(papers)

    dict_file = tmp_path / "one.json"
    dict_file.write_text(json.dumps({"paperId": "p1"}), encoding="utf-8")
    assert db.load_from_json(str(dict_file)) == 1
    assert calls[-1] == [{"paperId": "p1"}]

    list_file = tmp_path / "many.json"
    list_file.write_text(json.dumps([{"paperId": "p2"}, {"paperId": "p3"}]), encoding="utf-8")
    assert db.load_from_json(str(list_file)) == 2


def test_get_publication_none_and_full(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchone_results = [None]
    assert db.get_publication("missing") is None

    cursor.fetchone_results = [{"paperId": "p1", "title": "T"}]
    cursor.fetchall_results = [
        [{"authorId": "a1", "name": "A"}],
        [{"type": "JournalArticle"}],
        [{"field": "CS"}],
        [{"id": 7, "citingPaperId": "cp", "citingPaperTitle": "CT", "citingPaperYear": 2018, "isInfluential": 1}],
        [{"context": "ctx1"}],
        [{"intent": "Background"}],
        [{"authorId": "ca1", "name": "CA"}],
    ]
    result = db.get_publication("p1")
    assert result["paperId"] == "p1"
    assert result["publicationTypes"] == ["JournalArticle"]
    assert result["fieldsOfStudy"] == ["CS"]
    assert result["citations"][0]["isInfluential"] is True


def test_search_publications_with_all_filters(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchall_results = [[{"paperId": "p1"}]]
    rows = db.search_publications(
        title_contains="abc",
        year_from=2010,
        year_to=2020,
        min_citations=5,
        has_doi=True,
        has_open_access=False,
        field_of_study="CS",
        limit=3,
    )
    assert rows == [{"paperId": "p1"}]
    query, params = cursor.executed[-1]
    assert "ILIKE" in query
    assert params[-1] == 3


def test_search_publications_doi_false_open_access_true(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchall_results = [[]]
    db.search_publications(has_doi=False, has_open_access=True)
    query, _ = cursor.executed[-1]
    assert "e.doi IS NULL" in query
    assert "oa.url IS NOT NULL" in query


def test_get_statistics_with_avg_and_zero(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchone_results = [
        {"count": 10},
        {"min": 2000, "max": 2024},
        {"avg": 2.5, "max": 9, "sum": 25},
        {"count": 8},
        {"count": 7},
        {"count": 6},
        {"count": 5},
        {"count": 4},
        {"count": 3},
        {"count": 2},
        {"count": 1},
        {"count": 1},
    ]
    cursor.fetchall_results = [
        [{"venue": "v1", "count": 3}],
        [{"field": "CS", "count": 4}],
    ]
    stats = db.get_statistics()
    assert stats["citation_stats"]["average"] == 2.5
    assert stats["top_venues"]["v1"] == 3
    assert stats["top_fields"]["CS"] == 4

    cursor.fetchone_results = [
        {"count": 0},
        {"min": None, "max": None},
        {"avg": None, "max": None, "sum": None},
        {"count": 0},
        {"count": 0},
        {"count": 0},
        {"count": 0},
        {"count": 0},
        {"count": 0},
        {"count": 0},
        {"count": 0},
        {"count": 0},
    ]
    cursor.fetchall_results = [[], []]
    stats = db.get_statistics()
    assert stats["citation_stats"]["average"] == 0


def test_get_pipeline_status(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchone_results = [{"total_papers": 1}]
    assert db.get_pipeline_status() == {"total_papers": 1}
    cursor.fetchone_results = [None]
    assert db.get_pipeline_status() == {}


def test_get_papers_needing_download_limit_and_no_limit(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchall_results = [[{"paperId": "p1"}], [{"paperId": "p2"}]]
    assert db.get_papers_needing_download() == [{"paperId": "p1"}]
    assert db.get_papers_needing_download(limit=2) == [{"paperId": "p2"}]
    query, params = cursor.executed[-1]
    assert "LIMIT %s" in query
    assert params == [2]


def test_get_papers_needing_conversion_and_rendering(db_obj):
    db, cursor, _ = db_obj
    cursor.fetchall_results = [
        [{"paperId": "p1", "title": "t", "pdf_path": "x"}],
        [{"paperId": "p2", "title": "t2", "pdf_path": "y"}],
        [{"paperId": "p3", "title": "t3", "xml_path": "z"}],
        [{"paperId": "p4", "title": "t4", "xml_path": "w"}],
    ]
    assert db.get_papers_needing_conversion() == [{"paperId": "p1", "title": "t", "pdf_path": "x"}]
    assert db.get_papers_needing_conversion(limit=1) == [{"paperId": "p2", "title": "t2", "pdf_path": "y"}]
    assert db.get_papers_needing_rendering() == [{"paperId": "p3", "title": "t3", "xml_path": "z"}]
    assert db.get_papers_needing_rendering(limit=1) == [{"paperId": "p4", "title": "t4", "xml_path": "w"}]


def test_clear_and_drop_tables(db_obj):
    db, cursor, conn = db_obj
    calls = {"created": 0}
    db._create_tables = lambda: calls.__setitem__("created", calls["created"] + 1)
    db.clear_db()
    db.drop_tables()
    assert conn.commit_calls == 2
    assert calls["created"] == 1
    assert len(cursor.executed) >= 24


def test_reset_database_confirm_false(db_obj):
    db, cursor, conn = db_obj
    before_exec = len(cursor.executed)
    db.reset_database(confirm=False)
    assert conn.commit_calls == 0
    assert len(cursor.executed) == before_exec


def test_reset_database_full_path_with_suspicious_table(db_obj, monkeypatch, tmp_path):
    db, cursor, conn = db_obj
    calls = {"created": 0}
    db._create_tables = lambda: calls.__setitem__("created", calls["created"] + 1)
    db._clear_directory = lambda d: 1
    cursor.fetchall_results = [[{"tablename": "safe_table"}, {"tablename": "bad-name"}]]

    monkeypatch.setattr(db_module, "__file__", str(tmp_path / "src" / "db" / "db.py"))
    db.reset_database(confirm=True)
    assert conn.commit_calls == 1
    assert calls["created"] == 1
    assert any("DROP TABLE IF EXISTS" in q for q, _ in cursor.executed)


def test_clear_directory(tmp_path):
    f1 = tmp_path / "a.txt"
    nested = tmp_path / "nested"
    nested.mkdir()
    f2 = nested / "b.txt"
    f1.write_text("x", encoding="utf-8")
    f2.write_text("y", encoding="utf-8")
    assert IDRDDatabase._clear_directory(tmp_path) == 2
    assert not f1.exists()
    assert not f2.exists()
    assert IDRDDatabase._clear_directory(tmp_path / "missing") == 0


def test_reset_pipeline_status(db_obj):
    db, cursor, conn = db_obj
    db._clear_directory = lambda d: 1
    cursor.rowcount = 12
    db.reset_pipeline_status()
    assert conn.commit_calls == 1
    assert any("UPDATE publications SET" in q for q, _ in cursor.executed)


def test_export_to_json_limit_and_no_limit(db_obj, tmp_path):
    db, cursor, _ = db_obj
    output = tmp_path / "out.json"
    cursor.fetchall_results = [[{"paperId": "p1"}]]
    db.get_publication = lambda pid: {"paperId": pid, "title": "T"}
    db.export_to_json(str(output), limit=1)
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data == [{"paperId": "p1", "title": "T"}]
    assert "LIMIT 1" in cursor.executed[-1][0]

    output2 = tmp_path / "out2.json"
    cursor.fetchall_results = [[{"paperId": "p2"}]]
    db.export_to_json(str(output2))
    assert json.loads(output2.read_text(encoding="utf-8"))[0]["paperId"] == "p2"


def test_commit_close_and_context_manager(db_obj):
    db, _, conn = db_obj
    db.commit()
    db.close()
    assert conn.commit_calls == 1
    assert conn.close_calls == 1

    with db as ctx:
        assert ctx is db
    assert conn.commit_calls == 2
    assert conn.close_calls == 2
