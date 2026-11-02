"""_flush_files stores a row Postgres rejects without its evidence instead of failing the batch."""
import psycopg2.extras

from books import scan


class _FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self):
        self.log, self.committed = [], []

    def cursor(self):
        return _FakeCursor(self.log)

    def commit(self):
        self.committed.append(list(self.log))
        self.log.clear()

    def rollback(self):
        self.log.clear()


def test_poison_row_is_stored_without_evidence(monkeypatch):
    def fake_execute_values(cur, sql, rows, template=None):
        for row in rows:
            if row[6] is None and "poison" in str(row[5]):
                raise psycopg2.DataError("unsupported Unicode escape sequence")
        cur.log.extend(rows)

    monkeypatch.setattr(psycopg2.extras, "execute_values", fake_execute_values)
    conn = _FakeConn()
    good = (1, "/a.epub", "epub", 1, 1.0, "ok", None)
    bad = (1, "/b.epub", "epub", 1, 1.0, "poison", None)
    scan._flush_files(conn, [good, bad])
    stored = [row for batch in conn.committed for row in batch]
    assert good in stored
    assert [r for r in stored if r[1] == "/b.epub"][0][5:] == (None, "unstorable: unsupported Unicode escape sequence")
