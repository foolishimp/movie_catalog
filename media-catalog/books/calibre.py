"""
Read Calibre libraries as one source of evidence.

A Calibre metadata.db is a curated but possibly stale record: files may have
moved or vanished, and titles and authors were sometimes filled from
inconsistently named files the wrong way round. The resolver weighs it
against the files themselves; this module only reports what Calibre says.

The database is copied to a temp file and opened read-only, so nothing is
ever written to (or locked on) the network share.
"""
import os
import shutil
import sqlite3
import tempfile

from books.naming import html_to_text

GENERIC_LIBRARY_NAMES = {"calibre", "calibre library", "calibre_lib", "lib", "library", "ebooks", "books"}


def library_label(library_root: str) -> str:
    """'…/calibre/calibre fiction 1990s' → 'fiction 1990s'; '/Volumes/Share/docs/lib/calibre' → 'docs lib calibre'."""
    parts = [p for p in library_root.rstrip("/").split("/") if p]
    name = parts[-1] if parts else library_root
    if name.lower().startswith("calibre ") and len(name) > 8:
        return name[8:]
    if name.lower() in GENERIC_LIBRARY_NAMES:
        if parts[0].lower() == "volumes":       # the mount isn't part of the library's name
            parts = parts[2:] or parts[-1:]
        return " ".join(parts[-3:])
    return name


def _columns(con, table: str) -> set:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}


def _multi(con, sql: str) -> dict:
    out: dict = {}
    for book, value in con.execute(sql):
        if value is not None:
            out.setdefault(book, []).append(value)
    return out


def load_library(db_path: str) -> dict:
    """Return {absolute book folder: calibre evidence} for one metadata.db."""
    library_root = os.path.dirname(db_path)
    label = library_label(library_root)
    tmpdir = tempfile.mkdtemp(prefix="calibre-")
    try:
        local = os.path.join(tmpdir, "metadata.db")
        shutil.copyfile(db_path, local)
        con = sqlite3.connect(f"file:{local}?mode=ro", uri=True)
        cols = _columns(con, "books")
        if "path" not in cols:
            return {}
        pubdate = "pubdate" if "pubdate" in cols else "NULL"
        timestamp = "timestamp" if "timestamp" in cols else "NULL"
        has_cover = "has_cover" if "has_cover" in cols else "0"
        books = con.execute(
            f"SELECT id, title, author_sort, path, {pubdate}, {timestamp}, {has_cover}, series_index FROM books"
            if "series_index" in cols else
            f"SELECT id, title, author_sort, path, {pubdate}, {timestamp}, {has_cover}, NULL FROM books"
        ).fetchall()

        def link(sql):
            try:
                return _multi(con, sql)
            except sqlite3.OperationalError:
                return {}

        authors = link("SELECT l.book, a.name FROM books_authors_link l JOIN authors a ON a.id = l.author ORDER BY l.id")
        tags = link("SELECT l.book, t.name FROM books_tags_link l JOIN tags t ON t.id = l.tag")
        series = link("SELECT l.book, s.name FROM books_series_link l JOIN series s ON s.id = l.series")
        publishers = link("SELECT l.book, p.name FROM books_publishers_link l JOIN publishers p ON p.id = l.publisher")
        languages = link("SELECT l.book, g.lang_code FROM books_languages_link l JOIN languages g ON g.id = l.lang_code ORDER BY l.item_order")
        ratings = link("SELECT l.book, r.rating FROM books_ratings_link l JOIN ratings r ON r.id = l.rating")
        comments = link("SELECT book, text FROM comments")
        idents = {}
        try:
            for book, typ, val in con.execute("SELECT book, type, val FROM identifiers"):
                idents.setdefault(book, {})[typ] = val
        except sqlite3.OperationalError:
            pass
        data_cols = _columns(con, "data")
        formats = {}
        if {"book", "format", "name"} <= data_cols:
            size_col = "uncompressed_size" if "uncompressed_size" in data_cols else "NULL"
            for book, fmt, name, size in con.execute(f"SELECT book, format, name, {size_col} FROM data"):
                formats.setdefault(book, []).append({"format": fmt, "name": name, "size": size})
        con.close()
    except sqlite3.DatabaseError:
        return {}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    out = {}
    for book_id, title, author_sort, path, pub, ts, cover, sidx in books:
        if not path:
            continue
        book_tags = tags.get(book_id, [])
        pub = str(pub)[:10] if pub and not str(pub).startswith("0101") else None
        out[os.path.join(library_root, path)] = {
            "library": label,
            "library_root": library_root,
            "calibre_id": book_id,
            "title": title,
            "authors": authors.get(book_id, []),
            "author_sort": author_sort,
            "series": (series.get(book_id) or [None])[0],
            "series_index": sidx if series.get(book_id) else None,
            "tags": book_tags,
            "isfdb": "_isfdb" in book_tags,
            "publisher": (publishers.get(book_id) or [None])[0],
            "languages": languages.get(book_id, []),
            "pubdate": pub,
            "timestamp": str(ts) if ts else None,
            "rating": (ratings.get(book_id) or [None])[0],
            "description": html_to_text((comments.get(book_id) or [None])[0])[:12000] or None,
            "identifiers": idents.get(book_id, {}),
            "formats": formats.get(book_id, []),
            "has_cover": bool(cover),
        }
    return out
