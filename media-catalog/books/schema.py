"""Books schema (additive and idempotent, like the favourites schema in web/app.py).

books        one row per book unit: a Calibre book folder, or same-named files in one folder.
             Resolved fields (title, authors, ...) are rewritten by books.resolve; unit
             identity (unit_key) is stable across scans so favourites survive.
book_files   one row per ebook file with its raw extracted evidence (JSONB).
book_awards  awards derived from tags by books.awards.
book_favourites / book_labels
             stars and labels for books, sharing the app-wide tags table.
isbn_lookups catalogue records fetched by ISBN (books.lookup), misses included, so
             each ISBN is fetched once.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import execute

BOOKS_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS books (
    id              SERIAL PRIMARY KEY,
    unit_key        TEXT NOT NULL UNIQUE,
    root            TEXT NOT NULL,
    directory       TEXT NOT NULL,
    calibre_dir     BOOLEAN NOT NULL DEFAULT false,
    library         TEXT,
    calibre         JSONB,
    -- resolved by books.resolve
    title           TEXT,
    sort_title      TEXT,
    authors         TEXT[] NOT NULL DEFAULT '{}',
    author_sort     TEXT,
    series          TEXT,
    series_index    REAL,
    subjects        TEXT[] NOT NULL DEFAULT '{}',
    publisher       TEXT,
    languages       TEXT[] NOT NULL DEFAULT '{}',
    pubdate         DATE,
    pub_year        INTEGER,
    rating          REAL,
    description     TEXT,
    isbn            TEXT,
    identifiers     JSONB NOT NULL DEFAULT '{}',
    goodreads_id    TEXT,
    isfdb_url       TEXT,
    formats         TEXT[] NOT NULL DEFAULT '{}',
    file_count      INTEGER NOT NULL DEFAULT 0,
    size_bytes      BIGINT NOT NULL DEFAULT 0,
    available       BOOLEAN NOT NULL DEFAULT false,
    cover_path      TEXT,
    cover_member    TEXT,
    provenance      JSONB NOT NULL DEFAULT '{}',
    confidence      REAL,
    needs_review    BOOLEAN NOT NULL DEFAULT false,
    review_reason   TEXT,
    swapped         BOOLEAN NOT NULL DEFAULT false,
    hidden          BOOLEAN NOT NULL DEFAULT false,
    work_key        TEXT,
    added_at        TIMESTAMPTZ,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tsv             tsvector
);

CREATE TABLE IF NOT EXISTS book_files (
    id              SERIAL PRIMARY KEY,
    book_id         INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
    file_path       TEXT NOT NULL UNIQUE,
    format          TEXT,
    size_bytes      BIGINT,
    mtime           DOUBLE PRECISION,
    present         BOOLEAN NOT NULL DEFAULT true,
    evidence        JSONB,
    error           TEXT,
    extracted_at    TIMESTAMPTZ,
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS book_awards (
    book_id         INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
    award           TEXT NOT NULL,
    category        TEXT,
    year            INTEGER,
    result          TEXT,
    raw             TEXT NOT NULL,
    PRIMARY KEY (book_id, raw, award)
);

CREATE TABLE IF NOT EXISTS isbn_lookups (
    isbn            TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    response        JSONB,          -- NULL: the catalogue has no record of this ISBN
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tags (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS book_favourites (
    book_id    INTEGER PRIMARY KEY REFERENCES books(id) ON DELETE CASCADE,
    starred_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS book_labels (
    book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
    tag_id  INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_books_title_trgm ON books USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_books_authors ON books USING gin (authors);
CREATE INDEX IF NOT EXISTS idx_books_subjects ON books USING gin (subjects);
CREATE INDEX IF NOT EXISTS idx_books_languages ON books USING gin (languages);
CREATE INDEX IF NOT EXISTS idx_books_formats ON books USING gin (formats);
CREATE INDEX IF NOT EXISTS idx_books_tsv ON books USING gin (tsv);
CREATE INDEX IF NOT EXISTS idx_books_series ON books (series);
CREATE INDEX IF NOT EXISTS idx_books_work_key ON books (work_key);
CREATE INDEX IF NOT EXISTS idx_books_pub_year ON books (pub_year);
CREATE INDEX IF NOT EXISTS idx_books_rating ON books (rating);
CREATE INDEX IF NOT EXISTS idx_books_work_key ON books (work_key);
CREATE INDEX IF NOT EXISTS idx_books_library ON books (library);
CREATE INDEX IF NOT EXISTS idx_books_root ON books (root);
CREATE INDEX IF NOT EXISTS idx_book_files_book ON book_files (book_id);
CREATE INDEX IF NOT EXISTS idx_book_awards_award ON book_awards (award, result);

CREATE OR REPLACE FUNCTION books_tsv_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.tsv :=
    setweight(to_tsvector('english', coalesce(NEW.title, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(array_to_string(NEW.authors, ' '), '')), 'A') ||
    setweight(to_tsvector('english', coalesce(NEW.series, '')), 'B') ||
    setweight(to_tsvector('english', coalesce(array_to_string(NEW.subjects, ' '), '')), 'C') ||
    setweight(to_tsvector('english', coalesce(NEW.description, '')), 'D');
  RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER books_tsv_trig BEFORE INSERT OR UPDATE ON books
  FOR EACH ROW EXECUTE FUNCTION books_tsv_update();
"""


def ensure_books_schema():
    execute(BOOKS_SCHEMA_SQL)
