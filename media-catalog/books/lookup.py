"""
Look up books that still lack an author in Open Library's catalogue, by ISBN.

Only the ISBN is sent: an ISBN with no record is not retried by title. A
978-prefixed ISBN-13 with no record is tried once more as its ISBN-10. Each
answer, misses included, is cached in isbn_lookups so an ISBN is fetched once.
The resolver reads the cache: a record's authors vote for a book whose own title
agrees with the record's title, so an ISBN read from the wrong page, or another
book's ISBN in the front matter, does not name the author.

Usage:
    python -m books.lookup                 # fetch, then resolve
    python -m books.lookup --dry-run       # count the ISBNs that would be sent
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn, query
from books.schema import ensure_books_schema

API = "https://openlibrary.org/api/books"
SOURCE = "openlibrary"
BATCH = 50                 # ISBNs per request
PAUSE = 1.0                # seconds between requests
USER_AGENT = "media-catalog/1.0 (personal library catalogue)"


def targets(limit: int | None = None) -> list[str]:
    """ISBNs of books on disk with no author that have not been looked up yet."""
    rows = query("""
        SELECT DISTINCT b.isbn FROM books b
        WHERE b.available AND NOT b.hidden AND cardinality(b.authors) = 0 AND b.isbn IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM isbn_lookups l WHERE l.isbn = b.isbn)
        ORDER BY b.isbn""")
    return [r["isbn"] for r in rows][:limit]


def isbn13_to_10(isbn: str) -> str | None:
    if not isbn.startswith("978"):
        return None
    core = isbn[3:12]
    check = (11 - sum((10 - i) * int(c) for i, c in enumerate(core)) % 11) % 11
    return core + ("X" if check == 10 else str(check))


def fetch(isbns: list[str]) -> dict:
    """{isbn: record} for the ISBNs Open Library knows, from one request."""
    params = {"bibkeys": ",".join(f"ISBN:{i}" for i in isbns), "format": "json", "jscmd": "data"}
    req = urllib.request.Request(f"{API}?{urllib.parse.urlencode(params)}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.load(resp)
    return {key.split(":", 1)[1]: rec for key, rec in data.items()}


def fetch_all(isbns: list[str]) -> dict:
    """{isbn13: record or None}, retrying misses as ISBN-10."""
    found = {}
    for i in range(0, len(isbns), BATCH):
        found.update(fetch(isbns[i:i + BATCH]))
        time.sleep(PAUSE)
    tens = {isbn13_to_10(i): i for i in isbns if i not in found and isbn13_to_10(i)}
    ten_list = list(tens)
    for i in range(0, len(ten_list), BATCH):
        found.update({tens[k]: rec for k, rec in fetch(ten_list[i:i + BATCH]).items()})
        time.sleep(PAUSE)
    return {i: found.get(i) for i in isbns}


def run(limit: int | None = None, dry_run: bool = False, resolve_after: bool = True):
    print("\n🔎 Looking up books with no author by ISBN (Open Library)")
    ensure_books_schema()
    isbns = targets(limit)
    print(f"   {len(isbns):,} ISBNs not looked up yet", flush=True)
    if dry_run or not isbns:
        return
    try:
        results = fetch_all(isbns)
    except (urllib.error.URLError, TimeoutError) as e:
        print(f"   Open Library did not answer: {e}")
        return
    rows = [(isbn, SOURCE, psycopg2.extras.Json(rec) if rec else None) for isbn, rec in results.items()]
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO isbn_lookups (isbn, source, response) VALUES (%s, %s, %s)
                ON CONFLICT (isbn) DO UPDATE SET source = EXCLUDED.source, response = EXCLUDED.response,
                                                 fetched_at = NOW()""", rows)
        conn.commit()
    finally:
        conn.close()
    hits = sum(1 for rec in results.values() if rec)
    print(f"   {hits:,} of {len(isbns):,} ISBNs have a record")
    if resolve_after:
        from books.resolve import resolve
        resolve()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Look up books with no author by ISBN in Open Library")
    parser.add_argument("--limit", type=int, help="Look up at most this many ISBNs")
    parser.add_argument("--dry-run", action="store_true", help="Count the ISBNs that would be sent; change nothing")
    parser.add_argument("--no-resolve", action="store_true", help="Skip the resolve stage")
    args = parser.parse_args()
    run(args.limit, args.dry_run, not args.no_resolve)
