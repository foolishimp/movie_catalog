"""
Books section: library grid with a Calibre-style category browser, book
detail, category listings, covers, favourites/labels, open/reveal and the
background book scan. Data comes from the books tables written by
books.scan and books.resolve; nothing here writes resolved fields.
"""
import asyncio
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import threading
import zipfile
from collections import OrderedDict
from functools import wraps
from pathlib import Path
from urllib.parse import quote, quote_plus, urlencode

from starlette.concurrency import run_in_threadpool
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from db import execute, query
from books import reading
from books.formats import split_member
from books.naming import html_to_text
from web.common import (BackgroundJob, PER_PAGE, ensure_mounted, host_open, json_safe,
                        pick_sort, positive_page, templates)

ROOT_DIR = Path(__file__).resolve().parent.parent


def _ensure_schema():
    try:
        if not query("SELECT to_regclass('public.books') AS t")[0]["t"]:
            from books.schema import ensure_books_schema
            ensure_books_schema()
    except Exception as e:
        import logging
        logging.warning("books schema migration failed: %s", e)


_ensure_schema()

# Calibre's tag-browser categories, in Calibre's order, plus Awards and
# Published (decade). param is the /books filter each value links to.
CATEGORIES = [
    {"kind": "author", "param": "author", "label": "Authors"},
    {"kind": "series", "param": "series", "label": "Series"},
    {"kind": "subject", "param": "subject", "label": "Tags"},
    {"kind": "award", "param": "award", "label": "Awards"},
    {"kind": "publisher", "param": "publisher", "label": "Publishers"},
    {"kind": "rating", "param": "min_rating", "label": "Rating"},
    {"kind": "decade", "param": "decade", "label": "Published"},
    {"kind": "language", "param": "language", "label": "Languages"},
    {"kind": "format", "param": "format", "label": "Formats"},
    {"kind": "library", "param": "library", "label": "Libraries"},
]
CATEGORY_BY_KIND = {c["kind"]: c for c in CATEGORIES}
FACET_LIMIT = 10
# Short categories are always shown in full in the sidebar.
FACET_FULL = {"rating", "decade", "language", "format", "library"}

FILTER_KEYS = ("q", "author", "series", "subject", "publisher", "language", "format", "library",
               "award", "award_result", "min_rating", "year_from", "year_to", "decade", "status",
               "source", "review", "swapped", "no_author", "starred", "label")

SORT_OPTIONS = {
    "relevance": "best_rank DESC, sort_title ASC NULLS LAST, id",
    "title": "sort_title ASC NULLS LAST, id",
    "author": "author_sort ASC NULLS LAST, series ASC NULLS LAST, series_index ASC NULLS LAST, sort_title ASC NULLS LAST, id",
    "series": "series ASC NULLS LAST, series_index ASC NULLS LAST, sort_title ASC NULLS LAST, id",
    "year": "pub_year DESC NULLS LAST, sort_title ASC NULLS LAST, id",
    "oldest": "pub_year ASC NULLS LAST, sort_title ASC NULLS LAST, id",
    "rating": "rating DESC NULLS LAST, sort_title ASC NULLS LAST, id",
    "recent": "added_at DESC NULLS LAST, id DESC",
}
SORT_LABELS = {"relevance": "Relevance", "title": "Title", "author": "Author", "series": "Series order",
               "year": "Newest", "oldest": "Oldest", "rating": "Rating", "recent": "Recently added"}
AWARD_RESULTS = ("winner", "nominee", "longlist")
FORMAT_PREFERENCE = ["EPUB", "AZW3", "KEPUB", "MOBI", "AZW", "PDF", "FB2", "DJVU", "CBZ", "CBR",
                     "DOCX", "RTF", "LIT", "PDB", "TXT"]
LANGUAGE_NAMES = {"eng": "English", "fre": "French", "fra": "French", "ger": "German", "deu": "German",
                  "spa": "Spanish", "ita": "Italian", "rus": "Russian", "bul": "Bulgarian", "dut": "Dutch",
                  "nld": "Dutch", "por": "Portuguese", "jpn": "Japanese", "chi": "Chinese", "zho": "Chinese",
                  "pol": "Polish", "swe": "Swedish", "und": "Undetermined"}


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def _int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _float(value):
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def build_where(params, skip: tuple = ()) -> tuple[str, list]:
    """Translate /books query parameters into a WHERE clause over books b."""
    p = {k: (params.get(k) or "").strip() for k in FILTER_KEYS if k not in skip}
    clauses, args = ["NOT b.hidden"], []

    if p.get("q"):
        like = f"%{p['q']}%"
        clauses.append("(b.tsv @@ websearch_to_tsquery('english', %s) OR b.title ILIKE %s"
                       " OR array_to_string(b.authors, ' ') ILIKE %s OR b.series ILIKE %s OR b.isbn = %s)")
        args += [p["q"], like, like, like, p["q"].replace("-", "")]
    for key, column in (("author", "authors"), ("subject", "subjects"), ("language", "languages")):
        if p.get(key):
            clauses.append(f"b.{column} @> ARRAY[%s]::text[]")
            args.append(p[key])
    if p.get("format"):
        clauses.append("b.formats @> ARRAY[%s]::text[]")
        args.append(p["format"].upper())
    for key in ("series", "publisher", "library"):
        if p.get(key):
            clauses.append(f"b.{key} = %s")
            args.append(p[key])

    award, result = p.get("award"), p.get("award_result")
    if award or result in AWARD_RESULTS:
        sub = ["a.book_id = b.id"]
        if award and award != "any":
            sub.append("a.award = %s")
            args.append(award)
        if result in AWARD_RESULTS:
            sub.append("a.result = %s")
            args.append(result)
        clauses.append(f"EXISTS (SELECT 1 FROM book_awards a WHERE {' AND '.join(sub)})")

    min_rating = _float(p.get("min_rating"))
    if min_rating:
        clauses.append("b.rating >= %s")
        args.append(min_rating)
    year_from, year_to, decade = _int(p.get("year_from")), _int(p.get("year_to")), _int(p.get("decade"))
    if decade is not None:
        clauses.append("b.pub_year BETWEEN %s AND %s")
        args += [decade, decade + 9]
    if year_from is not None:
        clauses.append("b.pub_year >= %s")
        args.append(year_from)
    if year_to is not None:
        clauses.append("b.pub_year <= %s")
        args.append(year_to)

    # Calibre keeps records of books whose files are gone. They are listed as
    # references, marked "not on disk", unless the view asks for one side only;
    # a work counts as on disk when any copy of it is.
    status = p.get("status")
    if status == "available":
        clauses.append("b.available")
    elif status == "missing":
        clauses.append("NOT b.available AND NOT EXISTS (SELECT 1 FROM books b2"
                       " WHERE b2.work_key = b.work_key AND b2.available AND NOT b2.hidden)")
    if p.get("source") == "calibre":
        clauses.append("b.calibre IS NOT NULL")
    elif p.get("source") == "loose":
        clauses.append("b.calibre IS NULL")
    if p.get("review") == "1":
        clauses.append("b.needs_review")
    if p.get("no_author") == "1":
        clauses.append("cardinality(b.authors) = 0")
    if p.get("swapped") == "1":
        clauses.append("b.swapped")
    if p.get("starred") == "1":
        clauses.append("EXISTS (SELECT 1 FROM book_favourites bf WHERE bf.book_id = b.id)")
    if p.get("label"):
        clauses.append("EXISTS (SELECT 1 FROM book_labels bl JOIN tags t ON t.id = bl.tag_id"
                       " WHERE bl.book_id = b.id AND lower(t.name) = lower(%s))")
        args.append(p["label"])
    return " AND ".join(clauses), args


def active_params(params) -> dict:
    return {k: params[k].strip() for k in FILTER_KEYS if (params.get(k) or "").strip()}


def books_url(params: dict, **changes) -> str:
    """/books URL for params with changes applied (None removes a key)."""
    merged = {k: v for k, v in params.items() if k in FILTER_KEYS + ("sort",)}
    for k, v in changes.items():
        if v is None:
            merged.pop(k, None)
        else:
            merged[k] = v
    qs = urlencode({k: v for k, v in merged.items() if v not in ("", None)})
    return "/books" + ("?" + qs if qs else "")


def _rating_label(value) -> str:
    n = int(value)
    return "★" * n + "☆" * (5 - n)


def value_label(kind: str, value) -> str:
    if kind == "rating":
        return _rating_label(_float(value) or 0) + " & up"
    if kind == "decade":
        return f"{value}s"
    if kind == "language":
        return LANGUAGE_NAMES.get(str(value), str(value))
    if kind == "status":
        return STATUS_LABELS.get(str(value), str(value))
    return str(value)


STATUS_LABELS = {"available": "on disk only", "missing": "references only (not on disk)"}

FILTER_LABELS = {"q": "Search", "author": "Author", "series": "Series", "subject": "Tag", "publisher": "Publisher",
                 "language": "Language", "format": "Format", "library": "Library", "award": "Award",
                 "award_result": "Award result", "min_rating": "Rating", "year_from": "From", "year_to": "To",
                 "decade": "Decade", "status": "Status", "source": "Source", "review": "Needs review",
                 "swapped": "Swapped title/author", "no_author": "No author", "starred": "Starred", "label": "Label"}


def filter_chips(params: dict) -> list[dict]:
    chips = []
    for key, value in params.items():
        if key not in FILTER_LABELS:
            continue
        if key == "min_rating":
            shown = value_label("rating", value)
        elif key == "decade":
            shown = value_label("decade", value)
        elif key in ("language", "status"):
            shown = value_label(key, value)
        elif key in ("review", "swapped", "no_author", "starred"):
            shown = "yes"
        elif key == "award" and value == "any":
            shown = "any"
        else:
            shown = value
        chips.append({"label": FILTER_LABELS[key], "value": shown, "remove": books_url(params, **{key: None})})
    return chips


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

# The grid, category counts and library totals each read the whole catalogue (about 200,000 rows,
# seconds of work), yet change only when a scan, resolve, star or label writes to these tables.
# Postgres counts the rows written to each table, so one small query tells whether a kept result
# still holds. Each result depends only on the tables it reads: starring a book leaves the
# category counts alone unless the view is filtered to starred books.
CACHE_SIZE = 64
_cache: OrderedDict = OrderedDict()
_cache_lock = threading.Lock()


def data_version(tables: list[str]) -> int:
    return query("SELECT COALESCE(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint AS v"
                 " FROM pg_stat_user_tables WHERE relname = ANY(%s)", (tables,))[0]["v"]


def filter_tables(params) -> list[str]:
    """Tables that build_where reads beyond books and book_awards."""
    return (["book_favourites"] if params.get("starred") else []) + \
           (["book_labels", "tags"] if params.get("label") else [])


def _freeze(value):
    if hasattr(value, "items"):
        return tuple(sorted((k, str(v)) for k, v in value.items()))
    return value


def cached(tables):
    """Keep a function's result per arguments until one of tables (a list, or a function of the
    arguments giving one) is written to."""
    def decorate(fn):
        @wraps(fn)
        def wrapper(*args):
            key = (fn.__name__,) + tuple(_freeze(a) for a in args)
            version = data_version(tables(*args) if callable(tables) else tables)   # before fn runs
            with _cache_lock:
                hit = _cache.get(key)
                if hit and hit[0] == version:
                    _cache.move_to_end(key)
                    return hit[1]
            value = fn(*args)
            with _cache_lock:
                _cache[key] = (version, value)
                _cache.move_to_end(key)
                while len(_cache) > CACHE_SIZE:
                    _cache.popitem(last=False)
            return value
        return wrapper
    return decorate


def warm_cache():
    """Fill the landing page's results in the background, so the first visit after a start is quick."""
    def run():
        try:
            card_page({}), facets({}), library_stats()
        except Exception as e:      # the database may still be starting; the first visit fills it instead
            print(f"   Books cache not warmed: {e}", flush=True)
    threading.Thread(target=run, daemon=True, name="books-cache-warm").start()


@cached(lambda params, per_page=PER_PAGE: ["books", "book_awards", "book_favourites"] + filter_tables(params))
def card_page(params, per_page: int = PER_PAGE):
    """One page of book cards. Copies of the same work (same work_key) collapse
    into one card represented by its best copy: on disk, in Calibre, most
    confident, most files."""
    where, args = build_where(params)
    q = (params.get("q") or "").strip()
    default = "relevance" if q else "series" if params.get("series") else "author" if params.get("author") else "title"
    sort_key, order = pick_sort(params.get("sort"), default, SORT_OPTIONS)
    if sort_key == "relevance" and not q:
        sort_key, order = "title", SORT_OPTIONS["title"]
    page = positive_page(params.get("page"))
    rank = "ts_rank(b.tsv, websearch_to_tsquery('english', %s))" if q else "0"
    rank_args = [q] if q else []

    rows = query(f"""
        WITH f AS MATERIALIZED (
            SELECT b.id, COALESCE(b.work_key, 'id' || b.id) AS card_key,
                   COALESCE(b.title, regexp_replace(b.directory, '^.*/', '')) AS title,
                   b.sort_title, b.authors, b.author_sort, b.series, b.series_index, b.pub_year, b.rating,
                   b.formats, b.available, b.cover_path IS NOT NULL OR b.cover_member IS NOT NULL AS has_cover,
                   b.library, b.needs_review, b.swapped, b.added_at, b.calibre IS NOT NULL AS in_calibre,
                   b.confidence, b.file_count, b.directory, b.isbn, b.goodreads_id, {rank} AS rank
            FROM books b WHERE {where}
        ),
        cards AS (
            SELECT DISTINCT ON (card_key) *,
                   count(*) OVER (PARTITION BY card_key) AS copies,
                   max(rank) OVER (PARTITION BY card_key) AS best_rank
            FROM f
            ORDER BY card_key, available DESC, in_calibre DESC, confidence DESC NULLS LAST, file_count DESC, id
        )
        SELECT c.*, count(*) OVER () AS total, count(*) FILTER (WHERE c.available) OVER () AS on_disk_total,
               EXISTS (SELECT 1 FROM book_favourites bf WHERE bf.book_id = c.id) AS starred,
               -- Formats and wins of the card's copies within the filter, found through the work_key
               -- index for the page's cards only (scanning f for each card took over a second).
               (SELECT array_agg(DISTINCT x ORDER BY x) FROM books b, unnest(b.formats) x
                 WHERE (b.work_key = c.card_key OR b.id = c.id) AND {where}) AS all_formats,
               (SELECT array_agg(DISTINCT a.award ORDER BY a.award) FROM books b JOIN book_awards a ON a.book_id = b.id
                 WHERE (b.work_key = c.card_key OR b.id = c.id) AND {where} AND a.result = 'winner') AS wins
        FROM cards c
        ORDER BY {order}
        LIMIT %s OFFSET %s
    """, rank_args + args + args + args + [per_page, (page - 1) * per_page])

    if rows:
        total = rows[0]["total"]
    else:
        total = query(f"SELECT count(DISTINCT COALESCE(b.work_key, 'id' || b.id)) AS n FROM books b WHERE {where}",
                      args)[0]["n"]
    for r in rows:
        r["goodreads"] = goodreads_url(r)
    total_pages = max(1, -(-total // per_page))
    return rows, total, page, total_pages, sort_key


@cached(lambda params: ["books", "book_awards"] + filter_tables(params))
def _facets(params) -> dict:
    where, args = build_where(params)
    rows = query(f"""
        WITH f AS MATERIALIZED (
            SELECT b.id, COALESCE(b.work_key, 'id' || b.id) AS card_key, b.authors, b.series, b.subjects,
                   b.publisher, b.languages, b.formats, b.library, b.rating, b.pub_year
            FROM books b WHERE {where}
        ),
        facet AS (
            SELECT 'author' AS kind, x AS value, count(DISTINCT card_key) AS n FROM f, unnest(authors) x GROUP BY x
            UNION ALL SELECT 'series', series, count(DISTINCT card_key) FROM f WHERE series IS NOT NULL GROUP BY series
            UNION ALL SELECT 'subject', x, count(DISTINCT card_key) FROM f, unnest(subjects) x GROUP BY x
            UNION ALL SELECT 'award', a.award, count(DISTINCT f.card_key) FROM f JOIN book_awards a ON a.book_id = f.id
                      GROUP BY a.award
            UNION ALL SELECT 'publisher', publisher, count(DISTINCT card_key) FROM f WHERE publisher IS NOT NULL
                      GROUP BY publisher
            UNION ALL SELECT 'rating', floor(rating)::int::text, count(DISTINCT card_key) FROM f
                      WHERE rating >= 1 GROUP BY floor(rating)
            UNION ALL SELECT 'decade', (pub_year / 10 * 10)::text, count(DISTINCT card_key) FROM f
                      WHERE pub_year IS NOT NULL GROUP BY pub_year / 10
            UNION ALL SELECT 'language', x, count(DISTINCT card_key) FROM f, unnest(languages) x GROUP BY x
            UNION ALL SELECT 'format', x, count(DISTINCT card_key) FROM f, unnest(formats) x GROUP BY x
            UNION ALL SELECT 'library', library, count(DISTINCT card_key) FROM f WHERE library IS NOT NULL
                      GROUP BY library
        )
        SELECT kind, value, n, distinct_values FROM (
            SELECT kind, value, n, count(*) OVER (PARTITION BY kind) AS distinct_values,
                   row_number() OVER (PARTITION BY kind ORDER BY n DESC, value) AS rn
            FROM facet
        ) x
        WHERE rn <= %s OR kind = ANY(%s)
    """, args + [FACET_LIMIT, list(FACET_FULL)])

    out = {c["kind"]: {**c, "values": [], "distinct": 0} for c in CATEGORIES}
    for r in rows:
        cat = out[r["kind"]]
        cat["distinct"] = r["distinct_values"]
        cat["values"].append({"value": r["value"], "n": r["n"], "label": value_label(r["kind"], r["value"])})
    for kind in ("rating", "decade"):
        out[kind]["values"].sort(key=lambda v: -int(v["value"]))
    return out


def category_values(kind: str, params, find: str = "", order: str = "name") -> list[dict]:
    """Every value of one category within the current filter (the category's own filter is ignored)."""
    cat = CATEGORY_BY_KIND[kind]
    where, args = build_where(params, skip=(cat["param"],))
    expr = {
        "author": ("x", "FROM f, unnest(f.authors) x"),
        "series": ("f.series", "FROM f WHERE f.series IS NOT NULL"),
        "subject": ("x", "FROM f, unnest(f.subjects) x"),
        "award": ("a.award", "FROM f JOIN book_awards a ON a.book_id = f.id"),
        "publisher": ("f.publisher", "FROM f WHERE f.publisher IS NOT NULL"),
        "rating": ("floor(f.rating)::int::text", "FROM f WHERE f.rating >= 1"),
        "decade": ("(f.pub_year / 10 * 10)::text", "FROM f WHERE f.pub_year IS NOT NULL"),
        "language": ("x", "FROM f, unnest(f.languages) x"),
        "format": ("x", "FROM f, unnest(f.formats) x"),
        "library": ("f.library", "FROM f WHERE f.library IS NOT NULL"),
    }[kind]
    value_sql, from_sql = expr
    # Calibre lists authors by surname.
    sort_sql = "regexp_replace(x, '^(.*)\\s(\\S+)$', '\\2, \\1')" if kind == "author" else value_sql
    having, having_args = "", []
    if find:
        having = f"HAVING {value_sql} ILIKE %s"
        having_args = [f"%{find}%"]
    order_sql = "n DESC, lower(sort_key)" if order == "count" else "lower(sort_key)"
    if kind in ("rating", "decade") and order != "count":
        order_sql = "value::int DESC"
    return query(f"""
        WITH f AS MATERIALIZED (
            SELECT b.id, COALESCE(b.work_key, 'id' || b.id) AS card_key, b.authors, b.series, b.subjects,
                   b.publisher, b.languages, b.formats, b.library, b.rating, b.pub_year
            FROM books b WHERE {where}
        )
        SELECT value, n FROM (
            SELECT {value_sql} AS value, {sort_sql} AS sort_key, count(DISTINCT f.card_key) AS n
            {from_sql}
            GROUP BY {value_sql}
            {having}
        ) v
        ORDER BY {order_sql}
    """, args + having_args)


def facets(params) -> dict:
    """Category counts (distinct works) within the current filter, top values per category."""
    return _facets(active_params(params))      # page and sort leave the counts unchanged


@cached(["books", "book_files"])
def library_stats() -> dict:
    return query("""
        SELECT count(DISTINCT COALESCE(work_key, 'id' || id)) AS works,
               count(*) AS units,
               count(*) FILTER (WHERE available) AS on_disk,
               count(DISTINCT COALESCE(work_key, 'id' || id))
                 - count(DISTINCT COALESCE(work_key, 'id' || id)) FILTER (WHERE available) AS missing,
               count(*) FILTER (WHERE needs_review) AS review,
               count(*) FILTER (WHERE swapped) AS swapped,
               count(*) FILTER (WHERE cardinality(authors) = 0) AS no_author,
               (SELECT count(DISTINCT x) FROM books, unnest(authors) x WHERE NOT hidden) AS authors,
               (SELECT count(DISTINCT series) FROM books WHERE NOT hidden) AS series,
               (SELECT count(*) FROM book_files WHERE present) AS files
        FROM books WHERE NOT hidden
    """)[0]


def goodreads_url(book) -> str:
    if book.get("goodreads_id"):
        return f"https://www.goodreads.com/book/show/{quote(str(book['goodreads_id']))}"
    if book.get("isbn"):
        return f"https://www.goodreads.com/book/isbn/{quote(book['isbn'])}"
    author = (book.get("authors") or [""])[0]
    return "https://www.goodreads.com/search?q=" + quote_plus(f"{book.get('title') or ''} {author}".strip())


def external_links(book) -> list[dict]:
    title, author = book.get("title") or "", (book.get("authors") or [""])[0]
    terms = quote_plus(f"{title} {author}".strip())
    links = [{"label": "Goodreads", "url": goodreads_url(book), "primary": True}]
    if book.get("isfdb_url"):
        links.append({"label": "ISFDB", "url": book["isfdb_url"]})
    else:
        links.append({"label": "ISFDB", "url": "https://www.isfdb.org/cgi-bin/se.cgi?arg=" + quote_plus(title)
                      + "&type=Title"})
    links.append({"label": "Open Library", "url": f"https://openlibrary.org/isbn/{quote(book['isbn'])}"
                  if book.get("isbn") else f"https://openlibrary.org/search?q={terms}"})
    return links


def _best_file(book_id: int) -> dict | None:
    files = query("SELECT id, file_path, format FROM book_files WHERE book_id = %s AND present", (book_id,))
    if not files:
        return None
    rank = {f: i for i, f in enumerate(FORMAT_PREFERENCE)}
    return min(files, key=lambda f: (rank.get((f["format"] or "").upper(), len(rank)), f["file_path"]))


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def _human_size(n) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit in ("B", "KB") else f"{n:.1f} {unit}"
        n /= 1024
    return ""


templates.env.filters.setdefault("human_size", _human_size)
templates.env.globals.setdefault("can_read", lambda formats: any(reading.readable(f) for f in formats or []))


def _label_counts() -> list[dict]:
    return query("""
        SELECT t.name, count(*) AS n FROM tags t JOIN book_labels bl ON bl.tag_id = t.id
        GROUP BY t.name ORDER BY lower(t.name)
    """)


async def books_page(request):
    params = active_params(request.query_params)
    if request.query_params.get("sort"):
        params["sort"] = request.query_params["sort"]
    # The queries run side by side in worker threads, so the server keeps answering meanwhile.
    (cards, total, page, total_pages, sort_key), facet_counts, stats, labels = await asyncio.gather(
        run_in_threadpool(card_page, request.query_params), run_in_threadpool(facets, request.query_params),
        run_in_threadpool(library_stats), run_in_threadpool(_label_counts))
    params["sort"] = sort_key
    filters_only = {k: v for k, v in params.items() if k != "sort"}
    return templates.TemplateResponse(request, "books.html", {
        "cards": cards,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "sort_key": sort_key,
        "sort_labels": {k: v for k, v in SORT_LABELS.items() if k != "relevance" or params.get("q")},
        "params": params,
        "chips": filter_chips(filters_only),
        "facets": facet_counts,
        "stats": stats,
        "labels": labels,
        "page_prefix": books_url(params) + ("&" if "?" in books_url(params) else "?"),
        "category_qs": urlencode(filters_only),
        "url": lambda **kw: books_url(params, page=None, **kw),
    })


async def book_detail(request):
    book_id = request.path_params["id"]
    rows = query("""
        SELECT b.*, EXISTS (SELECT 1 FROM book_favourites bf WHERE bf.book_id = b.id) AS starred
        FROM books b WHERE b.id = %s
    """, (book_id,))
    if not rows:
        return HTMLResponse("Not found", status_code=404)
    book = rows[0]
    files = query("""
        SELECT id, file_path, format, size_bytes, present, error, evidence->>'meta_group' AS meta_group,
               evidence->'meta' AS meta
        FROM book_files WHERE book_id = %s ORDER BY present DESC, format, file_path
    """, (book_id,))
    awards = query("""
        SELECT award, category, year, result FROM book_awards WHERE book_id = %s
        ORDER BY year NULLS LAST, award, category
    """, (book_id,))
    copies = query("""
        SELECT id, title, authors, library, formats, available, directory, calibre IS NOT NULL AS in_calibre
        FROM books WHERE work_key = %s AND id <> %s AND NOT hidden ORDER BY available DESC, library, id
    """, (book["work_key"], book_id)) if book["work_key"] else []
    labels = query("""
        SELECT t.name FROM tags t JOIN book_labels bl ON bl.tag_id = t.id WHERE bl.book_id = %s
        ORDER BY lower(t.name)
    """, (book_id,))
    series_books = query("""
        SELECT DISTINCT ON (COALESCE(work_key, 'id' || id)) id, title, series_index, pub_year, available
        FROM books WHERE series = %s AND NOT hidden AND id <> %s
        ORDER BY COALESCE(work_key, 'id' || id), available DESC, id
    """, (book["series"], book_id)) if book["series"] else []
    series_books.sort(key=lambda r: (r["series_index"] is None, r["series_index"] or 0, r["title"] or ""))
    description = html_to_text(book["description"]) if book["description"] else ""
    cal = book["calibre"] or {}
    pubdate = book["pubdate"]
    pub_label = (pubdate.strftime("%B %Y") if pubdate and (pubdate.month, pubdate.day) != (1, 1)
                 else str(book["pub_year"] or ""))
    return templates.TemplateResponse(request, "book_detail.html", {
        "book": book,
        "files": files,
        "awards": awards,
        "copies": copies,
        "labels": [l["name"] for l in labels],
        "series_books": series_books[:40],
        "description": description,
        "links": external_links(book),
        "calibre": cal,
        "calibre_added": (cal.get("timestamp") or "")[:10],
        "pub_label": pub_label,
        "language_names": [LANGUAGE_NAMES.get(l, l) for l in book["languages"] or []],
        "url": lambda **kw: books_url({}, **kw),
    })


async def book_category(request):
    kind = request.path_params["kind"]
    if kind not in CATEGORY_BY_KIND:
        return HTMLResponse("Unknown category", status_code=404)
    cat = CATEGORY_BY_KIND[kind]
    params = active_params(request.query_params)
    params.pop(cat["param"], None)
    find = (request.query_params.get("find") or "").strip()
    order = "count" if request.query_params.get("order") == "count" else "name"
    values = await run_in_threadpool(category_values, kind, request.query_params, find, order)
    for v in values:
        v["label"] = value_label(kind, v["value"])
        v["url"] = books_url(params, **{cat["param"]: v["value"]})
    return templates.TemplateResponse(request, "book_category.html", {
        "cat": cat,
        "categories": CATEGORIES,
        "values": values,
        "find": find,
        "order": order,
        "params": params,
        "chips": filter_chips(params),
        "context_qs": urlencode(params),
    })


def _read_cover(cover_path, cover_member):
    """A cover file, or an image inside a zip ("archive::member"): Calibre's cover.jpg
    in a library zip (cover_path) or a cover embedded in an EPUB or CBZ (cover_member)."""
    if cover_path and os.path.isfile(cover_path):
        return "file", cover_path
    for ref in (cover_path, cover_member):
        if ref and "::" in ref:
            archive, member = ref.rsplit("::", 1)
            try:
                with zipfile.ZipFile(archive) as zf:
                    return "bytes", (zf.read(member), mimetypes.guess_type(member)[0] or "image/jpeg")
            except (OSError, KeyError, zipfile.BadZipFile):
                continue
    return None, None


async def book_cover(request):
    rows = query("SELECT cover_path, cover_member FROM books WHERE id = %s", (request.path_params["id"],))
    if not rows:
        return Response(status_code=404)
    kind, value = await run_in_threadpool(_read_cover, rows[0]["cover_path"], rows[0]["cover_member"])
    headers = {"Cache-Control": "public, max-age=86400"}
    if kind == "file":
        return FileResponse(value, headers=headers)
    if kind == "bytes":
        data, media_type = value
        return Response(data, media_type=media_type, headers=headers)
    return Response(status_code=404)


# ---------------------------------------------------------------------------
# JSON API
# ---------------------------------------------------------------------------

async def api_books(request):
    cards, total, page, total_pages, sort_key = await run_in_threadpool(card_page, request.query_params)
    return JSONResponse({"results": json_safe(cards), "total": total, "page": page,
                         "total_pages": total_pages, "sort": sort_key})


async def api_book(request):
    rows = query("SELECT * FROM books WHERE id = %s", (request.path_params["id"],))
    if not rows:
        return JSONResponse({"error": "not found"}, status_code=404)
    book = rows[0]
    book.pop("tsv", None)
    book["files"] = query("SELECT id, file_path, format, size_bytes, present, error FROM book_files"
                          " WHERE book_id = %s ORDER BY file_path", (book["id"],))
    book["awards"] = query("SELECT award, category, year, result, raw FROM book_awards WHERE book_id = %s",
                           (book["id"],))
    return JSONResponse(json_safe(book))


def _book_exists(book_id: int) -> bool:
    return bool(query("SELECT 1 FROM books WHERE id = %s", (book_id,)))


def _book_tag_state(book_id: int) -> dict:
    starred = bool(query("SELECT 1 FROM book_favourites WHERE book_id = %s", (book_id,)))
    tags = query("""
        SELECT t.id, t.name FROM tags t JOIN book_labels bl ON bl.tag_id = t.id
        WHERE bl.book_id = %s ORDER BY lower(t.name)
    """, (book_id,))
    return {"book_id": book_id, "starred": starred, "tags": tags}


async def api_book_star(request):
    """POST = star a book, DELETE = unstar. Same semantics as /api/media/{id}/star."""
    book_id = request.path_params["id"]
    if not _book_exists(book_id):
        return JSONResponse({"error": "book not found"}, status_code=404)
    if request.method == "POST":
        execute("INSERT INTO book_favourites (book_id) VALUES (%s) ON CONFLICT (book_id) DO NOTHING", (book_id,))
    else:
        execute("DELETE FROM book_favourites WHERE book_id = %s", (book_id,))
    return JSONResponse({"ok": True, **_book_tag_state(book_id)})


async def api_book_tags(request):
    """All labels with this book's membership (the shape the tag menu expects)."""
    book_id = request.path_params["id"]
    if not _book_exists(book_id):
        return JSONResponse({"error": "book not found"}, status_code=404)
    rows = query("""
        SELECT t.id, t.name,
               EXISTS (SELECT 1 FROM book_labels bl WHERE bl.tag_id = t.id AND bl.book_id = %s) AS applied
        FROM tags t ORDER BY lower(t.name)
    """, (book_id,))
    starred = bool(query("SELECT 1 FROM book_favourites WHERE book_id = %s", (book_id,)))
    return JSONResponse({"tags": rows, "book_id": book_id, "starred": starred})


async def api_book_tag(request):
    """POST = apply a label (auto-stars), DELETE = remove it."""
    book_id, tag_id = request.path_params["id"], request.path_params["tag_id"]
    if not _book_exists(book_id):
        return JSONResponse({"error": "book not found"}, status_code=404)
    if not query("SELECT 1 FROM tags WHERE id = %s", (tag_id,)):
        return JSONResponse({"error": "tag not found"}, status_code=404)
    if request.method == "POST":
        execute("INSERT INTO book_labels (book_id, tag_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (book_id, tag_id))
        execute("INSERT INTO book_favourites (book_id) VALUES (%s) ON CONFLICT (book_id) DO NOTHING", (book_id,))
    else:
        execute("DELETE FROM book_labels WHERE book_id = %s AND tag_id = %s", (book_id, tag_id))
    return JSONResponse({"ok": True, **_book_tag_state(book_id)})


def _open_path(file_path: str, reveal: bool, file_id: int | None = None) -> tuple[int, dict]:
    """Open a book with the server's default reader, or reveal it in Finder. A book
    inside a zip is revealed as its zip, and opened from a copy in the reader cache."""
    archive, member = split_member(file_path)
    if member is not None:
        if reveal:
            file_path = archive
        else:
            ensure_mounted(archive)
            try:
                file_path = reading.unpack_member(file_id, file_path)
            except READ_ERRORS as e:
                return 500, {"error": str(e) or type(e).__name__, "file_path": file_path}
    if not shutil.which("open"):
        result = host_open("reveal", file_path) if reveal else None
        if result and result.get("ok"):
            return 200, {"ok": True, "file_path": file_path}
        return 501, {"error": (result or {}).get("error", "no_open_cmd"), "file_path": file_path}
    ensure_mounted(file_path)
    try:
        result = subprocess.run(["open", "-R", file_path] if reveal else ["open", file_path],
                                capture_output=True, text=True, timeout=10)
    except Exception as e:
        return 500, {"error": str(e), "file_path": file_path}
    if result.returncode != 0:
        return 500, {"error": result.stderr.strip() or "Could not open file", "file_path": file_path}
    return 200, {"ok": True, "file_path": file_path}


async def _open_response(file: dict | None, reveal: bool):
    if not file:
        return JSONResponse({"error": "no file on disk"}, status_code=404)
    status, body = await run_in_threadpool(_open_path, file["file_path"], reveal, file["id"])
    return JSONResponse(body, status_code=status)


async def api_book_open(request):
    return await _open_response(_best_file(request.path_params["id"]), reveal=False)


async def api_book_reveal(request):
    return await _open_response(_best_file(request.path_params["id"]), reveal=True)


async def api_book_file_open(request):
    if request.path_params["action"] not in ("open", "reveal"):
        return JSONResponse({"error": "action must be open or reveal"}, status_code=404)
    rows = query("SELECT id, file_path FROM book_files WHERE id = %s", (request.path_params["file_id"],))
    return await _open_response(rows[0] if rows else None, reveal=request.path_params["action"] == "reveal")


# ---------------------------------------------------------------------------
# In-browser reader (foliate-js; PDFs in the browser's own viewer)
# ---------------------------------------------------------------------------

READER_TYPES = {"epub": "application/epub+zip", "mobi": "application/x-mobipocket-ebook",
                "prc": "application/x-mobipocket-ebook", "azw": "application/vnd.amazon.ebook",
                "azw3": "application/vnd.amazon.mobi8-ebook", "fb2": "application/x-fictionbook+xml",
                "cbz": "application/vnd.comicbook+zip", "pdf": "application/pdf"}
READ_ERRORS = (reading.NotReadable, OSError, zipfile.BadZipFile, subprocess.SubprocessError)


def _read_candidates(book) -> list[dict]:
    """Browser-readable files on disk for this book and its other copies, best format first."""
    rows = query("""
        SELECT f.id, f.format, f.size_bytes, f.file_path, f.book_id, b.library
        FROM book_files f JOIN books b ON b.id = f.book_id
        WHERE f.present AND (f.book_id = %s OR (b.work_key = %s AND NOT b.hidden))
    """, (book["id"], book["work_key"]))
    rows = [dict(r, how=reading.how(r["format"])) for r in rows if reading.readable(r["format"])]
    # Same format: a loose file before one that must first be copied out of a zip.
    rows.sort(key=lambda r: (reading.read_rank(r["format"]), "::" in r["file_path"], r["book_id"] != book["id"],
                             r["file_path"]))
    return rows


async def book_reader(request):
    rows = query("SELECT id, title, authors, work_key FROM books WHERE id = %s", (request.path_params["id"],))
    if not rows:
        return HTMLResponse("Not found", status_code=404)
    book = rows[0]
    candidates = _read_candidates(book)
    wanted = _int(request.query_params.get("file"))
    chosen = next((c for c in candidates if c["id"] == wanted), candidates[0] if candidates else None)
    has_files = bool(candidates) or bool(query("""
        SELECT 1 FROM book_files f JOIN books b ON b.id = f.book_id
        WHERE f.present AND (f.book_id = %s OR b.work_key = %s) LIMIT 1
    """, (book["id"], book["work_key"])))
    return templates.TemplateResponse(request, "book_reader.html", {
        "book": book,
        "candidates": candidates,
        "chosen": chosen,
        "has_files": has_files,
        "reader_data": {
            "bookId": book["id"],
            "fileId": chosen and chosen["id"],
            "explicit": wanted is not None and chosen is not None and chosen["id"] == wanted,
            "candidates": [{"id": c["id"], "format": (c["format"] or "").upper(), "how": c["how"]}
                           for c in candidates],
        },
    })


def _present_file(file_id: int) -> dict | None:
    rows = query("""
        SELECT f.file_path, f.format, b.title FROM book_files f JOIN books b ON b.id = f.book_id
        WHERE f.id = %s AND f.present
    """, (file_id,))
    return rows[0] if rows else None


async def api_book_file_prepare(request):
    """Make a file readable in the browser (converting it once if needed) and return its URL."""
    file_id = request.path_params["file_id"]
    f = _present_file(file_id)
    if not f:
        return JSONResponse({"error": "file is not on disk"}, status_code=404)
    await run_in_threadpool(ensure_mounted, split_member(f["file_path"])[0])
    try:
        _, ext, mode = await run_in_threadpool(reading.prepare, file_id, f["file_path"], f["format"])
    except READ_ERRORS as e:
        return JSONResponse({"error": str(e) or type(e).__name__}, status_code=422)
    stem = re.sub(r'[\\/:*?"<>|%#]+', " ", f["title"] or Path(f["file_path"]).stem).strip()[:120] or "book"
    return JSONResponse({"url": f"/books/files/{file_id}/content/{quote(f'{stem}.{ext}', safe='')}",
                         "mode": mode, "ext": ext})


async def book_file_content(request):
    """Serve a prepared file inline; the URL's name ends in the extension foliate-js detects by."""
    file_id = request.path_params["file_id"]
    f = _present_file(file_id)
    if not f:
        return Response("file is not on disk", status_code=404)
    try:
        path, ext, _ = await run_in_threadpool(reading.prepare, file_id, f["file_path"], f["format"])
    except READ_ERRORS as e:
        return Response(str(e), status_code=422)
    return FileResponse(path, media_type=READER_TYPES.get(ext, "application/octet-stream"))


# ---------------------------------------------------------------------------
# Background scan (same status shape as /api/refresh/status for the nav button)
# ---------------------------------------------------------------------------

SCAN_JOB = BackgroundJob(ROOT_DIR / ".books_scan.log", ROOT_DIR)


def _book_dirs() -> list[str]:
    return [d for d in os.getenv("BOOK_DIRS", "").split(":") if d.strip()]


async def api_scan_start(request):
    dirs = _book_dirs()
    if not dirs:
        return JSONResponse({"error": "BOOK_DIRS not set in .env"}, status_code=400)
    existing = [d for d in dirs if await run_in_threadpool(ensure_mounted, d)]
    if not existing:
        return JSONResponse({"error": "none of BOOK_DIRS is accessible"}, status_code=400)
    cmd = [sys.executable, "-u", "-m", "books.scan", *existing]
    if request.query_params.get("rescan") == "1":
        cmd.append("--rescan")
    if not SCAN_JOB.start([("scan", cmd)]):
        return JSONResponse({"error": "book scan already running"}, status_code=409)
    return JSONResponse({"ok": True, "dirs": existing, "skipped": [d for d in dirs if d not in existing]})


async def api_scan_status(request):
    s = SCAN_JOB.status()
    if not s["running"] and s["last_result"] and s["last_result"]["ok"]:
        st = await run_in_threadpool(library_stats)
        s["last_result"] = {"ok": True, "summary": f"{st['works']:,} books, {st['review']:,} need review"}
    return JSONResponse(json_safe(s))


async def api_scan_cancel(request):
    pid = SCAN_JOB.cancel()
    return JSONResponse({"ok": pid is not None, "pid": pid})


routes = [
    Route("/books", books_page),
    Route("/books/category/{kind}", book_category),
    Route("/books/cover/{id:int}", book_cover),
    Route("/books/{id:int}", book_detail),
    Route("/books/{id:int}/read", book_reader),
    Route("/books/files/{file_id:int}/content/{name:str}", book_file_content),
    Route("/api/books", api_books),
    Route("/api/books/scan", api_scan_start, methods=["POST"]),
    Route("/api/books/scan/status", api_scan_status),
    Route("/api/books/scan/cancel", api_scan_cancel, methods=["POST"]),
    Route("/api/books/{id:int}", api_book),
    Route("/api/books/{id:int}/star", api_book_star, methods=["POST", "DELETE"]),
    Route("/api/books/{id:int}/tags", api_book_tags),
    Route("/api/books/{id:int}/tags/{tag_id:int}", api_book_tag, methods=["POST", "DELETE"]),
    Route("/api/books/{id:int}/open", api_book_open, methods=["POST"]),
    Route("/api/books/{id:int}/reveal", api_book_reveal, methods=["POST"]),
    Route("/api/books/files/{file_id:int}/prepare", api_book_file_prepare),
    Route("/api/books/files/{file_id:int}/{action:str}", api_book_file_open, methods=["POST"]),
]
