"""
Books scanner: walk book folders, group ebook files into book units, and store
the evidence each file carries about itself (embedded metadata, front-matter
text clues, filename). Calibre libraries found along the way are read as one
more source of evidence; their catalogue entries whose files are gone become
units with no files, so stale Calibre records show up as missing books. A
library archived as one zip per author (Author/Title (id)/ folders inside each
zip) is read in place: each book inside becomes a file "archive.zip::member".

Resolution (deciding title, authors, series, ...) is a separate stage that
reads only the database: python -m books.resolve. This scanner runs it at the
end unless --no-resolve is given.

Usage:
    python -m books.scan                       # uses BOOK_DIRS from .env
    python -m books.scan /path/one /path/two   # explicit roots
    python -m books.scan --rescan              # re-extract unchanged files too
    python -m books.scan --workers 12          # parallel file reads (SMB latency bound)
"""
import argparse
import os
import re
import sqlite3
import sys
import time
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2.extras
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn, query
from books.calibre import library_label, load_library
from books.formats import MEMBER_SEP, extract, read_sidecar_opf, sanitize_json
from books.naming import CALIBRE_ONLY_EXTENSIONS, EBOOK_EXTENSIONS, clean_stem, file_stem, norm_text
from books.schema import ensure_books_schema

SCAN_EXTENSIONS = set(EBOOK_EXTENSIONS) | {".html", ".htm"}
TEXT_LIKE = {".txt", ".rtf", ".doc", ".html", ".htm"}
TEXT_MIN_BYTES = 20_000          # loose text files smaller than this are readmes, not books
JUNK_NAME = re.compile(r"^(?:readme|read me|info|license|licence|nfo|torrent|description|file_id)\b|downloaded from", re.I)
BATCH = 100


def book_dirs_from_env() -> list[str]:
    raw = os.getenv("BOOK_DIRS", "")
    return [d.strip() for d in raw.split(":") if d.strip()]


def walk_root(root: str):
    """Walk one root. Returns (files, dir_info, calibre_dbs, complete).

    complete is False when any directory could not be listed (dropped mount,
    permissions); the caller must then not mark unseen files as missing.
    """
    files, dir_info, dbs = [], {}, []
    complete = True
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                entries = list(it)
        except OSError as e:
            print(f"  ⚠ cannot list {d}: {e}")
            complete = False
            continue
        info = {"opf": False, "cover": False}
        for e in entries:
            name = e.name
            try:
                if e.is_dir(follow_symlinks=False):
                    if not name.startswith(("@", ".", "#")) and name != "__MACOSX":
                        stack.append(e.path)
                    continue
                if name == "metadata.db":
                    dbs.append(e.path)
                elif name == "metadata.opf":
                    info["opf"] = True
                elif name == "cover.jpg":
                    info["cover"] = True
                elif not name.startswith(".") and file_stem(name)[1] in SCAN_EXTENSIONS:
                    st = e.stat(follow_symlinks=False)
                    files.append((e.path, st.st_size, st.st_mtime))
            except OSError:
                complete = False
        dir_info[d] = info
    return files, dir_info, dbs, complete


def _calibre_library_for(directory: str, library_roots: set) -> str | None:
    """A Calibre book folder sits exactly two levels below its library root."""
    grandparent = os.path.dirname(os.path.dirname(directory))
    return grandparent if grandparent in library_roots else None


def _fold(path: str) -> str:
    return unicodedata.normalize("NFC", path).casefold()


def folder_index(calibre_by_dir: dict) -> dict:
    """Calibre book folders by case- and normalization-folded path: Calibre ran on
    filesystems that ignore case, so a zip may hold "The diamond age (230)" for
    the catalogue's "The Diamond Age (230)"."""
    return {_fold(d): d for d in calibre_by_dir}


def _library_zip_books(zip_path: str, library_root: str, folders: dict) -> tuple[list, dict]:
    """Books inside a zip kept in a Calibre library's own folder, matched to that
    library's book folders (folders: folder_index()). Returns ([(member path,
    size, book folder)], {book folder: cover member path}); both are empty for
    any other zip."""
    books, covers = [], {}
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            parts = info.filename.split("/")
            if len(parts) != 3 or not parts[2]:
                continue
            book_dir = folders.get(_fold(os.path.join(library_root, parts[0], parts[1])))
            if book_dir is None:
                continue
            ref = f"{zip_path}{MEMBER_SEP}{info.filename}"
            if parts[2] == "cover.jpg":
                covers[book_dir] = ref
            elif file_stem(parts[2])[1] in SCAN_EXTENSIONS:
                books.append((ref, info.file_size, book_dir))
    return books, covers


def group_units(files, dir_info: dict, file_root: dict, library_roots: set, calibre_by_dir: dict):
    """Group files into book units. Returns (units, file_unit, kept, storage):
    kept is the (path, size, mtime) list to store, storage the library zips read
    as their books rather than as files of their own. file_root gains the roots of
    books inside those zips."""
    storage, covers, members = set(), {}, []
    folders = None
    for path, size, mtime in files:
        d = os.path.dirname(path)
        if d not in library_roots or not path.lower().endswith(".zip"):
            continue
        folders = folders or folder_index(calibre_by_dir)
        try:
            found, found_covers = _library_zip_books(path, d, folders)
        except (OSError, zipfile.BadZipFile) as e:
            print(f"  ⚠ cannot list {path}: {e}")
            continue
        if found:
            storage.add(path)
            covers.update(found_covers)
            for ref, member_size, book_dir in found:
                members.append((ref, member_size, mtime, book_dir))
                file_root[ref] = file_root[path]

    units, file_unit, kept = {}, {}, []
    for path, size, mtime in files:
        if path in storage:
            continue
        d = os.path.dirname(path)
        library_root = _calibre_library_for(d, library_roots)
        # A library's own folder is never a book folder, even with a stray metadata.opf in it.
        calibre_dir = bool(library_root) or (dir_info.get(d, {}).get("opf", False) and d not in library_roots)
        if not calibre_dir and not _keep_loose_file(path, size):
            continue
        if calibre_dir:
            key = "dir:" + d
        else:
            stem = clean_stem(file_stem(os.path.basename(path))[0])[0]
            key = f"file:{d}/{norm_text(stem) or stem.lower()}"
        if key not in units:
            cal = calibre_by_dir.get(d)
            if cal is None and calibre_dir and dir_info.get(d, {}).get("opf"):
                sidecar = read_sidecar_opf(os.path.join(d, "metadata.opf"))
                cal = _sidecar_as_calibre(sidecar, library_label(library_root) if library_root else None) if sidecar else None
            units[key] = {
                "root": file_root[path], "directory": d, "calibre_dir": calibre_dir,
                "library": (cal or {}).get("library"), "calibre": cal,
                "cover_path": os.path.join(d, "cover.jpg") if dir_info.get(d, {}).get("cover") else None,
            }
        file_unit[path] = key
        kept.append((path, size, mtime))

    for ref, size, mtime, book_dir in members:
        key = "dir:" + book_dir
        if key not in units:
            cal = calibre_by_dir[book_dir]
            units[key] = {"root": file_root[ref], "directory": book_dir, "calibre_dir": True,
                          "library": cal["library"], "calibre": cal, "cover_path": covers.get(book_dir)}
        file_unit[ref] = key
        kept.append((ref, size, mtime))
    if storage:
        print(f"   {len(members):,} books inside {len(storage):,} zips in Calibre library folders", flush=True)
    return units, file_unit, kept, storage


def _keep_loose_file(path: str, size: int) -> bool:
    name = os.path.basename(path)
    _, ext = file_stem(name)
    if ext in CALIBRE_ONLY_EXTENSIONS or ext in (".html", ".htm"):
        return False
    if ext in TEXT_LIKE and (size < TEXT_MIN_BYTES or JUNK_NAME.search(name)):
        return False
    return True


def _sidecar_as_calibre(opf_meta: dict, library: str | None) -> dict:
    tags = opf_meta.get("subjects") or []
    return {
        "library": library, "calibre_id": None, "title": opf_meta.get("title"),
        "authors": opf_meta.get("authors") or [], "author_sort": None,
        "series": opf_meta.get("series"), "series_index": opf_meta.get("series_index"),
        "tags": tags, "isfdb": "_isfdb" in tags, "publisher": opf_meta.get("publisher"),
        "languages": [opf_meta["language"]] if opf_meta.get("language") else [],
        "pubdate": (opf_meta.get("date") or "")[:10] or None, "timestamp": None,
        "rating": opf_meta.get("rating"), "description": opf_meta.get("description"),
        "identifiers": opf_meta.get("identifiers") or {}, "formats": [], "has_cover": False,
        "sidecar": True,
    }


def _upsert_units(cur, units: dict) -> dict:
    rows = [(key, u["root"], u["directory"], u["calibre_dir"], u["library"],
             psycopg2.extras.Json(sanitize_json(u["calibre"])) if u["calibre"] else None, u["cover_path"])
            for key, u in units.items()]
    ids = {}
    for i in range(0, len(rows), 1000):
        result = psycopg2.extras.execute_values(cur, """
            INSERT INTO books (unit_key, root, directory, calibre_dir, library, calibre, cover_path)
            VALUES %s
            ON CONFLICT (unit_key) DO UPDATE SET
                root = EXCLUDED.root,
                directory = EXCLUDED.directory,
                calibre_dir = EXCLUDED.calibre_dir,
                library = COALESCE(EXCLUDED.library, books.library),
                calibre = COALESCE(EXCLUDED.calibre, books.calibre),
                cover_path = EXCLUDED.cover_path,
                updated_at = NOW()
            RETURNING id, unit_key
        """, rows[i:i + 1000], fetch=True)
        ids.update({key: book_id for book_id, key in result})
    return ids


_FILE_UPSERT = """
    INSERT INTO book_files (book_id, file_path, format, size_bytes, mtime, present,
                            evidence, error, extracted_at, last_seen_at)
    VALUES %s
    ON CONFLICT (file_path) DO UPDATE SET
        book_id = EXCLUDED.book_id, format = EXCLUDED.format,
        size_bytes = EXCLUDED.size_bytes, mtime = EXCLUDED.mtime, present = true,
        evidence = EXCLUDED.evidence, error = EXCLUDED.error,
        extracted_at = EXCLUDED.extracted_at, last_seen_at = NOW()
"""
_FILE_TEMPLATE = "(%s, %s, %s, %s, %s, true, %s, %s, NOW(), NOW())"


def _copies_read_before(conn, todo: list) -> dict:
    """{path: evidence} for files to read that have the same name and size as a file read before:
    a copy of a library inside another folder is not read again. A file's own earlier reading
    never counts, since it is in the list because it changed."""
    if not todo:
        return {}
    base = lambda path: re.split(r"[/:]", path)[-1]
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE scan_todo (path text PRIMARY KEY, base text, size bigint) ON COMMIT DROP")
        psycopg2.extras.execute_values(cur, "INSERT INTO scan_todo VALUES %s",
                                       [(path, base(path), size) for path, size, _ in todo], page_size=5000)
        cur.execute("""
            SELECT DISTINCT ON (t.path) t.path, f.evidence
            FROM scan_todo t JOIN book_files f
              ON f.size_bytes = t.size AND regexp_replace(f.file_path, '^.*[/:]', '') = t.base
            WHERE f.extracted_at IS NOT NULL AND f.evidence IS NOT NULL AND f.error IS NULL
              AND NOT EXISTS (SELECT 1 FROM scan_todo o WHERE o.path = f.file_path)
            ORDER BY t.path, f.extracted_at DESC
        """)
        found = dict(cur.fetchall())
    conn.commit()
    return found


def _flush_files(conn, rows: list):
    """Write a batch of extracted files. If Postgres rejects the batch, write the
    rows one by one and store any row it still rejects without its evidence, so
    one unstorable file is recorded as read and never stalls later scans."""
    if not rows:
        return
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, _FILE_UPSERT, rows, template=_FILE_TEMPLATE)
        conn.commit()
        return
    except psycopg2.DataError:
        conn.rollback()
    for row in rows:
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _FILE_UPSERT, [row], template=_FILE_TEMPLATE)
            conn.commit()
        except psycopg2.DataError as e:
            conn.rollback()
            book_id, path, fmt, size, mtime, _evidence, _error = row
            reason = f"unstorable: {str(e).splitlines()[0]}"[:300]
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _FILE_UPSERT, [(book_id, path, fmt, size, mtime, None, reason)],
                                               template=_FILE_TEMPLATE)
            conn.commit()


def scan(roots: list[str], rescan: bool = False, workers: int = 8, resolve_after: bool = True):
    print("\n📚 Media Catalog Books Scanner")
    print(f"   Roots: {len(roots)}\n")
    ensure_books_schema()
    started = time.time()
    roots = [os.path.abspath(os.path.expanduser(r)).rstrip("/") for r in roots]

    files, dir_info, dbs, complete_roots = [], {}, [], []
    file_root = {}
    for root in roots:
        if not os.path.isdir(root):
            print(f"  ⚠ Skipping {root} — does not exist")
            continue
        print(f"  Scanning {root} ...", flush=True)
        f, di, d, complete = walk_root(root)
        files.extend(f)
        dir_info.update(di)
        dbs.extend(d)
        file_root.update({p: root for p, _, _ in f})
        if complete:
            complete_roots.append(root)
        else:
            print(f"  ⚠ {root} was not walked completely — missing files will not be flagged")
    print(f"\n   Found {len(files):,} book files in {len(dir_info):,} folders", flush=True)

    calibre_by_dir = {}
    library_roots = set()
    for db_path in sorted(dbs):
        try:
            books = load_library(db_path)
        except (OSError, sqlite3.Error) as e:
            # Still a library: its book folders fall back to their metadata.opf sidecars.
            library_roots.add(os.path.dirname(db_path))
            print(f"  ⚠ Calibre library {db_path.rsplit('/', 2)[-2]} unreadable ({e}); using metadata.opf sidecars",
                  flush=True)
            continue
        if books:
            library_roots.add(os.path.dirname(db_path))
            calibre_by_dir.update(books)
            print(f"   Calibre library {db_path.rsplit('/', 2)[-2]}: {len(books):,} entries", flush=True)

    def root_of(path: str) -> str:
        return next((r for r in roots if path.startswith(r + "/")), roots[0])

    units, file_unit, kept, storage = group_units(files, dir_info, file_root, library_roots, calibre_by_dir)

    # Calibre entries whose folder holds no book files: catalogued but missing.
    catalogue_only = 0
    for book_dir, cal in calibre_by_dir.items():
        key = "dir:" + book_dir
        if key not in units:
            units[key] = {"root": root_of(book_dir), "directory": book_dir, "calibre_dir": True,
                          "library": cal["library"], "calibre": cal,
                          "cover_path": os.path.join(book_dir, "cover.jpg") if dir_info.get(book_dir, {}).get("cover") else None}
            catalogue_only += 1
    print(f"   {len(units):,} book units ({catalogue_only:,} Calibre entries with no files on disk)", flush=True)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            unit_ids = _upsert_units(cur, units)
        conn.commit()

        existing = {}
        for root in {file_root[p] for p, _, _ in kept}:
            for r in query("SELECT file_path, size_bytes, mtime, extracted_at FROM book_files "
                           "WHERE starts_with(file_path, %s)", (root + "/",)):
                existing[r["file_path"]] = r
        todo, unchanged = [], []
        for path, size, mtime in kept:
            old = existing.get(path)
            if (not rescan and old and old["extracted_at"] and old["size_bytes"] == size
                    and old["mtime"] is not None and abs(old["mtime"] - mtime) < 1):
                unchanged.append((unit_ids[file_unit[path]], path))
            else:
                todo.append((path, size, mtime))
        copies = {} if rescan else _copies_read_before(conn, todo)
        if copies:
            copied = [(unit_ids[file_unit[path]], path, copies[path]["format"], size, mtime,
                       psycopg2.extras.Json(copies[path]), None) for path, size, mtime in todo if path in copies]
            for i in range(0, len(copied), 2000):
                _flush_files(conn, copied[i:i + 2000])
            todo = [t for t in todo if t[0] not in copies]
        print(f"   {len(unchanged):,} unchanged, {len(copies):,} copies of files read before, "
              f"{len(todo):,} to read\n", flush=True)

        with conn.cursor() as cur:
            for i in range(0, len(unchanged), 2000):
                psycopg2.extras.execute_values(cur, """
                    UPDATE book_files f SET book_id = v.book_id, present = true, last_seen_at = NOW()
                    FROM (VALUES %s) AS v(book_id, file_path) WHERE f.file_path = v.file_path
                """, unchanged[i:i + 2000])
        conn.commit()

        rows, errors = [], 0
        pool = ThreadPoolExecutor(max_workers=workers)
        try:
            futures = {pool.submit(extract, path): (path, size, mtime) for path, size, mtime in todo}
            for fut in tqdm(as_completed(futures), total=len(futures), desc="   Reading", mininterval=2):
                path, size, mtime = futures[fut]
                evidence = fut.result()
                errors += bool(evidence.get("error"))
                rows.append((unit_ids[file_unit[path]], path, evidence["format"], size, mtime,
                             psycopg2.extras.Json(evidence), evidence.get("error")))
                if len(rows) >= BATCH:
                    _flush_files(conn, rows)
                    rows = []
            _flush_files(conn, rows)
        finally:
            # On failure, stop at once instead of reading every remaining file first.
            pool.shutdown(wait=True, cancel_futures=True)
        if errors:
            print(f"\n   ⚠ {errors:,} files could not be read (kept with filename evidence only)")

        with conn.cursor() as cur:
            # Library zips are read as the books inside them, not stored as files of their own.
            # A unit keyed on a library's own folder that is left with no files was made by a
            # stray metadata.opf grouping such zips into one "book"; it is not a book.
            if storage:
                cur.execute("DELETE FROM book_files WHERE file_path = ANY(%s)", (sorted(storage),))
            cur.execute("""
                DELETE FROM books b WHERE b.unit_key = ANY(%s)
                  AND NOT EXISTS (SELECT 1 FROM book_files f WHERE f.book_id = b.id)
            """, (["dir:" + r for r in sorted(library_roots)],))
            if cur.rowcount:
                print(f"   Removed {cur.rowcount:,} stray unit(s) made of a Calibre library's own folder")
            for root in complete_roots:
                cur.execute("""
                    UPDATE book_files SET present = false
                    WHERE starts_with(file_path, %s) AND last_seen_at < to_timestamp(%s) AND present
                """, (root + "/", started))
                if cur.rowcount:
                    print(f"   {cur.rowcount:,} files under {root} are no longer on disk")
        conn.commit()
    finally:
        conn.close()

    print(f"\n   Scan finished in {time.time() - started:,.0f}s")
    if resolve_after:
        from books.resolve import resolve
        resolve()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan folders for ebooks")
    parser.add_argument("dirs", nargs="*", help="Folders to scan (default: BOOK_DIRS)")
    parser.add_argument("--rescan", action="store_true", help="Re-read files even if unchanged")
    parser.add_argument("--workers", type=int, default=8, help="Parallel file reads")
    parser.add_argument("--no-resolve", action="store_true", help="Skip the resolve stage")
    args = parser.parse_args()

    dirs = args.dirs or book_dirs_from_env()
    if not dirs:
        print("Error: No folders specified. Pass them as arguments or set BOOK_DIRS in .env")
        sys.exit(1)
    scan(dirs, rescan=args.rescan, workers=args.workers, resolve_after=not args.no_resolve)
