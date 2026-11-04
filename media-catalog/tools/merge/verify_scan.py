"""After the scan: the zipped library, the stray-opf unit, per-library counts, second-library additions (read-only)."""
import json, os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import query

LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
SECOND = LOCAL["second_library_parent"]

one = lambda sql, *a: query(sql, a or None)[0]
print("zip members on disk:", one("SELECT count(*) n FROM book_files WHERE present AND file_path LIKE '%%::%%'")["n"])
print("Zipped-library units with a zip member:", one("""
    SELECT count(DISTINCT b.id) n FROM books b JOIN book_files f ON f.book_id = b.id
    WHERE f.present AND f.file_path LIKE '%%::%%'""")["n"])
print("unit 82240 (stray metadata.opf):", query("SELECT id, title, available, hidden FROM books WHERE id = 82240") or "gone")
print("library-root units:", query("""SELECT id, title, directory FROM books WHERE unit_key LIKE 'dir:%%'
    AND directory = ANY(%s) AND available""", (LOCAL["library_roots"],)) or "none")
print("--- on disk by library")
for r in query("""SELECT coalesce(library, '(loose)') lib, count(*) n, count(*) FILTER (WHERE cardinality(authors) = 0) noauth,
                  count(*) FILTER (WHERE needs_review) review
                  FROM books WHERE available AND NOT hidden GROUP BY 1 ORDER BY 2 DESC"""):
    print(f"  {r['n']:7} noauth {r['noauth']:6} review {r['review']:6}  {r['lib']}")
print("--- second library by folder")
for r in query("""SELECT split_part(substr(directory, length(%s) + 1), '/', 1) top,
                  count(*) n FROM books WHERE available AND NOT hidden AND directory LIKE %s
                  GROUP BY 1 ORDER BY 2 DESC LIMIT 15""", (SECOND, SECOND + "%")):
    print(f"  {r['n']:7}  {r['top']}")
r = query("""SELECT b.id, f.id fid, f.file_path FROM books b JOIN book_files f ON f.book_id = b.id
             WHERE b.title ILIKE 'Before They Are Hanged%%' AND f.present AND f.file_path LIKE '%%::%%' LIMIT 1""")
print("Before They Are Hanged inside a zip:", r[0] if r else "not found")
if r:
    from books import reading
    path, ext, mode = reading.prepare(r[0]["fid"], r[0]["file_path"], r[0]["file_path"].rsplit(".", 1)[-1].upper())
    print("  reader:", ext, mode, os.path.getsize(path), "bytes")
