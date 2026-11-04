"""Match missing Calibre records against records in every other Calibre library whose book folder exists.

Calibre truncates file and folder names (every Analog issue is 'Analog Science Fiction - Scienc'),
but its database keeps the full title, so record-to-record matching tells issues and editions apart.
"""
import json, os, shutil, sqlite3, sys, tempfile, time
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
from books.naming import norm_name, norm_title

DATA = os.path.join(ROOT, "tools", "data", "lost")
os.makedirs(DATA, exist_ok=True)
SIDECARS = {".opf", ".jpg", ".jpeg", ".png", ".db", ".json"}


def book_files(folder):
    """Files other than Calibre's sidecars: a folder holding only metadata.opf and cover.jpg has no book."""
    try:
        return [n for n in os.listdir(folder)
                if not n.startswith(".") and os.path.splitext(n)[1].lower() not in SIDECARS]
    except OSError:
        return []
missing = json.load(open(os.path.join(DATA, "missing.json")))
by_key = {}
for i, r in enumerate(missing):
    by_key.setdefault(r["key"], set()).add(i)

dbs = []
for name in ("found.jsonl", "found_local.jsonl"):
    for line in open(os.path.join(DATA, name)):
        r = json.loads(line)
        if r["kind"] == "metadata.db" and r["path"] not in dbs:
            dbs.append(r["path"])


def copy_local(path):
    for attempt in range(3):
        try:
            tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
            shutil.copyfile(path, tmp)
            return tmp
        except OSError as e:
            err = e
            time.sleep(2)
    raise err


out = open(os.path.join(DATA, "record_matches.jsonl"), "w")
summary = []
for db in dbs:
    root = os.path.dirname(db)
    try:
        local = copy_local(db)
        con = sqlite3.connect(f"file:{local}?mode=ro", uri=True)
        authors = {}
        for b, a in con.execute("SELECT l.book, a.name FROM books_authors_link l JOIN authors a ON a.id = l.author ORDER BY l.id"):
            authors.setdefault(b, []).append(a)
        books = con.execute("SELECT id, title, path FROM books").fetchall()
        con.close()
        os.unlink(local)
    except Exception as e:
        summary.append({"db": db, "error": str(e)[:120]})
        continue
    tops, listing, present, matched = set(os.listdir(root)), {}, 0, set()
    for bid, title, path in books:
        if "/" not in (path or ""):
            continue
        a_dir, b_dir = path.split("/", 1)
        if a_dir not in tops:
            continue
        if a_dir not in listing:
            try:
                listing[a_dir] = set(os.listdir(os.path.join(root, a_dir)))
            except OSError:
                listing[a_dir] = set()
        if b_dir not in listing[a_dir]:
            continue
        present += 1
        ids = by_key.get(f"{norm_title(title)}|{norm_name((authors.get(bid) or [''])[0])}")
        files = book_files(os.path.join(root, path)) if ids else []
        if ids and files:
            matched |= ids
            out.write(json.dumps({"kind": "calibre-record", "path": os.path.join(root, path), "ids": sorted(ids),
                                  "root": root, "title": title, "files": files}) + "\n")
    out.flush()
    summary.append({"db": db, "records": len(books), "present": present, "missing_matched": len(matched)})
    print(json.dumps(summary[-1]), flush=True)
out.close()
json.dump(summary, open(os.path.join(DATA, "record_summary.json"), "w"), indent=1)
