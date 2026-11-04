"""List Calibre records whose book folder is gone, with every name they could be found under."""
import json, os, sqlite3, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
from books.naming import norm_name, norm_title

DATA = os.path.join(ROOT, "tools", "data", "lost")
os.makedirs(DATA, exist_ok=True)
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
LABEL = LOCAL["decade_label"]
out = []
for dec in LOCAL["missing_decades"]:
    lib = f"{LOCAL['decade_root']}{dec}s"
    con = sqlite3.connect(f"{DATA}/{LABEL.lower()}{dec}.db")
    authors = {}
    for book, name in con.execute("SELECT l.book, a.name FROM books_authors_link l JOIN authors a ON a.id = l.author ORDER BY l.id"):
        authors.setdefault(book, []).append(name)
    data = {}
    for book, fmt, size, name in con.execute("SELECT book, format, uncompressed_size, name FROM data"):
        data.setdefault(book, []).append({"format": fmt, "size": size, "name": name})
    on_disk = set(os.listdir(lib))
    listing = {}
    for bid, title, path, added in con.execute("SELECT id, title, path, timestamp FROM books"):
        author_dir, book_dir = path.split("/", 1)
        if author_dir in on_disk and author_dir not in listing:
            try:
                listing[author_dir] = set(os.listdir(os.path.join(lib, author_dir)))
            except OSError:
                listing[author_dir] = set()
        if book_dir in listing.get(author_dir, ()):
            continue
        out.append({"library": f"{LABEL} {dec}s", "id": bid, "title": title, "authors": authors.get(bid, []),
                    "path": path, "added": (added or "")[:10], "files": data.get(bid, []),
                    "key": f"{norm_title(title)}|{norm_name((authors.get(bid) or [''])[0])}"})
json.dump(out, open(f"{DATA}/missing.json", "w"))
print(len(out), "missing records;", sum(len(r["files"]) for r in out), "stored files;",
      len({r["key"] for r in out}), "distinct title|author keys")
