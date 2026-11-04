"""Time metadata extraction on a random sample of book files (read-only)."""
import os, random, sys, time
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import query
from books.formats import extract
from books.naming import EBOOK_EXTENSIONS
random.seed(7)
dirs = [r["directory"] for r in query("select directory from books order by random() limit 60")]
files = []
for d in dirs:
    try:
        for e in os.scandir(d):
            n = e.name.lower()
            if e.is_file() and any(n.endswith(x) for x in EBOOK_EXTENSIONS):
                files.append((e.path, e.stat().st_size))
    except OSError:
        pass
files = files[:45]
tot = 0
for p, size in files:
    t = time.time()
    try:
        ev = extract(p)
        err = ev.get("error")
    except Exception as ex:
        err = repr(ex)
    dt = time.time() - t
    tot += dt
    print(f"{dt:6.2f}s {size/1e6:7.1f}MB {os.path.splitext(p)[1]:6} {err or ''} {os.path.basename(p)[:60]}")
print("total", round(tot, 1), "files", len(files))
