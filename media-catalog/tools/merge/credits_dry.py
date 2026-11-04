"""Which game-book PDFs would the credits re-read change, and how (read-only)."""
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from concurrent.futures import ThreadPoolExecutor
from books import frontpages as fp
files = fp.targets(None, reread_credits=True)
known = fp.known_authors()
groups = {}
for f in files:
    groups.setdefault((f["file_path"].rsplit("/", 1)[-1], f["size_bytes"]), f)
print(len(files), "files", len(groups), "distinct", flush=True)
with ThreadPoolExecutor(4) as pool:
    found = list(pool.map(lambda f: (f, fp.read_front(f["file_path"], f["format"])), groups.values()))
changed = 0
for f, fd in found:
    old = ((f["evidence"] or {}).get("text") or {}).get("bylines") or []
    new = ((fp.merge(f["evidence"], fd, known, [f["title"]] + f["authors"] if f["authors"] else None, credits_win=True).get("text") or {}).get("bylines")) or []
    if new != old:
        changed += 1
        print(f"   {f['file_path'].rsplit('/', 1)[-1][:55]!r:57} {old} -> {new}")
print("changed", changed)
