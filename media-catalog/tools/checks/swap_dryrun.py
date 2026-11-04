"""Resolve Calibre records in memory and report title/author swaps (nothing is written)."""
import json, os, sys, random, collections
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from books.calibre import load_library
from books.resolve import build_context, resolve_unit
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
DBS = [f"{LOCAL['decade_root']}{d}s/metadata.db" for d in LOCAL["all_decades"]]
DBS.append(LOCAL["extra_db"])
units = []
import time
def load(db):
    for i in range(4):
        try:
            return load_library(db)
        except OSError as e:
            print("retry", db, e); time.sleep(3)
    return {}
for db in DBS:
    for d, rec in load(db).items():
        units.append({"calibre": rec, "calibre_dir": True, "directory": d, "cover_path": None, "files": []})
ctx = build_context(units)
stats = collections.defaultdict(collections.Counter)
swaps, near = [], []
for u in units:
    r = resolve_unit(u, ctx)
    lib = u["calibre"]["library"]
    c = r["provenance"]["candidates"][0] if r["provenance"]["candidates"] else {}
    stats[lib]["entries"] += 1
    stats[lib]["isfdb"] += bool(u["calibre"].get("isfdb"))
    if r["swapped"]:
        stats[lib]["swapped"] += 1
        swaps.append((lib, u["calibre"]["title"], u["calibre"]["authors"], r["title"], r["authors"], c.get("margin")))
    elif c and not c.get("swapped") and c.get("margin", 9) < 0:
        stats[lib]["near"] += 1
        near.append((lib, u["calibre"]["title"], u["calibre"]["authors"], c.get("margin")))
for lib, s in stats.items():
    print(f"{lib:22} entries={s['entries']:6} isfdb={s['isfdb']:6} swapped={s['swapped']:5} near-miss={s['near']:5}")
random.seed(3)
print("\n== swaps (sample of 25)")
for x in random.sample(swaps, min(25, len(swaps))):
    print(f"  [{x[0]}] stored: {x[1]!r} by {x[2]}  ->  {x[3]!r} by {x[4]}  (margin {x[5]})")
print("\n== near-misses, kept as stored (sample of 20)")
for x in random.sample(near, min(20, len(near))):
    print(f"  [{x[0]}] {x[1]!r} by {x[2]}  (keep {x[3]})")
