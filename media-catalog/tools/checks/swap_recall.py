"""Plant swaps in non-ISFDB Calibre records and measure how many the resolver undoes (no file evidence)."""
import json, os, sys, random, collections, copy
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from books.calibre import load_library
from books.resolve import build_context, resolve_unit
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
DBS = [f"{LOCAL['decade_root']}{d}s/metadata.db" for d in LOCAL["all_decades"]]
random.seed(7)
units, planted = [], []
for db in DBS:
    lib = load_library(db)
    for d, rec in lib.items():
        u = {"calibre": rec, "calibre_dir": True, "directory": d, "cover_path": None, "files": []}
        units.append(u)
        if not rec.get("isfdb") and rec.get("title") and rec.get("authors") and random.random() < 0.25:
            orig = (rec["title"], list(rec["authors"]))
            rec = copy.deepcopy(rec)
            rec["title"], rec["authors"] = " & ".join(orig[1]), [orig[0]]
            u["calibre"] = rec
            planted.append((u, orig))
ctx = build_context(units)
hit, miss, wrong = 0, [], []
margins = collections.Counter()
for u, (t, a) in planted:
    r = resolve_unit(u, ctx)
    c = r["provenance"]["candidates"][0]
    margins[round(c["margin"])] += 1
    if r["swapped"] and r["title"] == t:
        hit += 1
    elif r["swapped"]:
        wrong.append((t, a, r["title"], r["authors"]))
    else:
        miss.append((t, a, c["margin"]))
print(f"planted={len(planted)} recovered={hit} ({hit/len(planted):.0%}) swapped-but-different={len(wrong)} missed={len(miss)}")
print("margin histogram (keep score of the planted, i.e. -margin means looked swapped):", sorted(margins.items()))
print("\nmissed sample:")
for x in random.sample(miss, min(20, len(miss))):
    print(f"  {x[0]!r} by {x[1]}  keep={x[2]}")
print("\nswapped-but-different sample:")
for x in wrong[:10]:
    print(f"  {x[0]!r} by {x[1]} -> {x[2]!r} by {x[3]}")
