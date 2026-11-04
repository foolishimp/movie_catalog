"""Resolve chosen units without writing anything: python probe.py ID [ID ...]"""
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import get_conn
from books import resolve as R

ids = {int(a) for a in sys.argv[1:]}
conn = get_conn()
with conn.cursor() as cur:
    units = R._load_units(cur)
conn.close()
ctx = R.build_context(units)
for u in units:
    if u["id"] in ids:
        files = R._pick_files(u["files"])
        cands, clues = R._candidates(u, files)
        print(f"== {u['id']}")
        for c in cands:
            o = R._orient(c, ctx, set())
            print("   cand", c["group"], {k: c.get(k) for k in ("title", "authors", "first", "second", "ambiguous")}, "->", o)
        print("   bylines", clues["bylines"], "holders", [c.get("name") for c in clues["copyright"]])
        r = R.resolve_unit(u, ctx)
        print("   RESULT", repr(r["title"]), r["authors"], r["series"], r["needs_review"], r["review_reason"])
