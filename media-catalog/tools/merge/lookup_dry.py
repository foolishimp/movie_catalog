"""In-memory: which cached ISBN records the resolver would accept, for units with no author (read-only)."""
import os, sys
from collections import Counter
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import get_conn
from books import resolve as R

conn = get_conn()
with conn.cursor() as cur:
    units = R._load_units(cur)
    lookups = R._load_lookups(cur)
    cur.execute("SELECT id, authors, isbn, available, hidden, work_key FROM books")
    cur_rows = {r[0]: r for r in cur.fetchall()}
conn.close()
ctx = R.build_context(units)
ctx.lookups = lookups
stats, seen = Counter(), set()
out = {"accepted": [], "rejected": [], "no names": [], "with authors, changed": []}
for u in units:
    _, authors, isbn, avail, hidden, wk = cur_rows[u["id"]]
    if not avail or hidden:
        continue
    files = R._pick_files(u["files"])
    cands, clues = R._candidates(u, files)
    rec = lookups.get(isbn) if isbn else None
    if authors:
        if rec:
            r = R.resolve_unit(u, ctx)
            if r["authors"] != authors:
                out["with authors, changed"].append((u["id"], cands, authors, r["authors"], rec))
        continue
    if not isbn:
        continue
    stats["authorless with isbn"] += 1
    if not rec:
        stats["no record"] += 1
        continue
    lc = R._lookup_candidate(u, files, cands, clues, ctx)
    names = R._clean_authors(a.get("name") for a in rec.get("authors") or [])
    key = "accepted" if lc else ("no names" if not names else "rejected")
    stats[key] += 1
    if (key, wk) in seen:
        continue
    seen.add((key, wk))
    r = R.resolve_unit(u, ctx)
    out[key].append((u["id"], cands, authors, r["authors"], rec))
print(stats)
for k, rows in out.items():
    print(f"\n== {k}: {len(rows)} distinct works")
    for uid, cands, before, after, rec in rows[: int(sys.argv[1]) if len(sys.argv) > 1 else 400]:
        own = next((c.get("title") or c.get("first") for c in cands if c.get("title") or c.get("first")), None)
        found = " / ".join(t for t in (rec.get("title"), rec.get("subtitle")) if t)
        print(f"  {uid}: {own!r:.70} | OL {found!r:.70} | {[a.get('name') for a in rec.get('authors') or []]} -> {after}"
              + (f" (was {before})" if before else ""))
