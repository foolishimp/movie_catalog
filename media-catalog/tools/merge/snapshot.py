"""Snapshot every on-disk unit's title/authors/series (read-only): python snapshot.py OUT.json"""
import json, os, sys
OUT = os.path.abspath(sys.argv[1])
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import query

rows = query("""SELECT id, title, authors, series, series_index, needs_review, library, directory
                FROM books WHERE available AND NOT hidden""")
json.dump({str(r["id"]): r for r in rows}, open(OUT, "w"), default=str)
print(len(rows), "units ->", OUT)
