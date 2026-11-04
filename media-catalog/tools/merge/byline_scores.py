"""How do stored bylines score as names? (read-only)"""
import os, sys, random
from collections import Counter
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import query
from books.naming import name_score
rows = query("""SELECT DISTINCT jsonb_array_elements_text(evidence->'text'->'bylines') b FROM book_files
                WHERE present AND evidence->'text'->'bylines' IS NOT NULL""")
buckets = Counter()
low = []
for r in rows:
    s = round(name_score(r["b"]), 1)
    buckets[s] += 1
    if s < 1.0:
        low.append((s, r["b"]))
print(sorted(buckets.items()))
random.seed(3)
for s, b in random.sample(low, min(60, len(low))): print(f"  {s:5} {b}")
