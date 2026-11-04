"""Print scan totals and random samples of swapped, needs-review and loose books (read-only)."""
import os, sys, json
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import query
print(query("""select root, count(*) units, count(*) filter (where available) avail, count(*) filter (where calibre is not null) cal,
    count(*) filter (where needs_review) review, count(*) filter (where swapped) swapped, count(*) filter (where hidden) hidden,
    count(distinct work_key) works, count(*) filter (where cover_path is not null or cover_member is not null) covers,
    count(*) filter (where pub_year is not null) years, count(*) filter (where isbn is not null) isbns
    from books group by root"""))
print(query("select count(*) files, count(*) filter (where error is not null) errs from book_files"))
print(query("select left(error, 60) e, count(*) from book_files where error is not null group by 1 order by 2 desc limit 8"))
print("\n== SWAPPED (sample)")
for r in query("""select id, calibre is not null as cal, title, authors, review_reason, provenance->'candidates' c
                  from books where swapped order by random() limit 25"""):
    cands = "; ".join(f"{c['source']}: {c['title']!r} / {c['authors']}{' SW' if c['swapped'] else ''}{' AMB' if c.get('ambiguous') else ''} m={c['margin']}" for c in r["c"] or [])
    print(f"#{r['id']} {'C' if r['cal'] else 'L'} -> {r['title']!r} by {r['authors']} | {cands[:260]}")
print("\n== NEEDS REVIEW (sample)")
for r in query("""select id, calibre is not null as cal, title, authors, review_reason, provenance->'candidates' c
                  from books where needs_review and not swapped order by random() limit 15"""):
    cands = "; ".join(f"{c['source']}: {c['title']!r} / {c['authors']}{' SW' if c['swapped'] else ''}{' AMB' if c.get('ambiguous') else ''} m={c['margin']}" for c in r["c"] or [])
    print(f"#{r['id']} {'C' if r['cal'] else 'L'} -> {r['title']!r} by {r['authors']} [{r['review_reason']}] | {cands[:260]}")
print("\n== review reasons")
print(query("select review_reason, count(*) from books where needs_review group by 1 order by 2 desc limit 12"))
print("\n== loose files sample")
for r in query("select id, title, authors, pub_year, formats from books where calibre is null and not hidden order by random() limit 15"):
    print(r)
