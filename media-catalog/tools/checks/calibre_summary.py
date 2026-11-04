"""Summarise copied Calibre metadata.db files listed in DIR/index.tsv: python calibre_summary.py DIR"""
import sqlite3, sys, os, collections, re

D = sys.argv[1]
idx = [l.rstrip("\n").split("\t") for l in open(os.path.join(D, "index.tsv"))]

def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

all_keys = collections.Counter()
lib_keys = {}
grand_fmt = collections.Counter()
grand_size = 0
grand_books = 0
years_all = collections.Counter()
authors_all = collections.Counter()
tags_all = collections.Counter()

for n, path in idx:
    con = sqlite3.connect(f"file:{os.path.join(D, n + '.db')}?mode=ro", uri=True)
    q = lambda s: con.execute(s).fetchall()
    books = q("select count(*) from books")[0][0]
    if books == 0:
        print(f"[{n}] {path}: EMPTY\n")
        continue
    nauth = q("select count(*) from authors")[0][0]
    size = q("select coalesce(sum(uncompressed_size),0) from data")[0][0]
    fmts = q("select format, count(*) from data group by format order by 2 desc")
    added = q("select min(timestamp), max(timestamp) from books")[0]
    try:
        pub = q("select min(substr(pubdate,1,4)), max(substr(pubdate,1,4)) from books where substr(pubdate,1,4) > '0101'")[0]
    except sqlite3.OperationalError:
        pub = ("n/a", "n/a")
    nofile = q("select count(*) from books b where not exists (select 1 from data d where d.book=b.id)")[0][0]
    top_auth = q("select a.name, count(*) from books_authors_link l join authors a on a.id=l.author group by a.id order by 2 desc limit 8")
    top_tags = q("select t.name, count(*) from books_tags_link l join tags t on t.id=l.tag group by t.id order by 2 desc limit 8")
    keys = set()
    for t, a in q("select b.title, b.author_sort from books b"):
        keys.add(norm(t) + "|" + norm((a or "").split("&")[0]))
    lib_keys[n] = keys
    all_keys.update(keys)
    grand_books += books
    grand_size += size
    for f, c in fmts:
        grand_fmt[f] += c
    for (y,) in q("select substr(timestamp,1,4) from books"):
        years_all[y] += 1
    for a, c in q("select a.name, count(*) from books_authors_link l join authors a on a.id=l.author group by a.id"):
        authors_all[a] += c
    for t, c in q("select t.name, count(*) from books_tags_link l join tags t on t.id=l.tag group by t.id"):
        tags_all[t] += c
    print(f"[{n}] {path}")
    print(f"   books={books:,} authors={nauth:,} files={sum(c for _, c in fmts):,} size={size/1e9:.1f} GB  no-file={nofile:,}")
    print(f"   added {added[0][:10]} .. {added[1][:10]}   pubyears {pub[0]}..{pub[1]}")
    print(f"   formats: " + ", ".join(f"{f}:{c}" for f, c in fmts[:8]))
    print(f"   top authors: " + "; ".join(f"{a} ({c})" for a, c in top_auth))
    print(f"   top tags: " + "; ".join(f"{t} ({c})" for t, c in top_tags))
    print()

print("==== TOTALS ====")
print(f"book records across libraries: {grand_books:,}")
print(f"distinct title+first-author: {len(all_keys):,}  (dupe records across libs: {grand_books - len(all_keys):,})")
print(f"indexed file size: {grand_size/1e9:.1f} GB")
print("formats: " + ", ".join(f"{f}:{c:,}" for f, c in grand_fmt.most_common(12)))
print("added-to-calibre by year: " + ", ".join(f"{y}:{c:,}" for y, c in sorted(years_all.items())))
print("top authors overall: " + "; ".join(f"{a} ({c})" for a, c in authors_all.most_common(20)))
print("top tags overall: " + "; ".join(f"{t} ({c})" for t, c in tags_all.most_common(20)))
print()
print("==== OVERLAP (distinct keys shared between libraries) ====")
ns = list(lib_keys)
for i, a in enumerate(ns):
    row = []
    for b in ns[i + 1:]:
        o = len(lib_keys[a] & lib_keys[b])
        if o >= 50:
            row.append(f"{b}:{o:,}")
    if row:
        print(f"{a} ({len(lib_keys[a]):,}) shares with " + ", ".join(row))
