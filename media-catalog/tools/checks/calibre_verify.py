"""Sample how many files in copied Calibre databases are on disk: python calibre_verify.py DIR BASE"""
import sqlite3, sys, os, random

D, BASE = sys.argv[1], sys.argv[2]
idx = [l.rstrip("\n").split("\t") for l in open(os.path.join(D, "index.tsv"))]
random.seed(1)

for n, rel in idx:
    con = sqlite3.connect(f"file:{os.path.join(D, n + '.db')}?mode=ro", uri=True)
    if con.execute("select count(*) from books").fetchone()[0] == 0:
        continue
    cols = {r[1] for r in con.execute("pragma table_info(data)")}
    bcols = {r[1] for r in con.execute("pragma table_info(books)")}
    libroot = os.path.join(BASE, os.path.dirname(rel))
    if "name" in cols and "path" in bcols:
        rows = con.execute("select b.path, d.name, d.format from data d join books b on b.id=d.book").fetchall()
        paths = [os.path.join(libroot, p, f"{nm}.{fmt.lower()}") for p, nm, fmt in rows]
    else:
        print(f"[{n}] {rel}: old schema, data cols={sorted(cols)} books cols={sorted(bcols)}")
        continue
    sample = random.sample(paths, min(300, len(paths)))
    ok = sum(os.path.exists(p) for p in sample)
    missing = [p for p in sample if not os.path.exists(p)][:2]
    print(f"[{n}] {rel}: {ok}/{len(sample)} sampled files present ({100*ok/len(sample):.0f}%)", flush=True)
    for m in missing:
        print(f"     missing e.g. {os.path.relpath(m, BASE)}")
