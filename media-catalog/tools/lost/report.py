"""Merge all search results: which missing Calibre books have a copy somewhere, where, and which are gone."""
import json, os
from collections import Counter, defaultdict

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA = os.path.join(ROOT, "tools", "data", "lost")
os.makedirs(DATA, exist_ok=True)
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
STRENGTH = {"calibre-folder": 4, "calibre-record": 4, "calibre-filename": 3, "title+author": 2, "fuzzy-author-folder": 1.5, "title+folder": 1}
AMBIGUOUS = 3   # a name matching more records than this (magazine issues, omnibus titles) identifies none of them
missing = json.load(open(os.path.join(DATA, "missing.json")))


def place(path):
    if "/CloudStorage/OneDrive" in path: return "OneDrive"
    if "/CloudStorage/GoogleDrive" in path: return "Google Drive"
    if "/Mobile Documents" in path: return "iCloud Drive"
    for prefix, label in LOCAL["places"]:
        if path.startswith(prefix): return label
    return "this Mac"


copies = defaultdict(list)
for name in ("found.jsonl", "found_local.jsonl", "record_matches.jsonl", "fuzzy_matches.jsonl"):
    p = os.path.join(DATA, name)
    for line in open(p) if os.path.exists(p) else ():
        r = json.loads(line)
        if r["kind"] in STRENGTH and len(r["ids"]) <= AMBIGUOUS:
            for i in r["ids"]:
                copies[i].append({"path": r["path"], "kind": r["kind"], "where": place(r["path"]), "archive": None})
p = os.path.join(DATA, "archive_matches.jsonl")
for line in open(p) if os.path.exists(p) else ():
    r = json.loads(line)
    for i in r["ids"] if len(r["ids"]) <= AMBIGUOUS else ():
        copies[i].append({"path": r["member"], "kind": r["kind"], "where": place(r["archive"]), "archive": r["archive"]})

found = {i: c for i, c in copies.items()}
by_lib = Counter(r["library"] for r in missing)
found_lib = Counter(missing[i]["library"] for i in found)
only_archive = sum(1 for c in found.values() if all(x["archive"] for x in c))
only_cloud = sum(1 for c in found.values() if all(x["where"] in ("OneDrive", "Google Drive", "iCloud Drive") for x in c))
places = Counter(w for c in found.values() for w in {x["where"] for x in c})
folders = Counter()
for c in found.values():
    for x in c:
        folders["/".join((x["archive"] or x["path"]).split("/")[:9])] += 1

print(f"Missing books with a copy somewhere: {len(found):,} of {len(missing):,} ({len(found)/len(missing):.0%})")
for lib in sorted(by_lib):
    print(f"  {lib}: {found_lib[lib]:,} of {by_lib[lib]:,}")
print(f"  only inside archives: {only_archive:,}; only in cloud storage: {only_cloud:,}")
print("Books with a copy, by place:", dict(places.most_common()))
print("Folders holding the most:")
for k, v in folders.most_common(12):
    print(f"  {v:6,}  {k}")
gone = [r for i, r in enumerate(missing) if i not in found]
print(f"Not found anywhere: {len(gone):,}")
top_gone = Counter(a for r in gone for a in r["authors"][:1]).most_common(15)
print("  authors with the most unfound books:", top_gone)
jm = [(r["title"], sorted({x['where'] for x in found.get(i, [])}) or "not found") for i, r in enumerate(missing)
      if "Julian May" in r["authors"]]
print("Julian May:", jm)
best = {}
for i, c in found.items():
    c.sort(key=lambda x: (-STRENGTH[x["kind"]], x["archive"] is not None,
                          x["where"] in ("OneDrive", "Google Drive", "iCloud Drive")))
    best[i] = c
json.dump([{**missing[i], "copies": best[i]} for i in sorted(best)], open(os.path.join(DATA, "recovered.json"), "w"))
json.dump(gone, open(os.path.join(DATA, "gone.json"), "w"))
