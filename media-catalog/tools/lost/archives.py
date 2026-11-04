"""List members of the archives the walkers logged and match them against the missing Calibre records.

Only the archive index is read. Cloud placeholders (dataless files) are counted, not downloaded.
"""
import json, os, stat, subprocess, sys, threading, zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
from books.naming import file_stem, norm_name, norm_title, parse_filename, split_authors

DATA = os.path.join(ROOT, "tools", "data", "lost")
os.makedirs(DATA, exist_ok=True)
SF_DATALESS = 0x40000000
BOOK_EXTS = {".epub", ".mobi", ".azw", ".azw3", ".pdf", ".lit", ".prc", ".pdb", ".rtf", ".txt", ".doc", ".docx",
             ".fb2", ".djvu", ".htm", ".html", ".lrf", ".rb", ".chm", ".odt", ".htmlz", ".cbr", ".cbz"}

missing = json.load(open(os.path.join(DATA, "missing.json")))
by_name, by_key, by_title = {}, {}, {}
for i, r in enumerate(missing):
    for f in r["files"]:
        if f["name"]:
            by_name.setdefault(f["name"].lower(), set()).add(i)
    by_key.setdefault(r["key"], set()).add(i)
    by_title.setdefault(norm_title(r["title"]), set()).add(i)


def match_member(member):
    name = member.rsplit("/", 1)[-1]
    stem, ext = file_stem(name)
    if ext.lower() not in BOOK_EXTS:
        return None, None
    ids = by_name.get(stem.lower())
    if ids:
        return "calibre-filename", ids
    fn = parse_filename(name)
    first, second = fn.get("first"), fn.get("second")
    if first and second:
        for t, a in ((first, second), (second, first)):
            names = split_authors(a)
            ids = by_key.get(f"{norm_title(t)}|{norm_name(names[0]) if names else ''}")
            if ids:
                return "title+author", ids
    if first:
        ids = by_title.get(norm_title(first))
        folders = {norm_name(p) for p in member.split("/")[:-1][-3:]}
        hit = {i for i in ids or () if norm_name((missing[i]["authors"] or [""])[0]) in folders}
        if hit:
            return "title+folder", hit
    return None, None


def members(path):
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path) as z:
            return z.namelist()
    res = subprocess.run(["/usr/bin/bsdtar", "-tf", path], capture_output=True, text=True, timeout=120)
    if res.returncode and not res.stdout:
        raise OSError(res.stderr.strip()[:200])
    return res.stdout.splitlines()


lock = threading.Lock()
stats = Counter()
out = open(os.path.join(DATA, "archive_matches.jsonl"), "w")


def work(a):
    path = a["path"]
    try:
        st = os.stat(path)
    except OSError:
        with lock: stats["gone"] += 1
        return
    if st.st_flags & SF_DATALESS:
        with lock:
            stats["cloud-only (skipped)"] += 1
            stats["cloud-only bytes"] += a["size"]
        return
    try:
        names = members(path)
    except Exception as e:
        with lock: stats["unreadable"] += 1
        return
    hits = 0
    for m in names:
        kind, ids = match_member(m)
        if kind:
            hits += 1
            with lock:
                out.write(json.dumps({"kind": kind, "archive": path, "member": m, "ids": sorted(ids), "root": a["root"]}) + "\n")
    with lock:
        stats["archives listed"] += 1
        stats["members"] += len(names)
        stats["member matches"] += hits


archives = []
for tag in sys.argv[1:] or ["", "_local"]:
    p = os.path.join(DATA, f"archives{tag}.jsonl")
    if os.path.exists(p):
        archives += [json.loads(l) for l in open(p) if l.strip()]
import re
CONTINUATION = re.compile(r"\.part0*(?:[2-9]|\d{2,})\.rar$", re.I)
archives = [a for a in archives if not CONTINUATION.search(a["path"])]
seen = set()
archives = [a for a in archives if not (a["path"] in seen or seen.add(a["path"]))]
print(f"{len(archives)} archives, {sum(a['size'] for a in archives) / 1e9:.1f} GB", flush=True)
with ThreadPoolExecutor(8) as pool:
    list(pool.map(work, archives))
out.close()
print(json.dumps(stats, indent=1))
