"""Walk every mounted NAS share in parallel and match files/folders against the missing Calibre records."""
import json, os, sys, threading, time
from collections import Counter, deque
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
from books.naming import EBOOK_EXTENSIONS, file_stem, norm_name, norm_title, parse_filename, split_authors

DATA = os.path.join(ROOT, "tools", "data", "lost")
os.makedirs(DATA, exist_ok=True)
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
ROOTS = sys.argv[1:] or LOCAL["search_roots"]
DECADE_ROOT, LABEL = LOCAL["decade_root"], LOCAL["decade_label"]
TAG = os.environ.get("SEARCH_TAG", "")
SKIP_NAMES = {"@eaDir", ".@__thumb", "@Recently-Snapshot", ".snapshot", "#snapshot", ".AppleDouble", ".Trashes",
              ".Spotlight-V100", ".fseventsd", "node_modules", ".git", "System Volume Information", "CloudStorage", ".Encrypted",
              "Caches", "Containers", "Group Containers"} | set(LOCAL["search_skip_names"])
SKIP_NAMES |= set(filter(None, os.environ.get("EXTRA_SKIP", "").split(":")))
SKIP_SUFFIXES = (".app", ".sparsebundle", ".photoslibrary", ".bundle", ".framework", ".xcarchive", ".musiclibrary",
                 ".tvlibrary", ".imovielibrary", ".fcpbundle", ".backupbundle", ".lrdata")
EXTS = set(EBOOK_EXTENSIONS) | {".zip", ".rar", ".7z", ".cbr", ".cbz", ".pdf", ".txt", ".doc", ".rtf"}
ARCHIVES = {".zip", ".rar", ".7z"}
ARCHIVE_MIN_BYTES = 200_000
WORKERS = 32

missing = json.load(open(os.path.join(DATA, "missing.json")))
by_name, by_key, by_title, by_book_dir = {}, {}, {}, {}
author_dirs = set()
for i, r in enumerate(missing):
    for f in r["files"]:
        if f["name"]:
            by_name.setdefault(f["name"].lower(), set()).add(i)
    by_key.setdefault(r["key"], set()).add(i)
    by_title.setdefault(norm_title(r["title"]), set()).add(i)
    a_dir, b_dir = r["path"].split("/", 1)
    by_book_dir.setdefault(b_dir, set()).add(i)
    author_dirs.add(a_dir)
original = {f"{DECADE_ROOT}{r['library'][len(LABEL) + 1:]}/{r['path']}" for r in missing}

lock = threading.Lock()
stats = Counter()
per_root = Counter()
out = open(os.path.join(DATA, f"found{TAG}.jsonl"), "w")
archives_out = open(os.path.join(DATA, f"archives{TAG}.jsonl"), "w")
todo = deque((r, r) for r in ROOTS if os.path.isdir(r))
active = 0
done = threading.Event()


def emit(kind, path, ids, root):
    with lock:
        stats["match:" + kind] += 1
        out.write(json.dumps({"kind": kind, "path": path, "ids": sorted(ids), "root": root}) + "\n")


def match_file(path, name, root, entry=None):
    stem, ext = file_stem(name)
    if ext.lower() not in EXTS:
        return
    if ext.lower() in ARCHIVES and entry is not None:
        try:
            size = entry.stat(follow_symlinks=False).st_size
        except OSError:
            size = 0
        if size >= ARCHIVE_MIN_BYTES:
            with lock:
                stats["archives"] += 1
                archives_out.write(json.dumps({"path": path, "size": size, "root": root}) + "\n")
    with lock:
        stats["candidate files"] += 1
    ids = by_name.get(stem.lower())
    if ids:
        return emit("calibre-filename", path, ids, root)
    fn = parse_filename(name)
    first, second = fn.get("first"), fn.get("second")
    if first and second:
        for t, a in ((first, second), (second, first)):
            names = split_authors(a)
            ids = by_key.get(f"{norm_title(t)}|{norm_name(names[0]) if names else ''}")
            if ids:
                return emit("title+author", path, ids, root)
    if first:
        ids = by_title.get(norm_title(first))
        if ids:
            folders = {norm_name(p) for p in path.split("/")[-4:-1]}
            hit = {i for i in ids if norm_name((missing[i]["authors"] or [""])[0]) in folders}
            if hit:
                emit("title+folder", path, hit, root)


def worker():
    global active
    while True:
        with lock:
            if todo:
                d, root = todo.popleft()
                active += 1
            elif active == 0:
                done.set()
                return
            else:
                d = None
        if d is None:
            time.sleep(0.05)
            continue
        try:
            with os.scandir(d) as it:
                entries = list(it)
        except OSError:
            with lock:
                stats["unreadable dirs"] += 1
                active -= 1
            continue
        subdirs = []
        for e in entries:
            try:
                is_dir = e.is_dir(follow_symlinks=False)
            except OSError:
                continue
            if is_dir:
                if e.name in SKIP_NAMES or e.name.endswith(SKIP_SUFFIXES):
                    continue
                subdirs.append(e.path)
                if e.name in by_book_dir and e.path not in original:
                    emit("calibre-folder", e.path, by_book_dir[e.name], root)
                elif e.name in author_dirs and not d.startswith(DECADE_ROOT):
                    with lock:
                        stats["author-folder"] += 1
                        out.write(json.dumps({"kind": "author-folder", "path": e.path, "root": root}) + "\n")
            elif e.name == "metadata.db":
                with lock:
                    out.write(json.dumps({"kind": "metadata.db", "path": e.path, "root": root}) + "\n")
                    stats["metadata.db"] += 1
            else:
                match_file(e.path, e.name, root, e)
        with lock:
            todo.extend((s, root) for s in subdirs)
            stats["dirs"] += 1
            per_root[root] += 1
            active -= 1


def progress():
    start = time.time()
    while not done.wait(15):
        with lock:
            line = {"elapsed_s": round(time.time() - start), "queued": len(todo), **stats, "per_root": dict(per_root)}
            out.flush()
            archives_out.flush()
        with open(os.path.join(DATA, f"progress{TAG}.json"), "w") as f:
            json.dump(line, f)


threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
for t in threads:
    t.start()
threading.Thread(target=progress, daemon=True).start()
for t in threads:
    t.join()
out.close()
archives_out.close()
print(json.dumps({**stats, "per_root": dict(per_root)}, indent=1))
