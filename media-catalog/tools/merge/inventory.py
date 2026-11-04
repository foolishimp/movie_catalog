"""Read-only inventory of book-like files on the main NAS outside the folders Media Catalog scans (BOOK_DIRS).

Writes outside_files.jsonl (one line per file) and inventory.json (counts by folder and kind).
"""
import json, os, threading, time
from collections import Counter, deque

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA = os.path.join(ROOT, "tools", "data", "merge")
os.makedirs(DATA, exist_ok=True)
ENV = os.path.join(ROOT, ".env")
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
ROOTS = LOCAL["inventory_roots"]
SKIP_NAMES = {"@eaDir", ".@__thumb", "@Recently-Snapshot", ".snapshot", "#snapshot", ".AppleDouble", ".Trashes",
              ".Spotlight-V100", ".fseventsd", "node_modules", ".git", "System Volume Information", "@Recycle",
              "#recycle", "$RECYCLE.BIN", ".TemporaryItems"}
SKIP_SUFFIXES = (".app", ".sparsebundle", ".photoslibrary", ".bundle", ".framework", ".xcarchive", ".musiclibrary",
                 ".tvlibrary", ".imovielibrary", ".fcpbundle", ".backupbundle", ".lrdata")
EBOOK = {".epub", ".kepub", ".mobi", ".azw", ".azw3", ".azw4", ".prc", ".fb2", ".lit", ".lrf", ".djvu", ".djv",
         ".pdb", ".cbr", ".cbz", ".cb7", ".chm", ".htmlz", ".snb", ".tcr", ".rb"}
MAYBE = {".pdf", ".zip", ".rar", ".7z", ".txt", ".rtf", ".doc", ".docx", ".odt"}
WORKERS = 24

book_dirs = []
for line in open(ENV):
    if line.startswith("BOOK_DIRS="):
        book_dirs = [d.rstrip("/") for d in line.split("=", 1)[1].strip().strip("\"'").split(":") if d.strip()]
skip_dirs = set(book_dirs)

lock = threading.Lock()
todo = deque(ROOTS)
active = 0
done = threading.Event()
stats = Counter()
by_folder = Counter()          # (top folder, kind) -> files
bytes_by_folder = Counter()
out = open(os.path.join(DATA, "outside_files.jsonl"), "w")


def top_folder(path):
    parts = path.split("/")
    return "/".join(parts[2:5])          # volume/top/sub


def worker():
    global active
    while True:
        with lock:
            if todo:
                d = todo.popleft()
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
        subdirs, rows = [], []
        for e in entries:
            try:
                if e.is_dir(follow_symlinks=False):
                    if e.name in SKIP_NAMES or e.name.endswith(SKIP_SUFFIXES) or e.path in skip_dirs:
                        continue
                    subdirs.append(e.path)
                    continue
                ext = os.path.splitext(e.name)[1].lower()
                if ext not in EBOOK and ext not in MAYBE:
                    continue
                size = e.stat(follow_symlinks=False).st_size
            except OSError:
                continue
            kind = "ebook" if ext in EBOOK else ext.lstrip(".")
            rows.append({"path": e.path, "ext": ext, "size": size, "kind": kind})
        with lock:
            for r in rows:
                out.write(json.dumps(r) + "\n")
                key = top_folder(r["path"])
                by_folder[(key, r["kind"])] += 1
                bytes_by_folder[key] += r["size"]
                stats["files:" + r["kind"]] += 1
            todo.extend(subdirs)
            stats["dirs"] += 1
            active -= 1


def dump(final=False):
    with lock:
        folders = {}
        for (key, kind), n in by_folder.items():
            folders.setdefault(key, {"bytes": bytes_by_folder[key]})[kind] = n
        data = {"final": final, "elapsed_s": round(time.time() - started), "queued": len(todo), **stats,
                "folders": dict(sorted(folders.items(), key=lambda kv: -kv[1].get("ebook", 0)))}
        out.flush()
    with open(os.path.join(DATA, "inventory.json"), "w") as f:
        json.dump(data, f, indent=1)


def progress():
    while not done.wait(20):
        dump()


started = time.time()
threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
for t in threads:
    t.start()
threading.Thread(target=progress, daemon=True).start()
for t in threads:
    t.join()
dump(final=True)
out.close()
print("done", dict(stats), f"{time.time() - started:.0f}s")
