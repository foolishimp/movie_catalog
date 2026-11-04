"""Count ebook files by format under each top-level folder: python lib_walk.py ROOT [ROOT ...]"""
import os, sys, collections, json
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

EBOOK = {"epub", "mobi", "azw", "azw3", "azw4", "kfx", "pdf", "prc", "pdb", "lit", "lrf", "fb2", "djvu",
         "cbr", "cbz", "cb7", "rtf", "txt", "doc", "docx", "htmlz", "mbp", "chm", "odt"}

out = {}
for root in sys.argv[1:]:
    for top in sorted(os.listdir(root)):
        p = os.path.join(root, top)
        if top.startswith(("@", ".")):
            continue
        files = 0
        size = 0
        ext = collections.Counter()
        ext_size = collections.Counter()
        dbs = []
        if os.path.isfile(p):
            walk = [(root, [], [top])]
        else:
            walk = os.walk(p)
        for d, dirs, fs in walk:
            dirs[:] = [x for x in dirs if not x.startswith("@")]
            for f in fs:
                if f.startswith("._") or f in (".DS_Store", "Thumbs.db"):
                    continue
                fp = os.path.join(d, f)
                try:
                    sz = os.lstat(fp).st_size
                except OSError:
                    continue
                files += 1
                size += sz
                e = f.rsplit(".", 1)[-1].lower() if "." in f else ""
                ext[e] += 1
                ext_size[e] += sz
                if f == "metadata.db":
                    dbs.append(fp)
        ebooks = sum(c for e, c in ext.items() if e in EBOOK)
        out[p] = {"files": files, "bytes": size, "ebook_files": ebooks,
                  "top_ext": ext.most_common(8), "metadata_dbs": dbs}
        print(f"{size/1e9:8.1f} GB {files:8d} files {ebooks:8d} ebook  {p}", flush=True)

DATA = os.path.join(ROOT, "tools", "data", "checks")
os.makedirs(DATA, exist_ok=True)
json.dump(out, open(os.path.join(DATA, "lib_walk.json"), "w"), indent=1)
