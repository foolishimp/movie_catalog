"""Load every known Calibre metadata.db on the NAS and report what each holds (read-only)."""
import json, os, sys, time, collections
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from books.calibre import load_library
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
DBS = LOCAL["calibre_dbs"]
for db in DBS:
    if not os.path.exists(db):
        print("MISSING", db); continue
    t = time.time()
    try:
        lib = load_library(db)
    except Exception as e:
        print("ERR", db, e); continue
    tops = collections.Counter(os.path.relpath(d, os.path.dirname(db)).split("/")[0] for d in lib)
    rec = next(iter(lib.values()), {})
    print(f"{len(lib):6} entries {time.time()-t:4.1f}s {db}  label={rec.get('library')!r}  top-level dirs={len(tops)} e.g. {tops.most_common(3)}")
