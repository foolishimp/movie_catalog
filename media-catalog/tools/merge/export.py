"""Export what the merge plan needs from Media Catalog's database to plan_data.json (read-only)."""
import json, os, re, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
from db import query
from books.naming import parse_filename

DATA = os.path.join(ROOT, "tools", "data", "merge")
os.makedirs(DATA, exist_ok=True)
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
# The main library folder and its mirror share one layout.
LIBS = (LOCAL["library_main"], LOCAL["library_mirror"])
# Game books live under <game folder>/<game>; the game is the shelf, whatever the file name says.
GAME_FOLDER, GAMES = LOCAL["game_folder"], LOCAL["games"]
TECHNICAL = tuple(LOCAL["technical_folders"])
# Personal documents filed among the books.
PERSONAL_DIRS = tuple(LOCAL["personal_dirs"])
PERSONAL_NAME = re.compile(LOCAL["personal_name_pattern"], re.I)

units = query("""
    SELECT b.id, COALESCE(b.work_key, 'id' || b.id) AS work, b.title, b.authors, b.series, b.series_index,
           b.pub_year, b.library, b.calibre_dir, b.needs_review, b.review_reason, b.directory
    FROM books b WHERE b.available AND NOT b.hidden""")
files = query("""
    SELECT f.book_id, f.file_path, upper(f.format) AS format, f.size_bytes
    FROM book_files f JOIN books b ON b.id = f.book_id
    WHERE f.present AND b.available AND NOT b.hidden""")
names = {}
for f in files:
    names.setdefault(f["book_id"], []).append(f["file_path"].split("::")[-1].rsplit("/", 1)[-1])

for u in units:
    rel = next((u["directory"][len(l):] for l in LIBS if u["directory"].startswith(l)), "")
    parts = rel.split("/")
    base = names.get(u["id"], [])
    u["shelf"], u["personal"] = None, False
    if parts[0] == GAME_FOLDER and len(parts) > 1:
        u["shelf"] = "RPG: " + GAMES.get(parts[1].lower(), parts[1])
    elif u["library"] is None and (any(rel.startswith(t) for t in TECHNICAL)
                                   or any(parse_filename(n)["pattern"] in ("slug_isbn", "slug") for n in base)):
        u["shelf"] = "Technical"
    if u["library"] is None and (any(d in u["directory"] for d in PERSONAL_DIRS)
                                 or any(PERSONAL_NAME.search(n) for n in base)):
        u["personal"] = True

refs = query("SELECT count(*) AS n, count(DISTINCT COALESCE(work_key, 'id' || id)) AS works FROM books "
             "WHERE NOT available AND NOT hidden")[0]
hidden = query("SELECT count(*) AS n FROM books WHERE hidden")[0]["n"]
json.dump({"units": units, "files": files, "references": refs, "hidden": hidden},
          open(os.path.join(DATA, "plan_data.json"), "w"), default=str)
shelves = {}
for u in units:
    shelves[u["shelf"]] = shelves.get(u["shelf"], 0) + 1
print(len(units), "units", len(files), "files", refs, "hidden", hidden)
print("shelves", shelves, "personal", sum(u["personal"] for u in units))
