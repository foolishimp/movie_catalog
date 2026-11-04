"""Write the book-merge dry run (nothing is copied) from plan_data.json, inventory.json and the audit."""
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA = os.path.join(ROOT, "tools", "data", "merge")
os.makedirs(DATA, exist_ok=True)
OUT = sys.argv[1]
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
NAS, BACKUP_NAS, DISK = LOCAL["main_nas"], LOCAL["backup_nas"], LOCAL["backup_disk"]
TARGET = LOCAL["target"]
LIBRARIES = {"Ebook": "Calibre Library", "Comic or magazine": "Comics"}
COMIC = {"CBR", "CBZ", "CB7"}
ARCHIVE = {"ZIP", "RAR", "7Z"}

data = json.load(open(os.path.join(DATA, "plan_data.json")))
ENV = os.path.join(ROOT, ".env")
book_dirs = []
for env_line in open(ENV):                        # read, never printed
    if env_line.startswith("BOOK_DIRS="):
        book_dirs = [d.rstrip("/") + "/" for d in env_line.split("=", 1)[1].strip().strip("\"'").split(":") if d.strip()]
recovered = json.load(open(os.path.join(DATA, "..", "lost", "recovered.json")))

FONT = "Arial"
HEAD = Font(name=FONT, bold=True, color="FFFFFF")
HEAD_FILL = PatternFill("solid", fgColor="5B4636")
BODY = Font(name=FONT, size=10)
BOLD = Font(name=FONT, size=10, bold=True)
TITLE = Font(name=FONT, size=14, bold=True)
NOTE = Font(name=FONT, size=9, italic=True, color="555555")
WRAP = Alignment(wrap_text=True, vertical="top")


def safe(name, limit=80):
    name = re.sub(r'[\\/:*?"<>|]+', "_", (name or "").strip()).strip(". ") or "Unknown"
    return name[:limit].rstrip(". ")


def shown(path):
    """A file, or 'zip  ⟶  member' for a book inside a zip."""
    archive, sep, member = path.partition("::")
    return f"{archive}  ⟶  {member}" if sep else path


def issue_numbers(path):
    name = path.split("::")[-1].rsplit("/", 1)[-1]
    return tuple(re.findall(r"#\s*(\d+)", name))


units = {u["id"]: u for u in data["units"]}
# The mirror is a copy of the main library: count the files it shares with it, and the ones only it has.
MAIN, MIRROR = LOCAL["library_main"], LOCAL["library_mirror"]
main_rel = {f["file_path"][len(MAIN):] for f in data["files"] if f["file_path"].startswith(MAIN)}
mirror_rel = [f["file_path"][len(MIRROR):] for f in data["files"] if f["file_path"].startswith(MIRROR)]
mirror_shared = sum(r in main_rel for r in mirror_rel)
files_by_unit = defaultdict(list)
for f in data["files"]:
    files_by_unit[f["book_id"]].append(f)
works = defaultdict(list)
for u in units.values():
    works[u["work"]].append(u)

work_rows, file_rows, call_rows, personal_rows = [], [], [], []
flag_counts = Counter()
for us in works.values():
    files = [f for u in us for f in files_by_unit[u["id"]]]
    if not files:
        continue
    if all(u["personal"] for u in us):          # personal documents, not books
        for f in files:
            personal_rows.append([units[f["book_id"]]["title"] or "", f["format"],
                                  round((f["size_bytes"] or 0) / 1024, 1), shown(f["file_path"])])
        continue
    us.sort(key=lambda u: (u["library"] is None, u["needs_review"], -len(files_by_unit[u["id"]]), u["id"]))
    lead = us[0]
    title = lead["title"] or os.path.basename(lead["directory"])
    authors = lead["authors"] or []
    fmts = {f["format"] for f in files}
    kind = "Comic or magazine" if fmts <= COMIC else "Ebook"
    shelf = next((u["shelf"] for u in us if u["shelf"]), "") if kind == "Ebook" else ""

    by_fmt = defaultdict(list)
    for f in files:
        by_fmt[f["format"]].append(f)
    kept, left_out = [], []
    versions_differ = False
    for fmt, fs in by_fmt.items():
        # Calibre keeps one file per format: prefer a copy from a Calibre library, then a loose file
        # over one inside a zip, then the largest.
        fs.sort(key=lambda f: (units[f["book_id"]]["library"] is None, "::" in f["file_path"],
                               -(f["size_bytes"] or 0), f["file_path"]))
        keep = fs[0]
        kept.append(keep)
        for f in fs[1:]:
            same = f["size_bytes"] == keep["size_bytes"]
            left_out.append((f, "duplicate" if same else "other version"))
            if not same and keep["size_bytes"] and abs((f["size_bytes"] or 0) - keep["size_bytes"]) > 0.1 * keep["size_bytes"]:
                versions_differ = True

    flags = []
    if all(u["needs_review"] for u in us):
        flags.append("title/author uncertain")
    # Comics and magazines are filed by title and issue; game books by their game.
    if not authors and kind == "Ebook" and not shelf.startswith("RPG"):
        flags.append("no author")
    if fmts <= ARCHIVE:
        flags.append("only a ZIP/RAR: unpack first")
    if kind == "Comic or magazine" and len({issue_numbers(f["file_path"]) for f in files}) > 1:
        flags.append("issue numbers differ: may be several issues")
    if versions_differ:
        flags.append("same format in different sizes")
    for fl in flags:
        flag_counts[fl] += 1

    wid = len(work_rows) + 1
    # A game book with no credited writer is filed under its game rather than "Unknown".
    filed_under = authors[0] if authors else shelf[len("RPG: "):] if shelf.startswith("RPG") else "Unknown"
    folder = f"{TARGET}/{LIBRARIES[kind]}/{safe(filed_under)}/{safe(title)}"
    kept.sort(key=lambda f: f["format"])
    dups = sum(1 for _, a in left_out if a == "duplicate")
    others = len(left_out) - dups
    row = [wid, kind, shelf, title, "; ".join(authors), lead["series"] or "", lead["series_index"] or "", lead["pub_year"] or "",
           " ".join(f["format"] for f in kept), len(kept), len(us), dups, others,
           round(sum(f["size_bytes"] or 0 for f in kept) / 1048576, 2), folder, "; ".join(flags),
           shown(kept[0]["file_path"])]
    work_rows.append(row)
    if flags:
        call_rows.append(row)
    stem = safe(f"{title} - {authors[0]}" if authors else title, 120)
    for f in kept:
        file_rows.append([wid, "keep", f["format"], round((f["size_bytes"] or 0) / 1024, 1), shown(f["file_path"]),
                          "yes" if "::" in f["file_path"] else "", f"{folder}/{stem}.{f['format'].lower()}"])
    for f, action in left_out:
        file_rows.append([wid, action, f["format"], round((f["size_bytes"] or 0) / 1024, 1), shown(f["file_path"]),
                          "yes" if "::" in f["file_path"] else "", ""])

# Books the audit found only away from the main NAS: a later pass.
elsewhere = []
for r in recovered:
    if not any(c["where"] == NAS for c in r["copies"]):
        best = r["copies"][0]
        elsewhere.append([r["library"], r["title"], "; ".join(a.replace("|", ",") for a in r["authors"]),
                          ", ".join(sorted({c["where"] for c in r["copies"]})), shown(best["archive"] + "::" + best["path"])
                          if best["archive"] else best["path"]])
elsewhere.sort(key=lambda x: (x[2].lower(), x[1].lower()))

# Book-like files on the main NAS outside the folders Media Catalog scans (inventory.py's walk).
LOOKS_LIKE = LOCAL["looks_like"]
counts = defaultdict(Counter)
for inv_line in open(os.path.join(DATA, "outside_files.jsonl")):
    r = json.loads(inv_line)
    if any(r["path"].startswith(d) for d in book_dirs):
        continue
    parts = r["path"].split("/")
    folder = "/".join(parts[2:6]) if len(parts) >= 7 else "/".join(parts[2:-1])
    kind = r["kind"]
    group = "ebook" if kind == "ebook" else "pdf" if kind == "pdf" else \
        "archive" if kind in ("zip", "rar", "7z") else "text"
    counts[folder][group] += 1
    counts[folder]["bytes"] += r["size"]
outside = []
for folder, c in counts.items():
    if c["ebook"] or c["pdf"] >= 20:
        looks = next((text for key, text in LOOKS_LIKE if key in folder), "")
        outside.append([folder, c["ebook"], c["pdf"], c["archive"], c["text"], round(c["bytes"] / 1024 ** 3, 2), looks])
outside.sort(key=lambda x: (-x[1], -x[2]))


def table(ws, headers, rows, widths):
    ws.append(headers)
    for c in ws[1]:
        c.font, c.fill, c.alignment = HEAD, HEAD_FILL, Alignment(vertical="center", wrap_text=True)
    for r in rows:
        ws.append([ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v for v in r])
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.font = BODY
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions


wb = Workbook()
s = wb.active
s.title = "Summary"
WORK_HEAD = ["Work #", "Kind", "Shelf", "Title", "Authors", "Series", "Series #", "Year", "Formats kept", "Files kept",
             "Copies on disk", "Exact duplicates left out", "Other versions left out", "Kept size (MB)",
             "Proposed folder", "Needs your call", "Best copy (source)"]
WORK_W = [8, 14, 16, 40, 28, 22, 7, 7, 14, 7, 8, 10, 10, 9, 70, 34, 90]
table(wb.create_sheet("Works"), WORK_HEAD, work_rows, WORK_W)
table(wb.create_sheet("Needs your call"), WORK_HEAD, call_rows, WORK_W)
table(wb.create_sheet("Files"), ["Work #", "Action", "Format", "Size (KB)", "Source", "Inside a zip",
                                 "Proposed file"], file_rows, [8, 13, 8, 10, 110, 8, 110])
table(wb.create_sheet("Not yet scanned"), [f"Folder on {NAS} (outside the scanned folders)", "Ebook files",
                                           "PDFs", "ZIP/RAR/7z", "TXT/RTF/DOC", "Size of these (GB)", "Looks like"],
      outside, [80, 10, 8, 10, 11, 12, 60])
table(wb.create_sheet("Only elsewhere"), ["Calibre library", "Title", "Authors", "Places with a copy", "Best copy"],
      elsewhere, [16, 40, 28, 22, 100])
personal_rows.sort(key=lambda x: x[3])
table(wb.create_sheet("Personal documents"), ["Name", "Format", "Size (KB)", "Where it is (left there)"],
      personal_rows, [50, 8, 10, 110])

W, F, N = "Works", "Files", "'Not yet scanned'"
nw, nf, nn, ne = len(work_rows) + 1, len(file_rows) + 1, len(outside) + 1, len(elsewhere) + 1
s["A1"] = f"{NAS} books merge — dry run"
s["A1"].font = TITLE
s["A2"] = (f"Prepared {date.today():%d %B %Y}. A plan only: no file has been copied, moved or deleted. Every book "
           f"Media Catalog found on disk on {NAS} becomes one entry in a new Calibre library, with one file per "
           f"format. {BACKUP_NAS}, OneDrive and {DISK} stay as they are, as backups.")
s["A2"].font = NOTE
s.merge_cells("A2:D2")
s["A2"].alignment = WRAP
s.row_dimensions[2].height = 54

row = 4


def header(cells):
    global row
    for col, text in enumerate(cells, 1):
        c = s.cell(row=row, column=col, value=text)
        c.font, c.fill = HEAD, HEAD_FILL
    row += 1


def line(values, bold=False, fmt=None):
    global row
    for col, v in enumerate(values, 1):
        c = s.cell(row=row, column=col, value=v)
        c.font = BOLD if bold else BODY
        if fmt and col > 1:
            c.number_format = fmt[col - 2] if isinstance(fmt, list) else fmt
    row += 1


def note(text, height=28):
    global row
    c = s.cell(row=row, column=1, value=text)
    c.font, c.alignment = NOTE, WRAP
    s.merge_cells(start_row=row, start_column=1, end_row=row, end_column=4)
    s.row_dimensions[row].height = height
    row += 1


header(["What the merged library gets", "Entries", "Files kept", "Size kept (GB)"])
for kind, lib in LIBRARIES.items():
    line([f"{lib}  ({TARGET}/{lib})", f'=COUNTIF({W}!$B$2:$B${nw},"{kind}")',
          f'=SUMIF({W}!$B$2:$B${nw},"{kind}",{W}!$J$2:$J${nw})',
          f'=SUMIF({W}!$B$2:$B${nw},"{kind}",{W}!$N$2:$N${nw})/1024'], fmt="#,##0")
line(["Total", f"=SUM(B{row - 2}:B{row - 1})", f"=SUM(C{row - 2}:C{row - 1})", f"=SUM(D{row - 2}:D{row - 1})"],
     bold=True, fmt="#,##0")
note(LOCAL["target_note"])

row += 1
header(["Shelves inside the Calibre Library", "Entries", "Files kept", "Size kept (GB)"])
for shelf in sorted({r[2] for r in work_rows if r[2]}, key=lambda x: (not x.startswith("RPG"), x)):
    line([shelf, f'=COUNTIF({W}!$C$2:$C${nw},"{shelf}")', f'=SUMIF({W}!$C$2:$C${nw},"{shelf}",{W}!$J$2:$J${nw})',
          f'=SUMIF({W}!$C$2:$C${nw},"{shelf}",{W}!$N$2:$N${nw})/1024'], fmt=["#,##0", "#,##0", "#,##0.0"])
note(LOCAL["shelf_note"], 52)

row += 1
header(["Files on disk today", "Files", "Size (GB)", ""])
for action, label in (("keep", "Kept"), ("duplicate", "Left out: exact duplicate (same format and size)"),
                      ("other version", "Left out: same format, different size")):
    line([label, f'=COUNTIF({F}!$B$2:$B${nf},"{action}")',
          f'=SUMIF({F}!$B$2:$B${nf},"{action}",{F}!$D$2:$D${nf})/1024/1024'], fmt="#,##0")
line(["Kept files that are inside a zip (copied out of it)", f'=COUNTIFS({F}!$B$2:$B${nf},"keep",{F}!$F$2:$F${nf},"yes")'],
     fmt="#,##0")
note("Left-out files stay where they are; nothing is deleted. ‘Other version’ files are listed per work in Files, "
     "so you can swap one in before anything is copied.")
note(f"{LOCAL['mirror_label']} is a copy of {LOCAL['main_label']}: {mirror_shared:,} of its {len(mirror_rel):,} files are the same "
     f"files at the same paths, so they come out as exact duplicates. The other {len(mirror_rel) - mirror_shared:,} "
     "are only in the copy and are kept like any other book.", 40)

row += 1
header(["Needs your call before copying", "Works", "", ""])
line(["Any of the below", f'=COUNTA(\'Needs your call\'!$A$2:$A${len(call_rows) + 1})'], bold=True, fmt="#,##0")
for fl in ("title/author uncertain", "no author", "only a ZIP/RAR: unpack first",
           "issue numbers differ: may be several issues", "same format in different sizes"):
    line([fl[:1].upper() + fl[1:], f'=COUNTIF({W}!$P$2:$P${nw},"*{fl}*")'], fmt="#,##0")

row += 1
header(["Not in this plan", "Count", "", ""])
line([f"Ebook files in {NAS} folders not scanned (backups, documents, stray comics)",
      f"=SUM({N}!$B$2:$B${nn})"], fmt="#,##0")
line([f"Books the audit found only on {BACKUP_NAS}, OneDrive or {DISK}", f"=COUNTA('Only elsewhere'!$A$2:$A${ne})"], fmt="#,##0")
line(["Catalogue entries with no file on disk (references)", data["references"]["works"]], fmt="#,##0")
line(["Hidden entries (archives with no book inside, vanished loose files)", data["hidden"]], fmt="#,##0")
line([LOCAL["personal_label"],
      f"=COUNTA('Personal documents'!$A$2:$A${len(personal_rows) + 1})"], fmt="#,##0")
note(LOCAL["not_scanned_note"], 40)

row += 1
header(["How the merge would run", "", "", ""])
for step in ("1. Pick which folders under ‘Not yet scanned’ belong in the library; scan those and rebuild this plan.",
             "2. You settle ‘Needs your call’ and confirm the target folders.",
             "3. Copy (never move) each kept file into the new libraries with Calibre, carrying Media Catalog's "
             "title, authors, series and cover; check the counts and checksums.",
             "4. Point Media Catalog at the new libraries. The old folders stay until you decide to retire them."):
    note(step, 30)

for col, w in zip("ABCD", (62, 12, 12, 14)):
    s.column_dimensions[col].width = w
wb.calculation.fullCalcOnLoad = True
wb.save(OUT)

kinds = Counter(r[1] for r in work_rows)
acts = Counter(r[1] for r in file_rows)
print("works", len(work_rows), dict(kinds))
print("files", dict(acts), "kept GB", round(sum(r[3] for r in file_rows if r[1] == "keep") / 1024 / 1024, 1),
      "dup GB", round(sum(r[3] for r in file_rows if r[1] == "duplicate") / 1024 / 1024, 1),
      "other GB", round(sum(r[3] for r in file_rows if r[1] == "other version") / 1024 / 1024, 1))
print("kept inside zip", sum(1 for r in file_rows if r[1] == "keep" and r[5] == "yes"))
print("needs call", len(call_rows), dict(flag_counts))
print("shelves", dict(Counter(r[2] for r in work_rows if r[2])), "personal files", len(personal_rows))
print("outside folders", len(outside), "ebook files", sum(r[1] for r in outside), "elsewhere", len(elsewhere))
