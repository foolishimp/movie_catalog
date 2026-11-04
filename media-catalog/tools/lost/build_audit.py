"""Write the lost-books audit workbook from recovered.json / gone.json (made by report.py)."""
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import date

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA = os.path.join(ROOT, "tools", "data", "lost")
os.makedirs(DATA, exist_ok=True)
OUT = sys.argv[1]
ENV = os.path.join(ROOT, ".env")
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
NAS, BACKUP_NAS, DISK = LOCAL["main_nas"], LOCAL["backup_nas"], LOCAL["backup_disk"]

recovered = json.load(open(os.path.join(DATA, "recovered.json")))
gone = json.load(open(os.path.join(DATA, "gone.json")))
for r in recovered + gone:
    r["authors"] = [a.replace("|", ",") for a in r["authors"]]      # Calibre stores "Jr." commas as "|"

# Folders Media Catalog indexes (BOOK_DIRS); read here, never printed.
book_dirs = []
for line in open(ENV):
    if line.startswith("BOOK_DIRS="):
        book_dirs = [d.rstrip("/") + "/" for d in line.split("=", 1)[1].strip().strip('"\'').split(":") if d.strip()]

MATCH = {
    "calibre-record": ("Same book in another Calibre library", "certain"),
    "calibre-folder": ("Calibre's own folder for this book", "certain"),
    "calibre-filename": ("Calibre's own file name", "high"),
    "title+author": ("Title and author in the file name", "high"),
    "fuzzy-author-folder": ("Close title in the author's folder", "medium"),
    "title+folder": ("Title in a folder named for the author", "medium"),
}
CLOUD = {"OneDrive", "Google Drive", "iCloud Drive"}
COMIC = {"CBR", "CBZ", "CB7"}

FONT = "Arial"
HEAD = Font(name=FONT, bold=True, color="FFFFFF")
HEAD_FILL = PatternFill("solid", fgColor="5B4636")
BODY = Font(name=FONT, size=10)
BOLD = Font(name=FONT, size=10, bold=True)
TITLE = Font(name=FONT, size=14, bold=True)
NOTE = Font(name=FONT, size=9, italic=True, color="555555")
WRAP = Alignment(wrap_text=True, vertical="top")


def mb(files):
    return round(sum(f.get("size") or 0 for f in files) / 1048576, 1)


def where(copy):
    return copy["archive"] or copy["path"]


def indexed(copy):
    return not copy["archive"] and any(copy["path"].startswith(d) for d in book_dirs)


def table(ws, headers, rows, widths):
    ws.append(headers)
    for c in ws[1]:
        c.font, c.fill, c.alignment = HEAD, HEAD_FILL, Alignment(vertical="center", wrap_text=True)
    for r in rows:
        ws.append(r)
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.font = BODY
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions


wb = Workbook()
summary = wb.active
summary.title = "Summary"

# --- Found copies -------------------------------------------------------------
found_rows = []
folder_books = defaultdict(set)
folder_place = {}
for i, r in enumerate(recovered):
    copies = r["copies"]
    best = copies[0]
    label, confidence = MATCH[best["kind"]]
    places = sorted({c["where"] for c in copies})
    others = []
    for c in copies[1:]:
        p = where(c) + (f"  ⟶ {c['path']}" if c["archive"] else "")
        if p not in others and p != where(best):
            others.append(p)
    for c in copies:
        folder = "/".join(where(c).split("/")[:-1])
        folder_books[folder].add((r["library"], r["id"]))
        folder_place[folder] = c["where"]
    found_rows.append([
        r["library"], r["id"], r["title"], "; ".join(r["authors"]), r["added"],
        " ".join(sorted({f["format"] for f in r["files"]})), mb(r["files"]),
        where(best), best["path"] if best["archive"] else "", best["where"], label, confidence,
        "yes" if any(indexed(c) for c in copies) else "no",
        "yes" if all(c["where"] in CLOUD for c in copies) else "no",
        "yes" if all(c["archive"] for c in copies) else "no",
        len({where(c) for c in copies}), ", ".join(places), "\n".join(others[:3]),
    ])
found_rows.sort(key=lambda x: (x[0], x[3].lower(), x[2].lower()))
ws = wb.create_sheet("Found copies")
table(ws, ["Calibre library", "Calibre id", "Title", "Authors", "Added to Calibre", "Calibre formats",
           "Calibre size (MB)", "Best copy (file or archive)", "Member inside archive", "Best copy place",
           "How it was matched", "Confidence", "In a folder Media Catalog scans", "Only in cloud storage",
           "Only inside archives", "Copies found", "Places with a copy", "Other copies (up to 3)"],
      found_rows, [16, 9, 40, 28, 12, 18, 10, 70, 40, 12, 30, 11, 14, 11, 11, 9, 28, 70])

# --- Not found ----------------------------------------------------------------
gone_rows = []
for r in gone:
    fmts = sorted({f["format"] for f in r["files"]})
    kind = "Comic or magazine scan" if fmts and set(fmts) <= COMIC else "Ebook"
    gone_rows.append([r["library"], r["id"], r["title"], "; ".join(r["authors"]),
                      (r["authors"] or [""])[0], r["added"], " ".join(fmts), mb(r["files"]), kind, r["path"]])
gone_rows.sort(key=lambda x: (x[0], x[4].lower(), x[2].lower()))
ws = wb.create_sheet("Not found")
table(ws, ["Calibre library", "Calibre id", "Title", "Authors", "First author", "Added to Calibre",
           "Calibre formats", "Calibre size (MB)", "Kind", "Calibre folder (relative to its library)"],
      gone_rows, [16, 9, 44, 30, 22, 12, 18, 10, 20, 60])

# --- Sources ------------------------------------------------------------------
src_rows = [[folder, folder_place[folder], len(ids), "yes" if folder.lower().endswith((".rar", ".zip", ".7z")) else ""]
            for folder, ids in folder_books.items()]
src_rows.sort(key=lambda x: -x[2])
ws = wb.create_sheet("Sources")
table(ws, ["Folder (or archive) holding a copy", "Place", "Missing books with a copy here", "Archive"],
      src_rows, [110, 12, 14, 9])

# --- Summary (formulas over the data sheets) ----------------------------------
F = "'Found copies'"
G = "'Not found'"
nf, ng = len(found_rows) + 1, len(gone_rows) + 1
s = summary
s["A1"] = "Lost Calibre books — audit"
s["A1"].font = TITLE
s["A2"] = (f"Prepared {date.today():%d %B %Y}. Calibre records whose files are no longer in their library "
           f"folder, searched for across {NAS}, {BACKUP_NAS}, {DISK}, OneDrive, Google Drive, iCloud Drive and this Mac "
           "(read-only; nothing was moved or copied).")
s["A2"].font = NOTE
s.merge_cells("A2:F2")
s["A2"].alignment = WRAP
s.row_dimensions[2].height = 42

row = 4
def header(cells):
    global row
    for col, text in enumerate(cells, 1):
        c = s.cell(row=row, column=col, value=text)
        c.font, c.fill = HEAD, HEAD_FILL
    row += 1

def line(values, bold=False):
    global row
    for col, v in enumerate(values, 1):
        c = s.cell(row=row, column=col, value=v)
        c.font = BOLD if bold else BODY
    row += 1

header(["Calibre library", "Missing records", "Copy found", "Not found", "Found %"])
libs = sorted({r[0] for r in found_rows} | {r[0] for r in gone_rows})
first = row
for lib in libs:
    r = row
    line([lib, f"=C{r}+D{r}", f"=COUNTIF({F}!$A$2:$A${nf},A{r})", f"=COUNTIF({G}!$A$2:$A${ng},A{r})",
          f"=IF(B{r}=0,0,C{r}/B{r})"])
last = row - 1
r = row
line(["Total", f"=SUM(B{first}:B{last})", f"=SUM(C{first}:C{last})", f"=SUM(D{first}:D{last})",
      f"=IF(B{r}=0,0,C{r}/B{r})"], bold=True)
for rr in range(first, row):
    s.cell(row=rr, column=5).number_format = "0%"
    for col in (2, 3, 4):
        s.cell(row=rr, column=col).number_format = "#,##0"

row += 1
header(["Where the found copies are", "Books with a copy there", "", "", ""])
first = row
for place in (NAS, BACKUP_NAS, "OneDrive", DISK, "Google Drive", "iCloud Drive", "this Mac"):
    line([place, f'=COUNTIF({F}!$Q$2:$Q${nf},"*{place}*")'])
s.cell(row=row, column=1, value="A book can have copies in several places, so these overlap.").font = NOTE
row += 1

row += 1
header(["Found copies — how sure", "Books", "", "", ""])
for conf in ("certain", "high", "medium"):
    line([f"Best match {conf}", f'=COUNTIF({F}!$L$2:$L${nf},"{conf}")'])
line(["In a folder Media Catalog scans", f'=COUNTIF({F}!$M$2:$M${nf},"yes")'])
line(["Found only inside an archive (RAR/ZIP)", f'=COUNTIF({F}!$O$2:$O${nf},"yes")'])
line(["Found only in cloud storage", f'=COUNTIF({F}!$N$2:$N${nf},"yes")'])
s.cell(row=row, column=1, value="“In a folder Media Catalog scans” means at least one copy is a loose file inside the folders Media "
       "Catalog scans (BOOK_DIRS), so its book scan picks that copy up as its own record.").font = NOTE
row += 1

row += 1
header(["Not found — kind", "Books", "", "", ""])
for kind in ("Ebook", "Comic or magazine scan"):
    line([kind, f'=COUNTIF({G}!$I$2:$I${ng},"{kind}")'])

row += 1
header(["Not found — authors with the most", "Books", "", "", ""])
for author, _ in Counter(r[4] for r in gone_rows).most_common(25):
    line([author, f'=COUNTIF({G}!$E$2:$E${ng},A{row})'])

row += 1
header(["How copies were matched", "", "", "", ""])
for kind, (label, conf) in MATCH.items():
    line([label, conf])
notes = [
    "Calibre truncates file names, so a name that fits more than 3 records (magazine issues, omnibus titles) "
    "was ignored rather than credited to all of them.",
    "Archives (RAR/ZIP/7z over 200 KB) were listed member by member; cloud-only OneDrive files not downloaded "
    "to this Mac (391 archives, 3.3 GB) could not be listed and were skipped.",
    "Medium-confidence matches are worth a glance before relying on them; the ‘Other copies’ column shows alternatives.",
]
for n in notes:
    c = s.cell(row=row, column=1, value=n)
    c.font, c.alignment = NOTE, WRAP
    s.merge_cells(start_row=row, start_column=1, end_row=row, end_column=6)
    s.row_dimensions[row].height = 28
    row += 1

for col, w in zip("ABCDEF", (44, 18, 12, 12, 10, 10)):
    s.column_dimensions[col].width = w
for rr in s.iter_rows():
    for c in rr:
        if isinstance(c.value, str) and c.value.startswith("=COUNT"):
            c.number_format = "#,##0"

wb.calculation.fullCalcOnLoad = True
wb.save(OUT)

# Python-side totals to check the formulas against.
print("found", len(found_rows), "gone", len(gone_rows))
print("by library", Counter(r[0] for r in found_rows), Counter(r[0] for r in gone_rows))
print("confidence", Counter(r[11] for r in found_rows))
print("indexed", sum(r[12] == "yes" for r in found_rows), "cloud-only", sum(r[13] == "yes" for r in found_rows))
print("archive-only", sum(r[14] == "yes" for r in found_rows))
print("places", {p: sum(p in r[16] for r in found_rows) for p in (NAS, BACKUP_NAS, "OneDrive", DISK)})
print("kinds", Counter(r[8] for r in gone_rows))
print("book_dirs configured:", len(book_dirs))
