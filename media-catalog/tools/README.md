# Tools

Standalone scripts for planning, checking and repairing the library. They aren't part of the web app.

Run them from anywhere with the project's Python. The workbook tools also need `pip install -r tools/requirements.txt`.

Everything the tools read and write stays in `tools/data/`, which git ignores. Keep data there and only code in git.

Folder names, NAS names and anything else personal belong in `tools/data/local.json`, never in the code. `tools/local.example.json` shows its layout with made-up values.

Run `python tools/checks/no_private.py` before every commit: it fails if a private name is in anything git would publish.

## `merge/` – the book merge plan

Nothing here copies or moves a book.

| Script | Purpose |
|---|---|
| `export.py` | Export the catalogue to `plan_data.json` |
| `inventory.py` | List book-like files outside the scanned folders |
| `build_plan.py OUT.xlsx` | Write the merge plan workbook |
| `check_formulas.py PLAN.xlsx` | Check the Summary formulas without a spreadsheet app |
| `snapshot.py OUT.json` | Save each book's title, authors and series |
| `diff_snap.py BEFORE.json AFTER.json` | Compare two snapshots |
| `probe.py ID [ID ...]` | Resolve chosen books without writing anything |
| `byline_scores.py` | Score stored bylines as names |
| `credits_dry.py` | Show what a credits re-read of game-book PDFs would change |
| `lookup_dry.py` | Show which cached ISBN records the resolver would accept |
| `verify_scan.py` | Check the results of a scan |

## `lost/` – finding books missing from Calibre

Run in this order: `missing.py`, `search.py`, `records.py`, `archives.py`, `fuzzy.py`, `report.py`, then `build_audit.py OUT.xlsx`.

| Script | Purpose |
|---|---|
| `missing.py` | List Calibre records whose folder is gone |
| `search.py [ROOT ...]` | Walk the mounted shares for copies |
| `records.py` | Match against other Calibre libraries |
| `archives.py` | Match against the contents of archives |
| `fuzzy.py` | Fuzzy-match inside folders named after the author |
| `report.py` | Merge the results into found and not found |
| `build_audit.py OUT.xlsx` | Write the audit workbook |

## `checks/` – catalogue and resolver checks

| Script | Purpose |
|---|---|
| `no_private.py` | Find private names in anything git would publish |
| `smoke_web.py [MODULE]` | Request the main pages and report server errors |
| `pilot_review.py` | Print scan totals and random samples |
| `profile_extract.py` | Time metadata extraction |
| `libs.py` | Load every known Calibre library on the NAS |
| `lib_walk.py ROOT [ROOT ...]` | Count ebooks by format under each folder |
| `calibre_summary.py DIR` | Summarise copied Calibre databases |
| `calibre_verify.py DIR BASE` | Sample how many of their files are on disk |
| `swap_dryrun.py` | Report title and author swaps, in memory |
| `swap_recall.py` | Measure how many planted swaps the resolver undoes |

## `nas/` – moving large folders between shares

| Script | Purpose |
|---|---|
| `watch_move.sh SOURCE DEST` | Report when a Finder move finishes, stalls or loses a share |
| `copy_missing.sh SOURCE DEST` | Copy files missing from DEST; never overwrites or deletes |
| `spotcheck.py SOURCE DEST` | Compare sizes and samples of each file; reads only |

Run long jobs under `caffeinate -i` so the Mac stays awake.
