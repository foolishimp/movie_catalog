# Media Catalog

A Python + Postgres tool for cataloging, searching, and deduplicating a large media collection.

1. **Scan** — Recursively walks your directories, parses filenames, stores everything in Postgres
2. **Enrich** — Pulls metadata from OMDb (genres, ratings, cast, directors, posters, synopses)
3. **Browse** — Web UI + CLI for searching, filtering, and finding duplicates

---

## Quick Start (one command)

```bash
python start.py
```

This single script handles everything:
- Starts Docker Desktop if it isn't running
- Starts the Postgres container
- Waits for the database to be ready
- Installs Python dependencies
- Runs the directory scanner in the background
- Runs the OMDb enricher in the background
- Opens the web UI in your browser
- Runs the web server (Ctrl+C to stop everything)

### First-time setup

**1. Install Docker Desktop** (if not already installed)
https://www.docker.com/products/docker-desktop/

**2. Install Python dependencies**
```bash
pip install -r requirements.txt
```

**3. Configure `.env`**
```bash
cp .env.example .env
```

Edit `.env` — the required fields are:

| Variable | Description |
|---|---|
| `MEDIA_DIRS` | Colon-separated paths to scan, e.g. `/Volumes/Movies:/Volumes/Series` |
| `OMDB_API_KEY` | Free key from http://www.omdbapi.com/apikey.aspx (1,000 lookups/day) |
| `DATABASE_URL` | Already set correctly for the Docker Postgres — no change needed |
| `BOOK_DIRS` | Optional. Colon-separated ebook folders for the Books tab, e.g. `/Volumes/Library/Books` |
| `GAME_BOOK_FOLDER` | Optional. Name of the folder holding game books, whose credits are always read. Default `rpg` |
| `EBOOK_CONVERT` | Optional. Calibre's `ebook-convert`, used by the reader for formats browsers can't show; found on `PATH` or in `/Applications/calibre.app` by default |
| `BOOKS_CACHE_DIR` | Optional. Where the reader keeps converted books; default `.books_cache/` |

**4. Run**
```bash
python start.py
```

Open http://localhost:8080

---

## start.py options

```
python start.py               # full startup (scanner + enricher + web)
python start.py --no-scan     # skip scanner (catalog already up to date)
python start.py --no-enrich   # skip OMDb enricher (quota used up, or not configured)
python start.py --no-browser  # don't auto-open browser tab
```

Background processes write logs to `.scanner.log` and `.enricher.log` in the project directory.

The OMDb enricher has a free tier of **1,000 requests/day**. If it stops mid-run just re-run `start.py` the next day — it only processes unenriched entries.

---

## Manual commands

If you prefer to run steps individually:

### Start Postgres
```bash
docker compose up -d db
```

### Scan directories
```bash
# Uses MEDIA_DIRS from .env
python -m scanner.scan

# Or specify directories explicitly
python -m scanner.scan /Volumes/Movies /Volumes/Series

# Re-scan everything (re-parses all filenames)
python -m scanner.scan --rescan
```

### Enrich with OMDb metadata
```bash
# Enrich all unenriched entries
python -m enricher.omdb

# Limit to a batch (useful when near the daily quota)
python -m enricher.omdb --limit 100

# Re-enrich everything
python -m enricher.omdb --all

# Enrich a specific entry
python -m enricher.omdb --id 42
```

### Index books
```bash
# Uses BOOK_DIRS from .env; Calibre libraries (metadata.db) inside are picked up
python -m books.scan

# Re-read every file instead of only new or changed ones
python -m books.scan --rescan

# Re-decide titles/authors from stored evidence without touching the share
python -m books.resolve

# Read the first pages of books still lacking an author (PDF text, OCR for
# scanned PDFs, Calibre for LRF/CHM/LIT...), then re-resolve; each file once
python -m books.frontpages
python -m books.frontpages --limit 40 --dry-run

# Look up books still lacking an author by ISBN in Open Library (only the
# ISBN is sent; answers are cached in isbn_lookups), then re-resolve
python -m books.lookup
python -m books.lookup --dry-run
```

### Start web server
```bash
python -m uvicorn web.app:app --port 8080 --reload
# Open http://localhost:8080
```

### Or run everything via Docker
```bash
docker compose up
# Open http://localhost:8080
```

---

## CLI

```bash
python cli.py search "inception"
python cli.py search "nolan" --field director
python cli.py search "Science Fiction" --field genre
python cli.py dupes
python cli.py stats
python cli.py export catalog.csv
```

### Tagging
```bash
python cli.py tag 42 add "favorites"
python cli.py tag 42 add "to-rewatch"
python cli.py tag 42 remove "favorites"
```

---

## Web UI

| Page | URL | Description |
|---|---|---|
| Dashboard | `/` | Stats, breakdowns, and recent content with one card per series |
| Browse | `/browse` | Search/filter content; series are grouped into drill-down cards |
| Detail | `/detail/<id>` | Full metadata, poster, cast, file info, duplicates |
| Series | `/series` | Sortable series browser with season/episode drill-down |
| Duplicates | `/duplicates` | All duplicate groups, file sizes, reclaimable space |

Movie cards in Browse expose file actions. A grouped series card opens its
season/episode browser and exposes its folder action instead of pretending one
episode represents the whole series.

The Browse and Series pages support title, release year, rating, popularity,
total size, and recently-added ordering. For series, these use show-level
aggregates; recently added is the newest catalogued file in that show.

Movie file actions include:
- **▶ VLC** — open the file directly in VLC
- **📂 Reveal** — open the enclosing folder in Finder with the file selected
- **🔗 Link** — copy a deep link to the detail page

When browsing from another Windows or macOS computer, the server-only actions
are replaced by **▶ VLC here**. Use **Local VLC setup** once on that computer;
afterward the browser launches its local VLC with the catalog's mapped
`smb://` URI. The generated handler is restricted to the SMB hosts configured
in `PATH_MAPPINGS`.

### Books

The 📚 Books tab (`/books`) has its own warm theme over the same layout,
cards, star/label menus and file actions as the movie pages.

| Page | URL | Description |
|---|---|---|
| Books | `/books` | Search and filter by author, series, tag, award, publisher, rating, decade, language and format, with a Calibre-style tag browser |
| Book | `/books/<id>` | Cover, awards, description, series run, every file and copy, and the evidence behind the title/author |
| Categories | `/books/category/<kind>` | Full lists of authors (by surname), series, tags, awards, publishers and so on |
| Reader | `/books/<id>/read` | Reads the book in the browser (📖 Read on cards and book pages); `?file=<id>` picks a file |

The reader uses [foliate-js](https://github.com/johnfactotum/foliate-js)
(vendored in `web/static/foliate/`, MIT) for EPUB, MOBI/AZW/AZW3/PRC, FB2
and CBZ, and the browser's own viewer for PDF. Comic RARs (CBR) are repacked
to CBZ, and LIT, RTF, DOC/DOCX, ODT, HTMLZ, PDB, LRF, CHM, TXT and zipped
books are converted to EPUB once with Calibre's `ebook-convert`; converted
files are cached locally and nothing is written to the share. It starts with
the best format among the book's files and its other copies, falls back to the
next if one can't be opened (DRM, failed conversion), and remembers your
place, theme, text size and layout in the browser. DJVU is not supported.

Links go to Goodreads (by Goodreads id, then ISBN, then search), ISFDB and
Open Library. **⟳ Rescan books** runs `books.scan` in the background.

`books.scan` treats a Calibre `metadata.db` as one source of evidence, not
the truth. Each book's title and authors are voted on by independent
sources: Calibre (ISFDB-matched records count most), the publisher metadata
inside the epub, PDF document info, the title page and copyright text, the
filename and the folder. Calibre records whose title and author were stored
the wrong way round are swapped back when the rest of the corpus clearly
treats the stored "author" as a title; doubtful cases are flagged
**Needs review**. Nothing on the share is written to.

A Calibre library archived as one zip per author (its usual `Author/Title (id)/`
folders inside each zip) is read in place: each book inside a zip is stored as
`archive.zip::member` and opens, reads and shows its cover like a loose file.
A file with the same name and size as one already read (a copy of a library
inside another folder) takes that reading instead of being read again.

Calibre's placeholder author "Unknown" counts as no author; such records often
hold "Title - Author" as the title, and that is split back into its parts.

`books.frontpages` gives books with no author, or an uncertain one, a second
look: their first pages are read (with OCR via `pdftoppm` and `tesseract` when
a PDF has no text layer) for "by …" and copyright lines, a known author's name,
or failing those a person's full name, as on technical books' title pages. For
a book that already has an author, these names only confirm one side of its
file name, so an epigraph's poet never replaces the novelist.

Game and technical books credit their writers instead of a "by" line, so
credits lines ("Design: Bruce R. Cordell", "Written by Jeff Grubb and Kate
Novak") count as bylines. Bylines set in capitals are title-cased, and more
than four bylines are a magazine's contributors, not its authors. In file
names, a game line or publisher (D&D, GURPS, Ars Magica, TSR, …) is the series
or a corporate name, never the author. Publishers' product codes ("ICE 5805",
"WOC 1150") and the kind of game book ("Campaigns", "Supplement") are labels,
neither title nor author.

Some names on a book are not its author. A one-word name in a PDF's author
field is a login ("Morgan", "a.b.example"); a title-page line that repeats
the title ("SPELL LAW") is not a byline; the names in a dated magazine issue
("New Scientist 2009-01-17") are its staff; studios and imprints in copyright
lines ("Paramount Pictures", "Pocket Books") are companies; and a tie-in
franchise ("Star Trek", "Doctor Who") files a book only when nothing names its
writer. A bare number between parts of a file name is the series number ("War
of the Spider Queen - 1 - Dissolution - R. A. Salvatore"), a bare hyphen after
a full name splits author from title ("Charles L. Harness-The Alchemist"), and
degrees after a name ("Lita Epstein, MBA") are dropped. A publisher's name is
a company on its own ("Springer", "John Wiley & Sons") but a surname after a
given name ("Nancy Springer", "Ridley Pearson", "O'Reilly, Bill").

`books.lookup` sends the ISBNs of books that still have no author to Open
Library, one request per 50, and caches each answer, misses included. A
record's authors vote only when its title agrees with the book's own (most
words of the shorter title appear in the other), since an ISBN read from a
front page can belong to another book in the same series. When the book's own
record names the same person, its author list stands unless the catalogue's is
longer, because catalogue records often name only the first author.
Organisations ("LearningExpress (Organization)", "Adobe Creative Team") are
dropped.

### JSON API
```
GET /api/search?q=inception
GET /api/browse?sort=recent — paged display cards; series are grouped
GET /api/stats
GET /api/open-local-vlc/<id> — launch local-client VLC protocol with mapped SMB URI
POST /api/open/<id>       — open in VLC
POST /api/reveal/<id>     — reveal in Finder
POST /api/tags/<id>       — add/remove tags
```

---

## Architecture

```
media-catalog/
├── start.py              # One-command startup script
├── docker-compose.yml    # Postgres + web UI
├── sql/init.sql          # Schema: full-text search, trigram indexes, views
├── db.py                 # Database connection helper
├── scanner/scan.py       # Phase 1: directory walker + guessit parser
├── books/scan.py         # Books: walk BOOK_DIRS, read Calibre + file evidence
├── books/resolve.py      # Books: vote on title/author, fix swaps, group copies
├── books/frontpages.py   # Books: read first pages of books lacking an author
├── books/lookup.py       # Books: look up authorless books by ISBN (Open Library)
├── enricher/omdb.py      # Phase 2: OMDb metadata enrichment
├── enricher/tmdb.py      # Phase 2 (alt): TMDb enrichment (requires API key)
├── web/app.py            # Phase 3: Starlette web app
├── books/reading.py      # Books: prepare files for the in-browser reader
├── web/books.py          # Books tab routes and queries
├── web/common.py         # Shared templates, path mapping, host opener, background jobs
├── web/templates/        # Jinja2 templates (dark theme)
├── web/static/foliate/   # Vendored foliate-js e-book renderer (MIT)
├── cli.py                # CLI: search, stats, export, tagging
├── .env                  # Your configuration (not committed)
└── requirements.txt
```

## Notes

- The scanner is incremental — re-running only processes new files unless you pass `--rescan`
- OMDb enrichment deduplicates API calls — 20 copies of "The Matrix (1999)" only costs 1 request
- Postgres uses trigram indexes for fuzzy search and full-text search with weighted ranking
- All data lives in a single Postgres volume — back up with `pg_dump media_catalog`
- Nothing is renamed or moved on disk — this is a read-only catalog
