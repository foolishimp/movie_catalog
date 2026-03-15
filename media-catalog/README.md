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
| Dashboard | `/` | Stats, genre breakdown, year distribution, recent additions |
| Browse | `/browse` | Full-text search, filter by type/genre/year/rating/director |
| Detail | `/detail/<id>` | Full metadata, poster, cast, file info, duplicates |
| Series | `/series` | Series browser with season/episode drill-down |
| Duplicates | `/duplicates` | All duplicate groups, file sizes, reclaimable space |

Each card in Browse has three action buttons:
- **▶ VLC** — open the file directly in VLC
- **📂 Reveal** — open the enclosing folder in Finder with the file selected
- **🔗 Link** — copy a deep link to the detail page

### JSON API
```
GET /api/search?q=inception
GET /api/stats
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
├── enricher/omdb.py      # Phase 2: OMDb metadata enrichment
├── enricher/tmdb.py      # Phase 2 (alt): TMDb enrichment (requires API key)
├── web/app.py            # Phase 3: Starlette web app
├── web/templates/        # Jinja2 templates (dark theme)
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
