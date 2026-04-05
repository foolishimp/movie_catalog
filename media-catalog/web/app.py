"""
Phase 3: Web UI for browsing, searching, and managing the catalog.

Run standalone:
    python -m uvicorn web.app:app --port 8080 --reload

Or via docker-compose:
    docker compose up web
"""
import os
import sys
import math
import json

from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.responses import JSONResponse, HTMLResponse
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import query

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

TMDB_IMG_BASE = "https://image.tmdb.org/t/p"
PER_PAGE = 48
HOST_OPENER_URL = os.getenv("HOST_OPENER_URL", "").rstrip("/")


def _parse_network_mounts():
    """Parse NETWORK_MOUNTS env var into {volume_path: mount_url} dict."""
    raw = os.getenv("NETWORK_MOUNTS", "")
    mounts = {}
    for pair in raw.split(":"):
        if "=" in pair:
            vol, url = pair.split("=", 1)
            # Rejoin if the URL had colons (smb://..., afp://...)
            mounts[vol] = url
    # Re-parse properly: split on volume paths
    mounts = {}
    for entry in raw.split(":/Volumes/"):
        if "=" not in entry:
            continue
        if not entry.startswith("/"):
            entry = "/Volumes/" + entry
        vol, url = entry.split("=", 1)
        mounts[vol] = url
    return mounts


def _host_open(endpoint: str, file_path: str) -> dict | None:
    """Delegate an open command to the host opener service. Returns response dict or None."""
    if not HOST_OPENER_URL:
        return None
    import urllib.request
    try:
        req = urllib.request.Request(
            f"{HOST_OPENER_URL}/{endpoint}",
            data=json.dumps({"file_path": file_path}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def ensure_mounted(file_path: str) -> bool:
    """If file_path doesn't exist, try to mount its network volume. Returns True if accessible."""
    if os.path.exists(file_path):
        return True
    import subprocess
    mounts = _parse_network_mounts()
    for vol_path, mount_url in mounts.items():
        if file_path.startswith(vol_path) and not os.path.ismount(vol_path):
            try:
                subprocess.run(["open", mount_url], check=True, timeout=15)
                # Wait briefly for the mount to appear
                import time
                for _ in range(10):
                    if os.path.exists(file_path):
                        return True
                    time.sleep(1)
            except Exception:
                pass
    return os.path.exists(file_path)


def _build_where(params) -> tuple[str, list]:
    """Build WHERE clause from query parameters."""
    clauses = []
    values = []

    q = params.get("q", "").strip()
    if q:
        # Use full-text search for plain words; always also match on path/filename
        has_special = any(c in q for c in '/\\.')
        if has_special:
            clauses.append("(directory ILIKE %s OR file_name ILIKE %s OR file_path ILIKE %s)")
            values.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
        else:
            clauses.append("(tsv @@ websearch_to_tsquery('english', %s) OR directory ILIKE %s OR file_name ILIKE %s)")
            values.extend([q, f"%{q}%", f"%{q}%"])

    media_type = params.get("type", "").strip()
    if media_type in ("movie", "series"):
        clauses.append("media_type = %s")
        values.append(media_type)

    genre = params.get("genre", "").strip()
    if genre:
        clauses.append("%s = ANY(genres)")
        values.append(genre)

    year = params.get("year", "").strip()
    if year:
        try:
            clauses.append("year = %s")
            values.append(int(year))
        except ValueError:
            pass

    year_decade = params.get("year_decade", "").strip()
    if year_decade:
        try:
            d = int(year_decade)
            clauses.append("year >= %s AND year < %s")
            values.extend([d, d + 10])
        except ValueError:
            pass

    min_rating = params.get("min_rating", "").strip()
    if min_rating:
        try:
            clauses.append("vote_average >= %s")
            values.append(float(min_rating))
        except ValueError:
            pass

    director = params.get("director", "").strip()
    if director:
        # Split on comma to match each name independently (handles "Joel Coen, Ethan Coen" vs "Ethan Coen, Joel Coen")
        parts = [p.strip() for p in director.split(",") if p.strip()]
        director_clauses = ["director ILIKE %s" for _ in parts]
        clauses.append("(" + " AND ".join(director_clauses) + ")")
        values.extend(f"%{p}%" for p in parts)

    actor = params.get("actor", "").strip()
    if actor:
        clauses.append("EXISTS (SELECT 1 FROM unnest(cast_names) a WHERE a ILIKE %s)")
        values.append(f"%{actor}%")

    tag = params.get("tag", "").strip()
    if tag:
        clauses.append("%s = ANY(tags)")
        values.append(tag)

    unenriched = params.get("unenriched", "").strip()
    if unenriched == "1":
        clauses.append("enriched_at IS NULL")

    where = " AND ".join(clauses) if clauses else "1=1"
    return where, values


async def homepage(request):
    stats = query("SELECT * FROM catalog_stats")
    s = stats[0] if stats else {}

    # Get genre counts
    genres = query("""
        SELECT unnest(genres) AS genre, count(*) AS cnt
        FROM media WHERE genres IS NOT NULL
        GROUP BY genre ORDER BY cnt DESC LIMIT 30
    """)

    # Get decade distribution
    years = query("""
        SELECT (year / 10 * 10) AS decade, count(*) AS cnt FROM media
        WHERE year IS NOT NULL
        GROUP BY decade ORDER BY decade DESC
    """)

    # Recent additions
    recent = query("""
        SELECT id, title, year, media_type, poster_path, vote_average, genres
        FROM media ORDER BY created_at DESC LIMIT 12
    """)

    return templates.TemplateResponse(request, "index.html", {
        "stats": s,
        "genres": genres,
        "years": years,
        "recent": recent,
        "tmdb_img": TMDB_IMG_BASE,
    })


async def browse(request):
    params = dict(request.query_params)
    where, values = _build_where(params)
    page = max(1, int(params.get("page", 1)))
    offset = (page - 1) * PER_PAGE

    sort_options = {
        "title": "title ASC",
        "year": "year DESC NULLS LAST",
        "rating": "vote_average DESC NULLS LAST",
        "size": "file_size_bytes DESC",
        "recent": "created_at DESC",
        "popularity": "popularity DESC NULLS LAST",
    }
    sort = sort_options.get(params.get("sort", "recent"), "created_at DESC")

    count_sql = f"SELECT count(*) AS total FROM media WHERE {where}"
    total = query(count_sql, values)[0]["total"]
    total_pages = max(1, math.ceil(total / PER_PAGE))

    sql = f"""
        SELECT id, title, year, media_type, poster_path, vote_average,
               genres, resolution, file_size_bytes, director, runtime_minutes, directory,
               season, episode
        FROM media
        WHERE {where}
        ORDER BY {sort}
        LIMIT {PER_PAGE} OFFSET {offset}
    """
    results = query(sql, values)

    # Get all genres for the filter dropdown
    all_genres = query("""
        SELECT DISTINCT unnest(genres) AS genre FROM media
        WHERE genres IS NOT NULL ORDER BY genre
    """)

    return templates.TemplateResponse(request, "browse.html", {
        "results": results,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "params": params,
        "genres": all_genres,
        "tmdb_img": TMDB_IMG_BASE,
    })


async def detail(request):
    entry_id = request.path_params["id"]
    rows = query("SELECT * FROM media WHERE id = %s", (entry_id,))
    if not rows:
        return HTMLResponse("Not found", status_code=404)

    entry = rows[0]

    # Find duplicates of this entry
    dupes = []
    if entry["duplicate_group"]:
        dupes = query(
            "SELECT id, file_path, file_size_bytes, resolution, codec, source FROM media WHERE duplicate_group = %s AND id != %s",
            (entry["duplicate_group"], entry_id)
        )

    return templates.TemplateResponse(request, "detail.html", {
        "entry": entry,
        "dupes": dupes,
        "tmdb_img": TMDB_IMG_BASE,
    })


async def duplicates(request):
    dupes = query("""
        SELECT norm_title, year, media_type, copy_count, paths, sizes, resolutions, ids
        FROM duplicate_candidates
        ORDER BY copy_count DESC
    """)
    total_waste = query("""
        SELECT pg_size_pretty(
            sum(total_extra)
        ) AS wasted FROM (
            SELECT sum(file_size_bytes) - max(file_size_bytes) AS total_extra
            FROM media
            WHERE duplicate_group IN (
                SELECT duplicate_group FROM media
                GROUP BY duplicate_group HAVING count(*) > 1
            )
            GROUP BY duplicate_group
        ) sub
    """)
    waste = total_waste[0]["wasted"] if total_waste and total_waste[0]["wasted"] else "0 bytes"

    return templates.TemplateResponse(request, "duplicates.html", {
        "dupes": dupes,
        "waste": waste,
        "tmdb_img": TMDB_IMG_BASE,
    })


async def api_browse(request):
    """JSON API for browse – supports infinite scroll."""
    params = dict(request.query_params)
    where, values = _build_where(params)
    page = max(1, int(params.get("page", 1)))
    offset = (page - 1) * PER_PAGE

    sort_options = {
        "title": "title ASC",
        "year": "year DESC NULLS LAST",
        "rating": "vote_average DESC NULLS LAST",
        "size": "file_size_bytes DESC",
        "recent": "created_at DESC",
        "popularity": "popularity DESC NULLS LAST",
    }
    sort = sort_options.get(params.get("sort", "recent"), "created_at DESC")

    count_sql = f"SELECT count(*) AS total FROM media WHERE {where}"
    total = query(count_sql, values)[0]["total"]
    total_pages = max(1, math.ceil(total / PER_PAGE))

    sql = f"""
        SELECT id, title, year, media_type, poster_path, vote_average,
               genres, resolution, file_size_bytes, director, runtime_minutes, directory,
               season, episode
        FROM media
        WHERE {where}
        ORDER BY {sort}
        LIMIT {PER_PAGE} OFFSET {offset}
    """
    results = query(sql, values)

    # Ensure JSON-safe types (datetimes, Decimals, etc.)
    safe = json.loads(json.dumps(results, default=str))

    return JSONResponse({
        "results": safe,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "per_page": PER_PAGE,
    })


async def api_search(request):
    """JSON API endpoint for programmatic search."""
    params = dict(request.query_params)
    where, values = _build_where(params)
    limit = min(int(params.get("limit", 50)), 500)

    sql = f"""
        SELECT id, title, year, media_type, genres, vote_average,
               director, file_path, resolution, file_size_bytes
        FROM media WHERE {where}
        ORDER BY title ASC LIMIT {limit}
    """
    results = query(sql, values)
    return JSONResponse({"count": len(results), "results": results})


async def api_stats(request):
    stats = query("SELECT * FROM catalog_stats")
    return JSONResponse(stats[0] if stats else {})


async def series(request):
    params = dict(request.query_params)
    show = params.get("show", "").strip()
    season_num = params.get("season", "").strip()
    q = params.get("q", "").strip()
    page = max(1, int(params.get("page", 1)))
    per_page = 48
    offset = (page - 1) * per_page

    # Level 3: episode list for a show + season
    if show and season_num:
        try:
            sn = int(season_num)
        except ValueError:
            sn = None
        episodes = query("""
            SELECT id, title, parsed_title, season, episode, resolution, codec,
                   file_size_bytes, file_path, directory, poster_path, vote_average
            FROM media
            WHERE media_type = 'series'
              AND lower(trim(parsed_title)) = lower(%s)
              AND season = %s
            ORDER BY episode ASC NULLS LAST, file_name ASC
        """, (show, sn))
        show_info = query("""
            SELECT max(title) AS title, max(poster_path) AS poster_path,
                   max(vote_average) AS vote_average, max(overview) AS overview
            FROM media WHERE media_type = 'series' AND lower(trim(parsed_title)) = lower(%s)
        """, (show,))
        return templates.TemplateResponse(request, "series_episodes.html", {
            "episodes": episodes,
            "show": show,
            "season": sn,
            "show_info": show_info[0] if show_info else {},
            "tmdb_img": TMDB_IMG_BASE,
        })

    # Level 2: season list for a show
    if show:
        seasons = query("""
            SELECT season, count(*) AS episode_count,
                   min(directory) AS directory
            FROM media
            WHERE media_type = 'series' AND lower(trim(parsed_title)) = lower(%s)
            GROUP BY season
            ORDER BY season ASC NULLS LAST
        """, (show,))
        show_info = query("""
            SELECT max(title) AS title, max(poster_path) AS poster_path,
                   max(vote_average) AS vote_average, max(overview) AS overview
            FROM media WHERE media_type = 'series' AND lower(trim(parsed_title)) = lower(%s)
        """, (show,))
        return templates.TemplateResponse(request, "series_seasons.html", {
            "seasons": seasons,
            "show": show,
            "show_info": show_info[0] if show_info else {},
            "tmdb_img": TMDB_IMG_BASE,
        })

    # Level 1: show list
    search_clause = ""
    values = []
    if q:
        search_clause = "AND (tsv @@ websearch_to_tsquery('english', %s) OR directory ILIKE %s OR file_name ILIKE %s)"
        values.extend([q, f"%{q}%", f"%{q}%"])

    count_sql = f"""
        SELECT count(DISTINCT lower(trim(parsed_title))) AS total
        FROM media WHERE media_type = 'series' AND parsed_title IS NOT NULL {search_clause}
    """
    total = query(count_sql, values or None)[0]["total"]
    total_pages = max(1, math.ceil(total / per_page))

    sql = f"""
        SELECT
            lower(trim(parsed_title)) AS norm_title,
            max(title) AS title,
            max(poster_path) AS poster_path,
            max(vote_average) AS vote_average,
            min(year) AS year,
            count(*) AS episode_count,
            count(DISTINCT season) AS season_count,
            max(directory) AS directory,
            min(id) AS id
        FROM media
        WHERE media_type = 'series' AND parsed_title IS NOT NULL {search_clause}
        GROUP BY lower(trim(parsed_title))
        ORDER BY max(title) ASC
        LIMIT {per_page} OFFSET {offset}
    """
    results = query(sql, values or None)

    return templates.TemplateResponse(request, "series.html", {
        "results": results,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "q": q,
        "tmdb_img": TMDB_IMG_BASE,
    })


async def api_open_vlc(request):
    """Open a media file in VLC. Delegates to host opener when running in Docker."""
    import shutil
    import subprocess
    entry_id = request.path_params["id"]
    rows = query("SELECT file_path FROM media WHERE id = %s", (entry_id,))
    if not rows:
        return JSONResponse({"error": "not found"}, status_code=404)
    file_path = rows[0]["file_path"]
    if not shutil.which("open"):
        result = _host_open("open-vlc", file_path)
        if result and result.get("ok"):
            return JSONResponse({"ok": True, "file_path": file_path})
        return JSONResponse({"error": (result or {}).get("error", "no_open_cmd"), "file_path": file_path}, status_code=501)
    ensure_mounted(file_path)
    try:
        result = subprocess.run(
            ["open", "-n", "-a", "VLC", file_path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return JSONResponse({"error": result.stderr.strip() or "Failed to open VLC", "file_path": file_path}, status_code=500)
        return JSONResponse({"ok": True, "file_path": file_path})
    except Exception as e:
        return JSONResponse({"error": str(e), "file_path": file_path}, status_code=500)


async def api_reveal(request):
    """Reveal a media file in Finder. Delegates to host opener when running in Docker."""
    import shutil
    import subprocess
    entry_id = request.path_params["id"]
    rows = query("SELECT file_path FROM media WHERE id = %s", (entry_id,))
    if not rows:
        return JSONResponse({"error": "not found"}, status_code=404)
    file_path = rows[0]["file_path"]
    if not shutil.which("open"):
        result = _host_open("reveal", file_path)
        if result and result.get("ok"):
            return JSONResponse({"ok": True, "file_path": file_path})
        return JSONResponse({"error": (result or {}).get("error", "no_open_cmd"), "file_path": file_path}, status_code=501)
    ensure_mounted(file_path)
    try:
        result = subprocess.run(
            ["open", "-R", file_path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return JSONResponse({"error": result.stderr.strip() or "File not found", "file_path": file_path}, status_code=500)
        return JSONResponse({"ok": True, "file_path": file_path})
    except Exception as e:
        return JSONResponse({"error": str(e), "file_path": file_path}, status_code=500)


async def api_tags(request):
    """Add or remove tags from an entry."""
    entry_id = request.path_params["id"]
    body = await request.json()
    action = body.get("action", "add")
    tag = body.get("tag", "").strip()

    if not tag:
        return JSONResponse({"error": "tag is required"}, status_code=400)

    if action == "add":
        execute_sql = "UPDATE media SET tags = array_append(tags, %s), updated_at = NOW() WHERE id = %s AND NOT (%s = ANY(tags))"
    elif action == "remove":
        execute_sql = "UPDATE media SET tags = array_remove(tags, %s), updated_at = NOW() WHERE id = %s"
    else:
        return JSONResponse({"error": "action must be add or remove"}, status_code=400)

    from db import execute
    execute(execute_sql, (tag, entry_id, tag) if action == "add" else (tag, entry_id))
    return JSONResponse({"ok": True})


app = Starlette(
    debug=True,
    routes=[
        Route("/", homepage),
        Route("/browse", browse),
        Route("/detail/{id:int}", detail),
        Route("/series", series),
        Route("/duplicates", duplicates),
        Route("/api/browse", api_browse),
        Route("/api/search", api_search),
        Route("/api/stats", api_stats),
        Route("/api/open/{id:int}", api_open_vlc, methods=["POST"]),
        Route("/api/reveal/{id:int}", api_reveal, methods=["POST"]),
        Route("/api/tags/{id:int}", api_tags, methods=["POST"]),
        Mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static"),
    ],
)
