"""
Phase 3: Web UI for browsing, searching, and managing the catalog.

Run standalone:
    python -m uvicorn web.app:app --port 8080 --reload

Or via docker-compose:
    docker compose up web
"""
import os
import re
import sys
import math
import json
import base64
import contextlib
import shlex
import signal
import asyncio
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import urlencode

from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.responses import JSONResponse, HTMLResponse, PlainTextResponse, RedirectResponse
from starlette.staticfiles import StaticFiles

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import query, execute
from web.common import (templates, PER_PAGE, HOST_OPENER_URL, PATH_MAPPINGS, map_path, ensure_mounted,
                        host_open as _host_open, parse_network_mounts as _parse_network_mounts,
                        positive_page as _positive_page, pick_sort)
from web import books as books_web


# ---------------------------------------------------------------------------
# Favourites + tags schema (additive, idempotent). media.id is stable across
# rescans (scanner upserts on the unique file_path key and never deletes), so
# both tables key on media.id with ON DELETE CASCADE.
# ---------------------------------------------------------------------------

FAVOURITES_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tags (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS tags_name_lower_idx ON tags (lower(name));
CREATE TABLE IF NOT EXISTS media_tags (
    media_id INT NOT NULL REFERENCES media(id) ON DELETE CASCADE,
    tag_id   INT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (media_id, tag_id)
);
CREATE TABLE IF NOT EXISTS favourites (
    media_id   INT PRIMARY KEY REFERENCES media(id) ON DELETE CASCADE,
    starred_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def ensure_favourites_schema():
    try:
        execute(FAVOURITES_SCHEMA_SQL)
    except Exception as e:
        import logging
        logging.warning("favourites schema migration failed: %s", e)


ensure_favourites_schema()

# Reusable SQL fragment: is this media row starred?
STARRED_SQL = "EXISTS (SELECT 1 FROM favourites f WHERE f.media_id = media.id) AS starred"

TMDB_IMG_BASE = "https://image.tmdb.org/t/p"
# Path mapping (map_path, PATH_MAPPINGS) lives in web/common.py, shared with the Books section.
LOCAL_VLC_SCHEME = "media-catalog-vlc"
WINDOWS_VLC_INSTALLER = Path(__file__).with_name("windows_vlc_protocol.ps1")
MACOS_VLC_INSTALLER = Path(__file__).with_name("macos_vlc_protocol.sh")


def local_vlc_uri(local_path: str) -> str | None:
    """Build a browser URI for the client-local VLC protocol handler.

    The payload is base64url rather than a nested URL so browsers cannot alter
    the colon or escaping in the mapped smb:// URI during protocol dispatch.
    """
    network_uri = map_path(local_path).get("smb")
    if not network_uri:
        return None
    payload = base64.urlsafe_b64encode(network_uri.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{LOCAL_VLC_SCHEME}://play/{payload}"


def _allowed_smb_hosts() -> list[str]:
    """SMB hosts the generated client handlers are allowed to launch."""
    from urllib.parse import urlsplit

    hosts = set()
    for targets in PATH_MAPPINGS.values():
        network_uri = targets.get("smb", "")
        hostname = urlsplit(network_uri).hostname
        if hostname:
            hosts.add(hostname.lower())
    return sorted(hosts)


async def api_path_mappings(request):
    """Expose the local-prefix → smb/UNC mappings for client-side use."""
    return JSONResponse({
        "mappings": PATH_MAPPINGS,
        "source": "env" if os.getenv("PATH_MAPPINGS", "").strip() else "default",
    })


async def api_paths(request):
    """All path variants (mac / smb / unc) for one entry's file and directory."""
    entry_id = request.path_params["id"]
    rows = query("SELECT file_path, directory FROM media WHERE id = %s", (entry_id,))
    if not rows:
        return JSONResponse({"error": "not found"}, status_code=404)
    fp = rows[0]["file_path"]
    d = rows[0]["directory"] or (fp.rsplit("/", 1)[0] if "/" in fp else fp)
    return JSONResponse({
        "id": entry_id,
        "file_path": fp,
        "directory": d,
        "variants": map_path(fp),
        "dir_variants": map_path(d),
    })


async def api_open_local_vlc(request):
    """Hand an SMB URI to VLC on the computer running the browser.

    Windows or macOS dispatches the custom URI to the per-user protocol handler
    from /local-vlc. The existing POST /api/open route remains the server-Mac
    action and never runs for this request.
    """
    entry_id = request.path_params["id"]
    rows = query("SELECT file_path FROM media WHERE id = %s", (entry_id,))
    if not rows:
        return JSONResponse({"error": "not found"}, status_code=404)
    launch_uri = local_vlc_uri(rows[0]["file_path"])
    if not launch_uri:
        return JSONResponse({
            "error": "no_smb_mapping",
            "message": "This file path has no configured smb:// mapping.",
        }, status_code=422)
    return RedirectResponse(launch_uri, status_code=302)


async def local_vlc_setup(request):
    """One-time setup instructions for browser-to-VLC handoff."""
    return templates.TemplateResponse(request, "local_vlc.html", {
        "allowed_hosts": _allowed_smb_hosts(),
    })


async def windows_vlc_installer(request):
    """Download a per-user protocol installer bound to configured SMB hosts."""
    allowed_hosts = _allowed_smb_hosts()
    powershell_hosts = "@(" + ", ".join("'" + host.replace("'", "''") + "'" for host in allowed_hosts) + ")"
    script = WINDOWS_VLC_INSTALLER.read_text(encoding="utf-8").replace(
        "__ALLOWED_SMB_HOSTS__", powershell_hosts,
    )
    return PlainTextResponse(
        script,
        media_type="text/plain",
        headers={
            "Content-Disposition": 'attachment; filename="install-media-catalog-vlc.ps1"',
            "Cache-Control": "no-store",
        },
    )


async def macos_vlc_installer(request):
    """Download a macOS protocol installer bound to configured SMB hosts."""
    allowed_hosts = _allowed_smb_hosts()
    shell_hosts = "(" + " ".join("'" + host.replace("'", "'\\''") + "'" for host in allowed_hosts) + ")"
    script = MACOS_VLC_INSTALLER.read_text(encoding="utf-8").replace(
        "__ALLOWED_SMB_HOSTS__", shell_hosts,
    )
    return PlainTextResponse(
        script,
        media_type="text/plain",
        headers={
            "Content-Disposition": 'attachment; filename="install-media-catalog-vlc-macos.sh"',
            "Cache-Control": "no-store",
        },
    )


_SERVER_IPS: set | None = None


def _server_ips() -> set:
    """All IPs that identify this server machine (cached per process)."""
    global _SERVER_IPS
    if _SERVER_IPS is not None:
        return _SERVER_IPS
    import socket
    ips = {"127.0.0.1", "::1"}
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ips.add(str(info[4][0]))
    except OSError:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("192.0.2.1", 9))  # RFC 5737 TEST-NET — no packet is sent
            ips.add(s.getsockname()[0])
        finally:
            s.close()
    except OSError:
        pass
    _SERVER_IPS = ips
    return ips


async def api_whoami(request):
    """Tell the client whether it is browsing from the server machine itself.

    Client IP == loopback or one of the server's own interface IPs means the
    browser runs on the same Mac that can execute VLC / Finder actions."""
    import socket
    client_ip = request.client.host if request.client else ""
    if client_ip.startswith("::ffff:"):
        client_ip = client_ip[len("::ffff:"):]
    return JSONResponse({
        "server_hostname": socket.gethostname(),
        "client_ip": client_ip,
        "is_server_host": client_ip in _server_ips(),
    })


def normalize_vlc_window() -> None:
    """Best-effort recovery for macOS remembering an off-screen VLC window."""
    import subprocess
    script = """
tell application "VLC"
    activate
    reopen
end tell
delay 0.5
tell application "System Events"
    if not (exists process "VLC") then return
    tell process "VLC"
        set frontmost to true
        repeat 10 times
            if (count of windows) > 0 then exit repeat
            delay 0.2
        end repeat
        set windowCount to count of windows
        repeat with i from 1 to windowCount
            try
                set w to window i
                try
                    set value of attribute "AXMinimized" of w to false
                end try
                set position of w to {120, 120}
                set size of w to {1120, 630}
            end try
        end repeat
    end tell
end tell
"""
    try:
        subprocess.run(
            ["osascript"],
            input=script,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:
        pass


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


CARD_SORT_OPTIONS = {
    "title": "lower(title) ASC NULLS LAST, card_key ASC",
    "year": "year DESC NULLS LAST, lower(title) ASC, card_key ASC",
    "rating": "vote_average DESC NULLS LAST, lower(title) ASC, card_key ASC",
    "size": "file_size_bytes DESC NULLS LAST, lower(title) ASC, card_key ASC",
    "recent": "recent_at DESC NULLS LAST, lower(title) ASC, card_key ASC",
    "popularity": "popularity DESC NULLS LAST, lower(title) ASC, card_key ASC",
}


def _card_sort(sort_key, default: str) -> tuple[str, str]:
    return pick_sort(sort_key, default, CARD_SORT_OPTIONS)


def _series_card_select(keys_cte: str) -> str:
    """Return the one authoritative series-card projection.

    Both Browse and the dedicated Series browser select through this projection,
    so title normalization, drill-down identity, counts, and sort values cannot
    diverge between the two surfaces.
    """
    return f"""
        SELECT
            min(series_media.id) AS id,
            max(series_media.title) AS title,
            min(series_media.year) AS year,
            'series'::media_type AS media_type,
            max(series_media.poster_path) AS poster_path,
            max(series_media.vote_average) AS vote_average,
            string_to_array(
                max(array_to_string(series_media.genres, E'\\x1f')),
                E'\\x1f'
            )::text[] AS genres,
            NULL::text AS resolution,
            sum(series_media.file_size_bytes)::bigint AS file_size_bytes,
            max(series_media.director) AS director,
            max(series_media.runtime_minutes) AS runtime_minutes,
            max(series_media.directory) AS directory,
            NULL::integer AS season,
            NULL::integer AS episode,
            bool_or(EXISTS (
                SELECT 1 FROM favourites favourite
                WHERE favourite.media_id = series_media.id
            )) AS starred,
            series_key.norm_title,
            count(*) AS episode_count,
            count(DISTINCT series_media.season) AS season_count,
            true AS is_series_card,
            max(series_media.created_at) AS recent_at,
            max(series_media.popularity) AS popularity,
            'series:' || series_key.norm_title AS card_key
        FROM media series_media
        JOIN {keys_cte} series_key
          ON lower(trim(series_media.parsed_title)) = series_key.norm_title
        WHERE series_media.media_type = 'series'
        GROUP BY series_key.norm_title
    """


def _browse_card_ctes(where: str) -> str:
    """Build mixed Browse cards: individual non-series plus grouped series."""
    return f"""
        WITH filtered_media AS (
            SELECT media.*
            FROM media
            WHERE {where}
        ),
        series_keys AS (
            SELECT DISTINCT lower(trim(parsed_title)) AS norm_title
            FROM filtered_media
            WHERE media_type = 'series'
              AND NULLIF(trim(parsed_title), '') IS NOT NULL
        ),
        series_cards AS (
            {_series_card_select('series_keys')}
        ),
        individual_cards AS (
            SELECT
                media.id,
                media.title,
                media.year,
                media.media_type,
                media.poster_path,
                media.vote_average,
                media.genres,
                media.resolution,
                media.file_size_bytes,
                media.director,
                media.runtime_minutes,
                media.directory,
                media.season,
                media.episode,
                EXISTS (
                    SELECT 1 FROM favourites favourite
                    WHERE favourite.media_id = media.id
                ) AS starred,
                NULL::text AS norm_title,
                NULL::bigint AS episode_count,
                NULL::bigint AS season_count,
                false AS is_series_card,
                media.created_at AS recent_at,
                media.popularity,
                'media:' || media.id::text AS card_key
            FROM filtered_media media
            WHERE media.media_type <> 'series'
               OR NULLIF(trim(media.parsed_title), '') IS NULL
        ),
        browse_cards AS (
            SELECT * FROM individual_cards
            UNION ALL
            SELECT * FROM series_cards
        )
    """


def _browse_card_page(params, per_page: int = PER_PAGE, include_total: bool = True):
    """Query a page of display-level Browse cards.

    A series contributes one card regardless of its episode count. The card's
    normalized title is the same identity consumed by /series?show=... .
    """
    where, values = _build_where(params)
    page = _positive_page(params.get("page", 1))
    offset = (page - 1) * per_page
    sort_key, sort_sql = _card_sort(params.get("sort"), "recent")
    ctes = _browse_card_ctes(where)

    total = None
    total_pages = None
    if include_total:
        total = query(f"{ctes} SELECT count(*) AS total FROM browse_cards", values)[0]["total"]
        total_pages = max(1, math.ceil(total / per_page))

    results = query(f"""
        {ctes}
        SELECT id, title, year, media_type, poster_path, vote_average,
               genres, resolution, file_size_bytes, director, runtime_minutes,
               directory, season, episode, starred, norm_title, episode_count,
               season_count, is_series_card, recent_at, popularity
        FROM browse_cards
        ORDER BY {sort_sql}
        LIMIT {per_page} OFFSET {offset}
    """, values)
    return results, total, page, total_pages, sort_key


def _series_card_page(q: str, sort_value, page: int, per_page: int = PER_PAGE):
    """Query the dedicated Series browser through the shared card projection."""
    search_clause = ""
    values = []
    if q:
        search_clause = """
            AND (
                tsv @@ websearch_to_tsquery('english', %s)
                OR directory ILIKE %s
                OR file_name ILIKE %s
            )
        """
        values.extend([q, f"%{q}%", f"%{q}%"])

    sort_key, sort_sql = _card_sort(sort_value, "title")
    offset = (page - 1) * per_page
    ctes = f"""
        WITH series_keys AS (
            SELECT DISTINCT lower(trim(parsed_title)) AS norm_title
            FROM media
            WHERE media_type = 'series'
              AND NULLIF(trim(parsed_title), '') IS NOT NULL
              {search_clause}
        ),
        series_cards AS (
            {_series_card_select('series_keys')}
        )
    """
    total = query(f"{ctes} SELECT count(*) AS total FROM series_keys", values or None)[0]["total"]
    total_pages = max(1, math.ceil(total / per_page))
    results = query(f"""
        {ctes}
        SELECT id, title, year, media_type, poster_path, vote_average,
               directory, norm_title, episode_count, season_count,
               file_size_bytes, recent_at, popularity
        FROM series_cards
        ORDER BY {sort_sql}
        LIMIT {per_page} OFFSET {offset}
    """, values or None)
    return results, total, total_pages, sort_key


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

    # Recent content: an episode batch contributes one series card, not one card
    # per file, using the same projection as Browse and the Series browser.
    recent, _, _, _, _ = _browse_card_page(
        {"sort": "recent"}, per_page=12, include_total=False,
    )

    return templates.TemplateResponse(request, "index.html", {
        "stats": s,
        "genres": genres,
        "years": years,
        "recent": recent,
        "tmdb_img": TMDB_IMG_BASE,
    })


async def browse(request):
    params = dict(request.query_params)
    results, total, page, total_pages, sort_key = _browse_card_page(params)
    params["sort"] = sort_key

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
        "filter_query": urlencode({
            key: value for key, value in params.items()
            if key != "page" and value != ""
        }),
        "genres": all_genres,
        "tmdb_img": TMDB_IMG_BASE,
    })


async def detail(request):
    entry_id = request.path_params["id"]
    rows = query(f"SELECT *, {STARRED_SQL} FROM media WHERE id = %s", (entry_id,))
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
    results, total, page, total_pages, sort_key = _browse_card_page(params)

    # Ensure JSON-safe types (datetimes, Decimals, etc.)
    safe = json.loads(json.dumps(results, default=str))

    return JSONResponse({
        "results": safe,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "per_page": PER_PAGE,
        "sort": sort_key,
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
    page = _positive_page(params.get("page", 1))
    per_page = 48

    # Level 3: episode list for a show + season
    if show and season_num:
        try:
            sn = int(season_num)
        except ValueError:
            sn = None
        episodes = query(f"""
            SELECT id, title, parsed_title, season, episode, resolution, codec,
                   file_size_bytes, file_path, directory, poster_path, vote_average,
                   {STARRED_SQL}
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

    # Level 1: one card per show, with the same identity and aggregate sort
    # values used when a series appears in mixed Browse results.
    results, total, total_pages, sort_key = _series_card_page(
        q, params.get("sort"), page, per_page,
    )

    return templates.TemplateResponse(request, "series.html", {
        "results": results,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "q": q,
        "sort_key": sort_key,
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
        normalize_vlc_window()
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


def _host_get(path: str) -> tuple[int, dict]:
    """GET the host opener. Returns (status_code, json-body)."""
    if not HOST_OPENER_URL:
        return 501, {"error": "HOST_OPENER_URL not configured"}
    import urllib.request
    import urllib.error
    try:
        with urllib.request.urlopen(f"{HOST_OPENER_URL}{path}", timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read())
        except Exception: return e.code, {"error": f"host opener HTTP {e.code}"}
    except Exception as e:
        return 502, {"error": f"host_opener.py not reachable: {e}"}


def _host_post(path: str, body: dict) -> tuple[int, dict]:
    """POST to the host opener. Returns (status_code, json-body)."""
    if not HOST_OPENER_URL:
        return 501, {"error": "HOST_OPENER_URL not configured"}
    import urllib.request
    import urllib.error
    try:
        req = urllib.request.Request(
            f"{HOST_OPENER_URL}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=35) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read())
        except Exception: return e.code, {"error": f"host opener HTTP {e.code}"}
    except Exception as e:
        return 502, {"error": f"host_opener.py not reachable: {e}"}


# ---------------------------------------------------------------------------
# In-app refresh: scan media folders → enrich from OMDb (background pipeline).
# Mirrors start.py's boot pipeline: filter MEDIA_DIRS to existing dirs, chain
# scan && touch .last_scan, then enrich — so start.py's staleness check stays
# coherent with UI-triggered refreshes.
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent.parent
SCAN_TS = ROOT_DIR / ".last_scan"      # same freshness marker start.py uses
REFRESH_LOG = ROOT_DIR / ".refresh.log"

_refresh_lock = threading.Lock()
_refresh_state = {
    "running": False,
    "stage": None,             # 'scan' | 'enrich' | None
    "started_at": None,        # epoch seconds
    "last_completed_at": None, # epoch seconds
    "last_result": None,       # {"ok": bool, "summary": str}
    "skipped_dirs": [],
    "warnings": [],
    "pid": None,
    "exit_code": None,         # exit code of the last finished stage
}
STALE_THRESHOLD_SEC = 180


def _refresh_set(**kv):
    with _refresh_lock:
        _refresh_state.update(kv)


def _run_stage(cmd, log_file) -> int:
    """Run one pipeline stage in its own process group, logging to log_file."""
    proc = subprocess.Popen(
        cmd, stdout=log_file, stderr=subprocess.STDOUT,
        cwd=ROOT_DIR, start_new_session=True,
    )
    _refresh_set(pid=proc.pid)
    return proc.wait()


def _refresh_worker(existing_dirs: list[str], do_enrich: bool, warnings: list[str]):
    summary = []
    ok = True
    try:
        with open(REFRESH_LOG, "w") as log:
            if existing_dirs:
                _refresh_set(stage="scan")
                # Chain the .last_scan touch into the subprocess (like start.py)
                # so scan freshness is recorded even if the web worker restarts.
                scan_cmd = (
                    shlex.quote(sys.executable) + " -u -m scanner.scan "
                    + " ".join(shlex.quote(d) for d in existing_dirs)
                    + " && touch " + shlex.quote(str(SCAN_TS))
                )
                rc = _run_stage(["bash", "-c", scan_cmd], log)
                _refresh_set(exit_code=rc)
                if rc == 0:
                    summary.append(f"scan ok ({len(existing_dirs)} dir(s))")
                else:
                    ok = False
                    summary.append(f"scan failed (exit {rc})")
            if do_enrich and ok:
                _refresh_set(stage="enrich")
                rc = _run_stage([sys.executable, "-u", "-m", "enricher.omdb"], log)
                _refresh_set(exit_code=rc)
                if rc == 0:
                    summary.append("enrich ok")
                else:
                    ok = False
                    summary.append(f"enrich failed (exit {rc})")
            elif do_enrich:
                summary.append("enrich skipped (scan failed)")
    except Exception as e:
        ok = False
        summary.append(f"refresh error: {e}")
    finally:
        _refresh_set(
            running=False, stage=None, pid=None,
            last_completed_at=time.time(),
            last_result={"ok": ok, "summary": "; ".join(summary + warnings) or "nothing ran"},
        )


def _kill_refresh_group() -> int | None:
    """SIGTERM the running stage's process group. Returns the pid, or None."""
    with _refresh_lock:
        pid = _refresh_state["pid"] if _refresh_state["running"] else None
    if pid is not None:
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    return pid


async def api_refresh_start(request):
    """Kick off a local folder scan → OMDb enrich as a background job.
    Accepts scan/enrich booleans and {force: true} to kill a running
    (e.g. stuck) pipeline first."""
    try:
        body_in = await request.json()
    except Exception:
        body_in = {}
    requested_scan = bool(body_in.get("scan", True))
    requested_enrich = bool(body_in.get("enrich", True))
    if bool(body_in.get("force")) and _kill_refresh_group() is not None:
        # Give the worker thread a moment to reap the stage and clear state.
        for _ in range(25):
            with _refresh_lock:
                if not _refresh_state["running"]:
                    break
            await asyncio.sleep(0.2)

    with _refresh_lock:
        if _refresh_state["running"]:
            return JSONResponse(
                {
                    "error": "refresh already running",
                    "stage": _refresh_state["stage"],
                    "started_at": _refresh_state["started_at"],
                },
                status_code=409,
            )

        raw = os.getenv("MEDIA_DIRS", "") if requested_scan else ""
        dirs = [d.strip() for d in raw.split(":") if d.strip()]
        existing = [d for d in dirs if os.path.isdir(d)]
        missing = [d for d in dirs if not os.path.isdir(d)]

        warnings = []
        if requested_scan and not dirs:
            warnings.append("MEDIA_DIRS not set — scan skipped")
        elif missing:
            warnings.append(f"skipped unmounted dir(s): {', '.join(missing)}")

        key = os.getenv("OMDB_API_KEY", "")
        do_enrich = requested_enrich and bool(key) and key != "your_omdb_api_key_here"
        if requested_enrich and not do_enrich:
            warnings.append("OMDB_API_KEY not set — enrich skipped")

        if not existing and not do_enrich:
            return JSONResponse(
                {"error": "nothing to do: scan and enrichment are unavailable or disabled",
                 "skipped_dirs": missing},
                status_code=400,
            )

        _refresh_state.update(
            running=True,
            stage="scan" if existing else "enrich",
            started_at=time.time(),
            skipped_dirs=missing,
            warnings=warnings,
            pid=None,
            exit_code=None,
        )

    threading.Thread(
        target=_refresh_worker, args=(existing, do_enrich, warnings), daemon=True,
    ).start()
    return JSONResponse(
        {"ok": True, "started_at": _refresh_state["started_at"],
         "skipped_dirs": missing, "warnings": warnings},
        status_code=202,
    )


def _read_refresh_log_tail() -> str:
    if not REFRESH_LOG.exists():
        return ""
    try:
        data = REFRESH_LOG.read_bytes()
        if len(data) > 64 * 1024:
            data = data[-64 * 1024:]
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _parse_refresh_counts(log_text: str) -> dict:
    """Extract progress counts from the pipeline log (for the dashboard bar)."""
    counts: dict = {}
    m = re.search(r"Found ([\d,]+) video files", log_text)
    if m:
        counts["scan_found"] = int(m.group(1).replace(",", ""))
    m = re.search(r"([\d,]+) already cataloged, ([\d,]+) new", log_text)
    if m:
        counts["scan_already"] = int(m.group(1).replace(",", ""))
        counts["scan_new"] = int(m.group(2).replace(",", ""))
    m = re.search(r"([\d,]+) entries to enrich \(([\d,]+) unique", log_text)
    if m:
        counts["enrich_entries"] = int(m.group(1).replace(",", ""))
        counts["enrich_unique"] = int(m.group(2).replace(",", ""))
    ticks = re.findall(r"Enriching:\s*\d+%\|[^|]*\|\s*([\d,]+)/([\d,]+)", log_text)
    if ticks:
        counts["enrich_done"] = int(ticks[-1][0].replace(",", ""))
        counts["enrich_total"] = int(ticks[-1][1].replace(",", ""))
    return counts


async def api_refresh_status(request):
    """Poll refresh pipeline progress."""
    with _refresh_lock:
        s = dict(_refresh_state)

    # Legacy fields consumed by the dashboard refresh bar (index.html):
    # phase / exit_code / stale / heartbeat_age_sec / scan+enrich counts / tail.
    if s["running"]:
        phase = {"scan": "scanning", "enrich": "enriching"}.get(s["stage"], "starting")
    elif s["last_result"] is not None:
        phase = "done" if s["last_result"]["ok"] else "error"
    else:
        phase = None

    age = None
    if REFRESH_LOG.exists():
        try:
            age = time.time() - REFRESH_LOG.stat().st_mtime
        except Exception:
            pass
    log_text = _read_refresh_log_tail()
    tail = [l for l in log_text.splitlines() if l.strip()][-8:]

    return JSONResponse({
        "running": s["running"],
        "stage": s["stage"],
        "started_at": s["started_at"],
        "last_completed_at": s["last_completed_at"],
        "last_result": s["last_result"],
        "skipped_dirs": s["skipped_dirs"],
        "warnings": s["warnings"],
        # legacy / dashboard fields
        "phase": phase,
        "exit_code": s["exit_code"],
        "heartbeat_age_sec": age,
        "stale_threshold_sec": STALE_THRESHOLD_SEC,
        "stale": bool(s["running"] and age is not None and age > STALE_THRESHOLD_SEC),
        "tail": tail,
        **_parse_refresh_counts(log_text),
    })


async def api_refresh_cancel(request):
    """Cancel a running refresh pipeline (kills the stage's process group)."""
    pid = _kill_refresh_group()
    if pid is None:
        return JSONResponse({"ok": True, "was_running": False})
    return JSONResponse({"ok": True, "was_running": True, "pid": pid})


async def api_resync(request):
    """Re-enrich one entry (overwrites metadata)."""
    entry_id = request.path_params["id"]
    status, body = _host_post("/resync", {"entry_id": entry_id})
    return JSONResponse(body, status_code=status)


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


# ---------------------------------------------------------------------------
# Favourites + tags (normalized: tags / media_tags / favourites tables).
# Semantics: starring is independent of tags; applying a tag auto-stars the
# entry; unstarring removes it from favourites but KEEPS its tag rows, so a
# later re-star restores the previous labelling.
# ---------------------------------------------------------------------------

def _media_exists(media_id: int) -> bool:
    return bool(query("SELECT 1 FROM media WHERE id = %s", (media_id,)))


def _media_tag_state(media_id: int) -> dict:
    """Current star + tag membership for one entry (used as mutation response)."""
    starred = bool(query("SELECT 1 FROM favourites WHERE media_id = %s", (media_id,)))
    tags = query("""
        SELECT t.id, t.name FROM tags t
        JOIN media_tags mt ON mt.tag_id = t.id
        WHERE mt.media_id = %s ORDER BY lower(t.name)
    """, (media_id,))
    return {"media_id": media_id, "starred": starred, "tags": tags}


async def api_star(request):
    """POST = star an entry, DELETE = unstar. Idempotent both ways."""
    media_id = request.path_params["id"]
    if not _media_exists(media_id):
        return JSONResponse({"error": "media not found"}, status_code=404)
    if request.method == "POST":
        execute("INSERT INTO favourites (media_id) VALUES (%s) ON CONFLICT (media_id) DO NOTHING", (media_id,))
    else:
        execute("DELETE FROM favourites WHERE media_id = %s", (media_id,))
    return JSONResponse({"ok": True, **_media_tag_state(media_id)})


async def api_tags_collection(request):
    """GET: list all tags (with per-media membership when ?media_id= given).
    POST {name}: create a tag — case-insensitively unique, 409 on duplicate."""
    if request.method == "GET":
        media_id = request.query_params.get("media_id", "").strip()
        if media_id:
            try:
                mid = int(media_id)
            except ValueError:
                return JSONResponse({"error": "media_id must be an integer"}, status_code=400)
            rows = query("""
                SELECT t.id, t.name,
                       EXISTS (SELECT 1 FROM media_tags mt
                               WHERE mt.tag_id = t.id AND mt.media_id = %s) AS applied
                FROM tags t ORDER BY lower(t.name)
            """, (mid,))
            starred = bool(query("SELECT 1 FROM favourites WHERE media_id = %s", (mid,)))
            return JSONResponse({"tags": rows, "media_id": mid, "starred": starred})
        rows = query("""
            SELECT t.id, t.name, count(mt.media_id) AS media_count
            FROM tags t LEFT JOIN media_tags mt ON mt.tag_id = t.id
            GROUP BY t.id, t.name ORDER BY lower(t.name)
        """)
        return JSONResponse({"tags": rows})

    # POST — create
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)
    if not isinstance(body, dict) or not isinstance(body.get("name"), str):
        return JSONResponse({"error": "body must be {\"name\": \"...\"}"}, status_code=400)
    name = body["name"].strip()
    if not name:
        return JSONResponse({"error": "name must not be empty"}, status_code=400)
    if len(name) > 80:
        return JSONResponse({"error": "name too long (max 80 chars)"}, status_code=400)
    existing = query("SELECT id, name FROM tags WHERE lower(name) = lower(%s)", (name,))
    if existing:
        return JSONResponse({"error": "tag already exists", "tag": existing[0]}, status_code=409)
    try:
        rows = query("INSERT INTO tags (name) VALUES (%s) RETURNING id, name", (name,))
    except Exception:
        # Lost a race to a concurrent insert — report the winner as duplicate.
        existing = query("SELECT id, name FROM tags WHERE lower(name) = lower(%s)", (name,))
        if existing:
            return JSONResponse({"error": "tag already exists", "tag": existing[0]}, status_code=409)
        raise
    return JSONResponse({"ok": True, "tag": rows[0]}, status_code=201)


async def api_media_tag(request):
    """POST = apply tag to entry (auto-stars it), DELETE = remove tag."""
    media_id = request.path_params["id"]
    tag_id = request.path_params["tag_id"]
    if not _media_exists(media_id):
        return JSONResponse({"error": "media not found"}, status_code=404)
    if not query("SELECT 1 FROM tags WHERE id = %s", (tag_id,)):
        return JSONResponse({"error": "tag not found"}, status_code=404)
    if request.method == "POST":
        execute("INSERT INTO media_tags (media_id, tag_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (media_id, tag_id))
        # Tagging implies favouriting: auto-star.
        execute("INSERT INTO favourites (media_id) VALUES (%s) ON CONFLICT (media_id) DO NOTHING", (media_id,))
    else:
        execute("DELETE FROM media_tags WHERE media_id = %s AND tag_id = %s", (media_id, tag_id))
    return JSONResponse({"ok": True, **_media_tag_state(media_id)})


def _favourites_data(tag_filter: str):
    """Starred media (with their tags) + per-tag starred counts.

    tag_filter: '' or 'all' → everything; 'untagged' → starred with no tags;
    otherwise case-insensitive tag name."""
    where = ""
    values: list = []
    tf = (tag_filter or "").strip()
    if tf.lower() == "untagged":
        where = "AND NOT EXISTS (SELECT 1 FROM media_tags mt WHERE mt.media_id = m.id)"
    elif tf and tf.lower() != "all":
        where = """AND EXISTS (SELECT 1 FROM media_tags mt JOIN tags t ON t.id = mt.tag_id
                               WHERE mt.media_id = m.id AND lower(t.name) = lower(%s))"""
        values.append(tf)
    results = query(f"""
        SELECT m.id, m.title, m.year, m.media_type, m.poster_path, m.vote_average,
               m.resolution, m.directory, m.season, m.episode, f.starred_at,
               COALESCE(array_agg(t.name ORDER BY lower(t.name))
                        FILTER (WHERE t.name IS NOT NULL), '{{}}') AS fav_tags
        FROM favourites f
        JOIN media m ON m.id = f.media_id
        LEFT JOIN media_tags mt ON mt.media_id = m.id
        LEFT JOIN tags t ON t.id = mt.tag_id
        WHERE 1=1 {where}
        GROUP BY m.id, f.starred_at
        ORDER BY f.starred_at DESC
    """, values or None)
    tag_counts = query("""
        SELECT t.id, t.name, count(f.media_id) AS starred_count
        FROM tags t
        LEFT JOIN media_tags mt ON mt.tag_id = t.id
        LEFT JOIN favourites f ON f.media_id = mt.media_id
        GROUP BY t.id, t.name ORDER BY lower(t.name)
    """)
    untagged = query("""
        SELECT count(*) AS cnt FROM favourites f
        WHERE NOT EXISTS (SELECT 1 FROM media_tags mt WHERE mt.media_id = f.media_id)
    """)[0]["cnt"]
    total = query("SELECT count(*) AS cnt FROM favourites")[0]["cnt"]
    return results, tag_counts, untagged, total


async def api_favourites(request):
    """Starred media, filterable by ?tag=<name|all|untagged>, with tag counts."""
    tag_filter = request.query_params.get("tag", "all")
    results, tag_counts, untagged, total = _favourites_data(tag_filter)
    safe = json.loads(json.dumps(results, default=str))
    return JSONResponse({
        "tag": tag_filter or "all",
        "count": len(safe),
        "results": safe,
        "tags": tag_counts,
        "untagged_count": untagged,
        "total_starred": total,
    })


async def favourites_page(request):
    """Favourites section: starred media filtered/grouped by tag."""
    tag_filter = request.query_params.get("tag", "all").strip() or "all"
    results, tag_counts, untagged, total = _favourites_data(tag_filter)

    # Grouped view for "all": one section per tag (a movie with two tags
    # appears under both), plus an Untagged section.
    groups = []
    if tag_filter.lower() == "all":
        by_tag: dict = {}
        untagged_items = []
        for item in results:
            if item["fav_tags"]:
                for t in item["fav_tags"]:
                    by_tag.setdefault(t, []).append(item)
            else:
                untagged_items.append(item)
        for name in sorted(by_tag, key=str.lower):
            groups.append({"name": name, "items": by_tag[name]})
        if untagged_items:
            groups.append({"name": "Untagged", "items": untagged_items})
    else:
        label = "Untagged" if tag_filter.lower() == "untagged" else tag_filter
        groups = [{"name": label, "items": results}]

    return templates.TemplateResponse(request, "favourites.html", {
        "groups": groups,
        "tag_filter": tag_filter,
        "tag_counts": tag_counts,
        "untagged_count": untagged,
        "total_starred": total,
        "tmdb_img": TMDB_IMG_BASE,
    })


@contextlib.asynccontextmanager
async def lifespan(app):
    books_web.warm_cache()
    yield


app = Starlette(
    debug=True,
    lifespan=lifespan,
    routes=[
        Route("/", homepage),
        Route("/browse", browse),
        Route("/detail/{id:int}", detail),
        Route("/series", series),
        Route("/favourites", favourites_page),
        Route("/duplicates", duplicates),
        Route("/api/browse", api_browse),
        Route("/api/search", api_search),
        Route("/api/stats", api_stats),
        Route("/api/path-mappings", api_path_mappings),
        Route("/api/paths/{id:int}", api_paths),
        Route("/api/whoami", api_whoami),
        Route("/api/open-local-vlc/{id:int}", api_open_local_vlc),
        Route("/api/open/{id:int}", api_open_vlc, methods=["POST"]),
        Route("/api/reveal/{id:int}", api_reveal, methods=["POST"]),
        Route("/api/tags/{id:int}", api_tags, methods=["POST"]),
        Route("/api/tags", api_tags_collection, methods=["GET", "POST"]),
        Route("/api/favourites", api_favourites),
        Route("/api/media/{id:int}/star", api_star, methods=["POST", "DELETE"]),
        Route("/api/media/{id:int}/tags/{tag_id:int}", api_media_tag, methods=["POST", "DELETE"]),
        Route("/api/refresh", api_refresh_start, methods=["POST"]),
        Route("/api/refresh/status", api_refresh_status),
        Route("/api/refresh/cancel", api_refresh_cancel, methods=["POST"]),
        Route("/api/resync/{id:int}", api_resync, methods=["POST"]),
        Route("/local-vlc", local_vlc_setup),
        Route("/local-vlc/install-windows.ps1", windows_vlc_installer),
        Route("/local-vlc/install-macos.sh", macos_vlc_installer),
        *books_web.routes,
        Mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static"),
    ],
)
