"""
Phase 2: Enrich catalog entries with metadata from TMDb.

Usage:
    python -m enricher.tmdb                  # enrich all unenriched entries
    python -m enricher.tmdb --all            # re-enrich everything
    python -m enricher.tmdb --limit 100      # do 100 at a time
    python -m enricher.tmdb --id 42          # enrich a specific entry
"""
import os
import sys
import time
import httpx
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import query, execute

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"
RATE_LIMIT_DELAY = 0.26  # TMDb allows ~40 req/10s, this keeps us safe


def tmdb_get(path: str, params: dict = None) -> dict | None:
    """Make a TMDb API request."""
    params = params or {}
    params["api_key"] = TMDB_API_KEY
    try:
        r = httpx.get(f"{TMDB_BASE}{path}", params=params, timeout=15)
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 2))
            print(f"  Rate limited, waiting {retry}s...")
            time.sleep(retry)
            return tmdb_get(path, params)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  TMDb error: {e}")
        return None


def search_movie(title: str, year: int | None) -> dict | None:
    """Search TMDb for a movie."""
    params = {"query": title}
    if year:
        params["year"] = year
    data = tmdb_get("/search/movie", params)
    if data and data.get("results"):
        return data["results"][0]
    # Retry without year if no results
    if year:
        data = tmdb_get("/search/movie", {"query": title})
        if data and data.get("results"):
            return data["results"][0]
    return None


def search_series(title: str, year: int | None) -> dict | None:
    """Search TMDb for a TV series."""
    params = {"query": title}
    if year:
        params["first_air_date_year"] = year
    data = tmdb_get("/search/tv", params)
    if data and data.get("results"):
        return data["results"][0]
    if year:
        data = tmdb_get("/search/tv", {"query": title})
        if data and data.get("results"):
            return data["results"][0]
    return None


def get_movie_details(tmdb_id: int) -> dict | None:
    """Get full movie details including credits."""
    return tmdb_get(f"/movie/{tmdb_id}", {"append_to_response": "credits"})


def get_series_details(tmdb_id: int) -> dict | None:
    """Get full series details including credits."""
    return tmdb_get(f"/tv/{tmdb_id}", {"append_to_response": "credits"})


def extract_credits(details: dict, is_series: bool = False) -> tuple[list[str], str | None]:
    """Extract top cast names and director from credits."""
    credits = details.get("credits", {})
    cast = [c["name"] for c in credits.get("cast", [])[:10]]

    if is_series:
        director = None
        created_by = details.get("created_by", [])
        if created_by:
            director = created_by[0].get("name")
    else:
        directors = [c["name"] for c in credits.get("crew", []) if c.get("job") == "Director"]
        director = directors[0] if directors else None

    return cast, director


def enrich_entry(entry: dict) -> dict | None:
    """Enrich a single catalog entry with TMDb data."""
    title = entry["parsed_title"] or entry["title"]
    year = entry["year"]
    mtype = entry["media_type"]

    # Search
    if mtype == "series":
        result = search_series(title, year)
    elif mtype == "movie":
        result = search_movie(title, year)
    else:
        # Try movie first, then series
        result = search_movie(title, year)
        if not result:
            result = search_series(title, year)
            if result:
                mtype = "series"
        else:
            mtype = "movie"

    if not result:
        return None

    tmdb_id = result["id"]

    # Get full details
    if mtype == "series":
        details = get_series_details(tmdb_id)
    else:
        details = get_movie_details(tmdb_id)

    if not details:
        return None

    cast, director = extract_credits(details, is_series=(mtype == "series"))

    # Extract genre names
    genres = [g["name"] for g in details.get("genres", [])]

    # Build metadata
    metadata = {
        "tmdb_id": tmdb_id,
        "imdb_id": details.get("imdb_id") or details.get("external_ids", {}).get("imdb_id"),
        "title": details.get("title") or details.get("name") or title,
        "overview": details.get("overview"),
        "genres": genres,
        "vote_average": details.get("vote_average"),
        "vote_count": details.get("vote_count"),
        "poster_path": details.get("poster_path"),
        "backdrop_path": details.get("backdrop_path"),
        "release_date": details.get("release_date") or details.get("first_air_date"),
        "original_language": details.get("original_language"),
        "popularity": details.get("popularity"),
        "cast_names": cast,
        "director": director,
        "tagline": details.get("tagline"),
        "runtime_minutes": details.get("runtime") or (
            details.get("episode_run_time", [None])[0] if details.get("episode_run_time") else None
        ),
        "status": details.get("status"),
        "media_type": mtype,
        "year": year or _extract_year(details),
    }

    return metadata


def _extract_year(details: dict) -> int | None:
    date_str = details.get("release_date") or details.get("first_air_date")
    if date_str and len(date_str) >= 4:
        try:
            return int(date_str[:4])
        except ValueError:
            pass
    return None


def update_entry(entry_id: int, metadata: dict):
    """Write enriched metadata back to the database."""
    sql = """
        UPDATE media SET
            tmdb_id = %(tmdb_id)s,
            imdb_id = %(imdb_id)s,
            title = %(title)s,
            overview = %(overview)s,
            genres = %(genres)s,
            vote_average = %(vote_average)s,
            vote_count = %(vote_count)s,
            poster_path = %(poster_path)s,
            backdrop_path = %(backdrop_path)s,
            release_date = %(release_date)s,
            original_language = %(original_language)s,
            popularity = %(popularity)s,
            cast_names = %(cast_names)s,
            director = %(director)s,
            tagline = %(tagline)s,
            runtime_minutes = %(runtime_minutes)s,
            status = %(status)s,
            media_type = %(media_type)s,
            year = COALESCE(%(year)s, year),
            enriched_at = NOW(),
            updated_at = NOW()
        WHERE id = %(id)s
    """
    metadata["id"] = entry_id
    execute(sql, metadata)


def enrich(all_entries: bool = False, limit: int = 0, entry_id: int = None):
    """Main enrichment entry point."""
    if not TMDB_API_KEY or TMDB_API_KEY == "your_tmdb_api_key_here":
        print("Error: Set TMDB_API_KEY in your .env file")
        print("Get a free key at https://www.themoviedb.org/settings/api")
        sys.exit(1)

    print(f"\n🎬 TMDb Metadata Enricher\n")

    # Select entries to enrich
    if entry_id:
        entries = query("SELECT id, title, parsed_title, year, media_type FROM media WHERE id = %s", (entry_id,))
    elif all_entries:
        sql = "SELECT id, title, parsed_title, year, media_type FROM media"
        if limit:
            sql += f" LIMIT {limit}"
        entries = query(sql)
    else:
        sql = "SELECT id, title, parsed_title, year, media_type FROM media WHERE tmdb_id IS NULL"
        if limit:
            sql += f" LIMIT {limit}"
        entries = query(sql)

    # Deduplicate: only enrich one entry per title+year+type, then propagate
    seen = {}
    unique_entries = []
    for e in entries:
        key = f"{(e['parsed_title'] or e['title']).lower().strip()}|{e['year']}|{e['media_type']}"
        if key not in seen:
            seen[key] = [e]
            unique_entries.append(e)
        else:
            seen[key].append(e)

    print(f"   {len(entries)} entries to enrich ({len(unique_entries)} unique titles)\n")

    if not unique_entries:
        print("   Nothing to enrich!")
        return

    enriched = 0
    failed = 0
    for entry in tqdm(unique_entries, desc="   Enriching"):
        metadata = enrich_entry(entry)
        if metadata:
            # Update this entry and all duplicates with same key
            key = f"{(entry['parsed_title'] or entry['title']).lower().strip()}|{entry['year']}|{entry['media_type']}"
            for e in seen[key]:
                update_entry(e["id"], metadata)
            enriched += len(seen[key])
        else:
            failed += 1

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\n   ✅ Enriched {enriched:,} entries")
    if failed:
        print(f"   ⚠ {failed} titles not found on TMDb")

    stats = query("SELECT * FROM catalog_stats")
    if stats:
        s = stats[0]
        print(f"\n   Catalog: {s['enriched']}/{s['total_entries']} enriched\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Enrich catalog with TMDb metadata")
    parser.add_argument("--all", action="store_true", help="Re-enrich all entries")
    parser.add_argument("--limit", type=int, default=0, help="Max entries to enrich")
    parser.add_argument("--id", type=int, default=None, help="Enrich specific entry ID")
    args = parser.parse_args()

    enrich(all_entries=args.all, limit=args.limit, entry_id=args.id)
