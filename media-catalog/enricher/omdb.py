"""
Phase 2 (alternative): Enrich catalog entries with metadata from OMDb.

OMDb is free (1000 req/day), no credit card needed.
Get a key at: http://www.omdbapi.com/apikey.aspx

Usage:
    python -m enricher.omdb                  # enrich all unenriched entries
    python -m enricher.omdb --all            # re-enrich everything
    python -m enricher.omdb --limit 100      # do 100 at a time
    python -m enricher.omdb --id 42          # enrich a specific entry
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

OMDB_API_KEY = os.getenv("OMDB_API_KEY", "")
OMDB_BASE = "https://www.omdbapi.com/"
RATE_LIMIT_DELAY = 0.1  # OMDb free tier: 1000/day, ~1 req/sec is fine


def omdb_get(params: dict) -> dict | None:
    params["apikey"] = OMDB_API_KEY
    try:
        r = httpx.get(OMDB_BASE, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("Response") == "False":
            return None
        return data
    except Exception as e:
        print(f"  OMDb error: {e}")
        return None


def search(title: str, year: int | None, media_type: str) -> dict | None:
    """Fetch OMDb record by title search. Falls back without year if no match."""
    omdb_type = {"movie": "movie", "series": "series"}.get(media_type, "")
    params = {"t": title, "plot": "full"}
    if omdb_type:
        params["type"] = omdb_type
    if year:
        params["y"] = year

    data = omdb_get(params)
    if data:
        return data

    # Retry without year
    if year:
        params.pop("y", None)
        data = omdb_get(params)
        if data:
            return data

    # Retry without type constraint
    if omdb_type:
        params.pop("type", None)
        data = omdb_get(params)

    return data


def filename_title_candidates(file_name: str) -> list[str]:
    """Extract candidate titles from a messy filename.

    Handles patterns like:
      'Peter Greenaway - Vertical Features Remake 1976.avi'  → ['Vertical Features Remake', 'Peter Greenaway']
      '003 S01E02 Soul Hunter.mkv'                           → ['Soul Hunter']
    """
    import re
    stem = re.sub(r'\.[a-z0-9]{2,4}$', '', file_name, flags=re.IGNORECASE)
    # Strip year, resolution, codec, release tags
    stem = re.sub(r'\b(19|20)\d{2}\b.*', '', stem).strip()
    stem = re.sub(r'\b(720p|1080p|2160p|bluray|web-?dl|hdtv|xvid|x264|x265|hevc|aac|mp3|dvdrip)\b.*', '', stem, flags=re.IGNORECASE).strip()
    # Strip S##E## and anything before it (episode number prefix)
    no_ep = re.sub(r'^\d+\s+', '', stem)          # leading digits like "003 "
    no_ep = re.sub(r'\bS\d+E\d+\b.*', '', no_ep, flags=re.IGNORECASE).strip(' -_.')

    candidates = []
    # If there's a dash separator, try the part after it first
    if ' - ' in stem:
        parts = [p.strip() for p in stem.split(' - ', 1)]
        candidates.append(parts[1])   # after dash = likely title
        candidates.append(parts[0])   # before dash = may be director/series
    elif no_ep and no_ep != stem:
        candidates.append(no_ep)

    candidates.append(stem.strip(' -_.'))
    # Deduplicate preserving order
    seen = set()
    result = []
    for c in candidates:
        c = c.strip()
        if c and c not in seen:
            seen.add(c)
            result.append(c)
    return result


def parse_runtime(runtime_str: str | None) -> int | None:
    """Convert '142 min' → 142."""
    if not runtime_str or runtime_str == "N/A":
        return None
    try:
        return int(runtime_str.split()[0])
    except (ValueError, IndexError):
        return None


def parse_rating(rating_str: str | None) -> float | None:
    """Convert '7.5/10' or '7.5' → 7.5."""
    if not rating_str or rating_str == "N/A":
        return None
    try:
        return float(rating_str.split("/")[0].replace(",", ""))
    except (ValueError, IndexError):
        return None


def parse_votes(votes_str: str | None) -> int | None:
    """Convert '1,234,567' → 1234567."""
    if not votes_str or votes_str == "N/A":
        return None
    try:
        return int(votes_str.replace(",", ""))
    except ValueError:
        return None


def parse_year(year_str: str | None) -> int | None:
    if not year_str or year_str == "N/A":
        return None
    try:
        return int(str(year_str)[:4])
    except (ValueError, TypeError):
        return None


def enrich_entry(entry: dict) -> dict | None:
    title = entry["parsed_title"] or entry["title"]
    year = entry["year"]
    mtype = entry["media_type"]

    def _search(t, y, mt):
        if mt == "unknown":
            d = search(t, y, "movie")
            if not d:
                d = search(t, y, "series")
            return d
        return search(t, y, mt)

    data = _search(title, year, mtype)

    # If not found, try candidate titles extracted from the raw filename
    if not data and entry.get("file_name"):
        for candidate in filename_title_candidates(entry["file_name"]):
            if candidate.lower() == title.lower():
                continue  # already tried
            data = _search(candidate, year, mtype)
            if data:
                break

    if not data:
        return None

    # Update title with what OMDb matched if we used a filename candidate
    matched_title = data.get("Title") or title

    # OMDb genres are comma-separated strings
    genres_raw = data.get("Genre", "")
    genres = [g.strip() for g in genres_raw.split(",") if g.strip() and g.strip() != "N/A"]

    # Cast: "Actor1, Actor2, Actor3"
    actors_raw = data.get("Actors", "")
    cast = [a.strip() for a in actors_raw.split(",") if a.strip() and a.strip() != "N/A"]

    director = data.get("Director", "").strip() or None
    if director == "N/A":
        director = None

    poster = data.get("Poster", "").strip() or None
    if poster == "N/A":
        poster = None

    imdb_id = data.get("imdbID", "").strip() or None
    if imdb_id == "N/A":
        imdb_id = None

    tagline = data.get("Awards", "").strip() or None
    if tagline == "N/A":
        tagline = None

    release_date = data.get("Released", "").strip() or None
    if release_date == "N/A":
        release_date = None

    language = data.get("Language", "").strip() or None
    if language == "N/A":
        language = None
    # Store just the first language code-ish
    if language:
        language = language.split(",")[0].strip()[:10]

    status = data.get("Type", "").strip() or None

    enriched_year = parse_year(data.get("Year")) or year

    return {
        "tmdb_id": None,          # Not a TMDb entry
        "imdb_id": imdb_id,
        "title": matched_title,
        "overview": data.get("Plot") if data.get("Plot") != "N/A" else None,
        "genres": genres,
        "vote_average": parse_rating(data.get("imdbRating")),
        "vote_count": parse_votes(data.get("imdbVotes")),
        "poster_path": poster,    # Full URL from OMDb, not a TMDb path
        "backdrop_path": None,
        "release_date": release_date,
        "original_language": language,
        "popularity": None,
        "cast_names": cast,
        "director": director,
        "tagline": tagline,
        "runtime_minutes": parse_runtime(data.get("Runtime")),
        "status": status,
        "media_type": mtype,
        "year": enriched_year,
    }


def update_entry(entry_id: int, metadata: dict):
    sql = """
        UPDATE media SET
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
    if not OMDB_API_KEY or OMDB_API_KEY == "your_omdb_api_key_here":
        print("Error: Set OMDB_API_KEY in your .env file")
        print("Get a free key at: http://www.omdbapi.com/apikey.aspx")
        sys.exit(1)

    print(f"\n🎬 OMDb Metadata Enricher\n")

    if entry_id:
        entries = query("SELECT id, title, parsed_title, year, media_type, file_name FROM media WHERE id = %s", (entry_id,))
    elif all_entries:
        sql = "SELECT id, title, parsed_title, year, media_type, file_name FROM media"
        if limit:
            sql += f" LIMIT {limit}"
        entries = query(sql)
    else:
        sql = "SELECT id, title, parsed_title, year, media_type, file_name FROM media WHERE enriched_at IS NULL"
        if limit:
            sql += f" LIMIT {limit}"
        entries = query(sql)

    # Deduplicate: enrich one per title+year+type, propagate to dupes
    seen = {}
    unique_entries = []
    for e in entries:
        key = f"{(e['parsed_title'] or e['title']).lower().strip()}|{e['year']}|{e['media_type']}"
        if key not in seen:
            seen[key] = [e]
            unique_entries.append(e)
        else:
            seen[key].append(e)

    print(f"   {len(entries)} entries to enrich ({len(unique_entries)} unique titles)")
    print(f"   OMDb free tier: 1,000 requests/day\n")

    if not unique_entries:
        print("   Nothing to enrich!")
        return

    enriched = 0
    failed = 0
    for entry in tqdm(unique_entries, desc="   Enriching"):
        metadata = enrich_entry(entry)
        if metadata:
            key = f"{(entry['parsed_title'] or entry['title']).lower().strip()}|{entry['year']}|{entry['media_type']}"
            for e in seen[key]:
                update_entry(e["id"], metadata)
            enriched += len(seen[key])
        else:
            failed += 1

        time.sleep(RATE_LIMIT_DELAY)

    print(f"\n   ✅ Enriched {enriched:,} entries")
    if failed:
        print(f"   ⚠ {failed} titles not found on OMDb")

    stats = query("SELECT * FROM catalog_stats")
    if stats:
        s = stats[0]
        print(f"\n   Catalog: {s['enriched']}/{s['total_entries']} enriched\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Enrich catalog with OMDb metadata")
    parser.add_argument("--all", action="store_true", help="Re-enrich all entries")
    parser.add_argument("--limit", type=int, default=0, help="Max entries to enrich")
    parser.add_argument("--id", type=int, default=None, help="Enrich specific entry ID")
    args = parser.parse_args()

    enrich(all_entries=args.all, limit=args.limit, entry_id=args.id)
