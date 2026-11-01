"""
Phase 1: Scan directories and parse filenames into structured data.

Usage:
    python -m scanner.scan /path/to/movies /path/to/series
    python -m scanner.scan   # uses MEDIA_DIRS from .env
"""
import os
import sys
import hashlib
import re
from pathlib import Path
from guessit import guessit
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import executemany, query

DEFAULT_EXTENSIONS = {
    '.mkv', '.mp4', '.avi', '.m4v', '.wmv', '.flv', '.mov',
    '.ts', '.webm', '.mpg', '.mpeg', '.divx', '.ogm', '.iso', '.img',
}

SEASON_DIR_RE = re.compile(r'^(?:season|s)\s*\d{1,3}$|^series\s+\d{1,3}$', re.IGNORECASE)
SERIES_DIR_RE = re.compile(r'^(?:series|tv|tv[-_\s]?shows?|shows)(?:\d+)?$', re.IGNORECASE)
MOVIE_DIR_RE = re.compile(r'^(?:movies?|films?)(?:\d+)?$', re.IGNORECASE)
SAMPLE_FILE_RE = re.compile(r'(^sample(?:[-._\s]|$)|[-._\s]sample(?:[-._\s]|$))', re.IGNORECASE)
EPISODE_MARKER_RE = re.compile(
    r'(?:^|[\s._\-\[])(?:s\d{1,2}e\d{1,3}|\d{1,2}x\d{1,3})(?:$|[\s._\-\]])',
    re.IGNORECASE,
)
RELEASE_TAG_RE = re.compile(
    r'\b(?:uhd|hdrip|brrip|bdrip|dvdrip|webrip|web[- .]?dl|bluray|blu[- .]?ray|'
    r'hdtv|xvid|x264|x265|h\.?264|h\.?265|hevc|avc|aac|ac3|ddp|dts|remux|'
    r'proper|repack|limited|dv|hdr|2160p|1080p|720p|480p)\b',
    re.IGNORECASE,
)


def get_extensions():
    env_ext = os.getenv("VIDEO_EXTENSIONS", "")
    if env_ext:
        return {e.strip().lower() for e in env_ext.split(",") if e.strip()}
    return DEFAULT_EXTENSIONS


def find_video_files(directories: list[str], extensions: set[str]) -> list[Path]:
    """Recursively find all video files in given directories."""
    files = []
    for directory in directories:
        root = Path(directory).expanduser().resolve()
        if not root.exists():
            print(f"  ⚠ Skipping {root} — does not exist")
            continue
        print(f"  Scanning {root} ...")
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in extensions and not SAMPLE_FILE_RE.search(p.stem):
                files.append(p)
    return files


def _path_has(pattern: re.Pattern, filepath: Path) -> bool:
    return any(pattern.match(part) for part in filepath.parent.parts)


def _has_episode_marker(filename: str) -> bool:
    return bool(EPISODE_MARKER_RE.search(filename))


def _movie_title_from_filename(filename: str, parsed_year: int | None) -> tuple[str | None, int | None]:
    """Recover movie titles when GuessIt mistakes bare numbers for episodes."""
    stem = Path(filename).stem
    stem = re.sub(r'[\[\(][^\]\)]*[\]\)]', ' ', stem)
    stem = re.sub(r'[._]+', ' ', stem)
    stem = re.sub(r'\s+', ' ', stem).strip(' -_.')

    release_match = RELEASE_TAG_RE.search(stem)
    if release_match:
        stem = stem[:release_match.start()].strip(' -_.')

    year = parsed_year
    if year and re.search(rf'\b{year}\b', stem):
        stem = re.sub(rf'\b{year}\b.*$', '', stem).strip(' -_.')
    else:
        for match in re.finditer(r'\b(19\d{2}|20[0-3]\d)\b', stem):
            prefix = stem[:match.start()].strip(' -_.')
            if prefix:
                year = int(match.group(1))
                stem = prefix
                break

    return (stem or None), year


def _title_from_directory(filepath: Path) -> tuple:
    """Walk up directory tree to find a title when filename parsing fails.

    Skips 'Season N' style folders and returns the first ancestor whose
    guessit parse yields a plausible title.
    """
    for ancestor in filepath.parents:
        name = ancestor.name
        if not name:
            break
        if SEASON_DIR_RE.match(name) or SERIES_DIR_RE.match(name) or MOVIE_DIR_RE.match(name):
            continue
        dir_info = guessit(name)
        dir_title = dir_info.get("title", "").strip()
        if dir_title and len(dir_title) >= 2 and not re.match(r'^\d+$', dir_title):
            return dir_title, dir_info.get("year")
    return None, None


def parse_filename(filepath: Path) -> dict:
    """Use guessit to extract structured info from a filename."""
    info = guessit(filepath.name)

    # Determine media type
    g_type = info.get("type", "")
    explicit_episode_marker = _has_episode_marker(filepath.name)
    explicit_season_dir = _path_has(SEASON_DIR_RE, filepath)
    movie_dir = _path_has(MOVIE_DIR_RE, filepath)
    series_dir = _path_has(SERIES_DIR_RE, filepath)
    movie_release_misread = g_type == "episode" and movie_dir and not explicit_episode_marker and not explicit_season_dir

    if movie_release_misread:
        media_type = "movie"
    elif g_type == "episode":
        media_type = "series"
    elif g_type == "movie":
        media_type = "movie"
    elif explicit_season_dir or series_dir:
        media_type = "series"
    elif movie_dir:
        media_type = "movie"
    else:
        media_type = "unknown"

    title = info.get("title", filepath.stem)
    year = info.get("year")
    if movie_release_misread:
        recovered_title, recovered_year = _movie_title_from_filename(filepath.name, year)
        if recovered_title:
            title = recovered_title
        year = recovered_year

    # Detect weak titles that need directory fallback:
    #  - all digits (episode number as filename)
    #  - very short (≤2 chars)
    #  - same as release group (e.g. ETRG.mp4)
    #  - starts with S##E## (episode code baked into title)
    import re as _re
    t = str(title).strip()
    needs_fallback = (
        not t
        or len(t) <= 2
        or _re.match(r'^\d+$', t)
        or t.lower() == str(info.get("release_group", "")).lower()
        or _re.match(r'^S\d+E\d+', t, _re.IGNORECASE)
    )

    if needs_fallback:
        dir_title, dir_year = _title_from_directory(filepath)
        if dir_title:
            title = dir_title
            if not year:
                year = dir_year

    # Strip leading disc/file-number prefix like "1 The Larry Sanders Show" → "The Larry Sanders Show"
    # Only strip when followed by an article (The/A/An) to avoid breaking titles like "30 Rock"
    title_stripped = _re.sub(r'^\d{1,2} (?=The |A |An )', '', str(title)).strip()
    if title_stripped and title_stripped != str(title):
        title = title_stripped

    return {
        "title": title,
        "parsed_title": title,
        "year": year,
        "media_type": media_type,
        "season": None if media_type == "movie" else (
            info.get("season") if not isinstance(info.get("season"), list) else info.get("season", [None])[0]
        ),
        "episode": None if media_type == "movie" else (
            info.get("episode") if not isinstance(info.get("episode"), list) else info.get("episode", [None])[0]
        ),
        "resolution": info.get("screen_size"),
        "codec": info.get("video_codec"),
        "source": info.get("source"),
        "release_group": info.get("release_group"),
        "file_path": str(filepath),
        "file_name": filepath.name,
        "file_size_bytes": filepath.stat().st_size,
        "file_ext": filepath.suffix.lower(),
        "directory": str(filepath.parent),
    }


def generate_duplicate_group(title: str, year: int | None, media_type: str) -> str:
    """Create a stable hash for grouping potential duplicates."""
    norm = f"{(title or '').lower().strip()}|{year or ''}|{media_type}"
    return hashlib.md5(norm.encode()).hexdigest()[:12]


def scan(directories: list[str], rescan: bool = False):
    """Main scan entry point."""
    extensions = get_extensions()

    print(f"\n🎬 Media Catalog Scanner")
    print(f"   Extensions: {', '.join(sorted(extensions))}")
    print(f"   Directories: {len(directories)}\n")

    # Find files
    files = find_video_files(directories, extensions)
    print(f"\n   Found {len(files):,} video files\n")

    if not files:
        return

    # Check which files are already in the database
    if not rescan:
        existing = {r["file_path"] for r in query("SELECT file_path FROM media")}
        new_files = [f for f in files if str(f) not in existing]
        print(f"   {len(existing):,} already cataloged, {len(new_files):,} new\n")
    else:
        new_files = files

    if not new_files:
        print("   Nothing new to add. Use --rescan to re-parse everything.")
        return

    # Parse and insert
    records = []
    errors = []
    for filepath in tqdm(new_files, desc="   Parsing"):
        try:
            rec = parse_filename(filepath)
            rec["duplicate_group"] = generate_duplicate_group(
                rec["title"], rec["year"], rec["media_type"]
            )
            records.append(rec)
        except Exception as e:
            errors.append((str(filepath), str(e)))

    if errors:
        print(f"\n   ⚠ {len(errors)} files had parse errors:")
        for path, err in errors[:10]:
            print(f"     {path}: {err}")
        if len(errors) > 10:
            print(f"     ... and {len(errors) - 10} more")

    if not records:
        return

    # Batch upsert
    sql = """
        INSERT INTO media (
            title, parsed_title, year, media_type,
            season, episode, resolution, codec, source, release_group,
            file_path, file_name, file_size_bytes, file_ext, directory,
            duplicate_group
        ) VALUES (
            %(title)s, %(parsed_title)s, %(year)s, %(media_type)s,
            %(season)s, %(episode)s, %(resolution)s, %(codec)s, %(source)s, %(release_group)s,
            %(file_path)s, %(file_name)s, %(file_size_bytes)s, %(file_ext)s, %(directory)s,
            %(duplicate_group)s
        )
        ON CONFLICT (file_path) DO UPDATE SET
            title = EXCLUDED.title,
            parsed_title = EXCLUDED.parsed_title,
            year = EXCLUDED.year,
            media_type = EXCLUDED.media_type,
            season = EXCLUDED.season,
            episode = EXCLUDED.episode,
            resolution = EXCLUDED.resolution,
            codec = EXCLUDED.codec,
            source = EXCLUDED.source,
            release_group = EXCLUDED.release_group,
            file_name = EXCLUDED.file_name,
            file_size_bytes = EXCLUDED.file_size_bytes,
            file_ext = EXCLUDED.file_ext,
            directory = EXCLUDED.directory,
            duplicate_group = EXCLUDED.duplicate_group,
            updated_at = NOW()
    """
    print(f"\n   Inserting {len(records):,} records into database...")
    executemany(sql, records)

    # Print summary
    stats = query("SELECT * FROM catalog_stats")
    if stats:
        s = stats[0]
        print(f"\n   ✅ Catalog now contains:")
        print(f"      {s['total_entries']:,} entries ({s['movies']:,} movies, {s['series']:,} series)")
        print(f"      {s['duplicate_groups']:,} potential duplicate groups")
        print(f"      {s['total_size']} total size on disk\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scan directories for video files")
    parser.add_argument("dirs", nargs="*", help="Directories to scan")
    parser.add_argument("--rescan", action="store_true", help="Re-parse all files, not just new ones")
    args = parser.parse_args()

    dirs = args.dirs
    if not dirs:
        env_dirs = os.getenv("MEDIA_DIRS", "")
        dirs = [d.strip() for d in env_dirs.split(":") if d.strip()]

    if not dirs:
        print("Error: No directories specified. Pass them as arguments or set MEDIA_DIRS in .env")
        sys.exit(1)

    scan(dirs, rescan=args.rescan)
