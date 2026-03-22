"""
Phase 1: Scan directories and parse filenames into structured data.

Usage:
    python -m scanner.scan /path/to/movies /path/to/series
    python -m scanner.scan   # uses MEDIA_DIRS from .env
"""
import os
import sys
import hashlib
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
            if p.is_file() and p.suffix.lower() in extensions:
                files.append(p)
    return files


def parse_filename(filepath: Path) -> dict:
    """Use guessit to extract structured info from a filename."""
    info = guessit(filepath.name)

    # Determine media type
    g_type = info.get("type", "")
    if g_type == "episode":
        media_type = "series"
    elif g_type == "movie":
        media_type = "movie"
    else:
        # Heuristic: if path contains season/series indicators, treat as series
        path_lower = str(filepath).lower()
        if any(x in path_lower for x in ["/season", "/series", "/s0", "/s1", "/s2"]):
            media_type = "series"
        else:
            media_type = "unknown"

    title = info.get("title", filepath.stem)
    year = info.get("year")

    # If title is just digits or very short, the filename likely starts with an episode
    # number (e.g. "003 S01E02 Soul Hunter.mkv"). Use the parent directory name instead.
    import re as _re
    if _re.match(r'^\d+$', str(title).strip()):
        dir_info = guessit(filepath.parent.name)
        dir_title = dir_info.get("title", "").strip()
        if dir_title and not _re.match(r'^\d+$', dir_title):
            title = dir_title
            if not year:
                year = dir_info.get("year")

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
        "season": info.get("season") if not isinstance(info.get("season"), list) else info.get("season", [None])[0],
        "episode": info.get("episode") if not isinstance(info.get("episode"), list) else info.get("episode", [None])[0],
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
