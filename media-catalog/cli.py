#!/usr/bin/env python3
"""
Quick CLI search for the media catalog.

Usage:
    python cli.py search "inception"
    python cli.py search "nolan" --field director
    python cli.py dupes
    python cli.py stats
    python cli.py tag 42 add "favorites"
    python cli.py tag 42 remove "favorites"
    python cli.py export catalog.csv
"""
import sys
import csv
import click
from dotenv import load_dotenv

load_dotenv()
from db import query, execute


def _format_size(b):
    if not b:
        return "?"
    gb = b / 1073741824
    if gb >= 1:
        return f"{gb:.1f} GB"
    return f"{b / 1048576:.0f} MB"


@click.group()
def cli():
    """Media Catalog CLI"""
    pass


@cli.command()
@click.argument("term")
@click.option("--field", default="title", help="Field to search: title, director, genre, path")
@click.option("--limit", default=20, help="Max results")
def search(term, field, limit):
    """Search the catalog."""
    if field == "title":
        sql = """
            SELECT id, title, year, media_type, vote_average, resolution, director,
                   file_path, file_size_bytes
            FROM media
            WHERE tsv @@ websearch_to_tsquery('english', %s)
               OR title ILIKE %s
               OR parsed_title ILIKE %s
            ORDER BY popularity DESC NULLS LAST
            LIMIT %s
        """
        like = f"%{term}%"
        results = query(sql, (term, like, like, limit))
    elif field == "director":
        results = query(
            "SELECT id, title, year, media_type, vote_average, director, file_path, file_size_bytes FROM media WHERE director ILIKE %s ORDER BY year DESC LIMIT %s",
            (f"%{term}%", limit)
        )
    elif field == "genre":
        results = query(
            "SELECT id, title, year, media_type, vote_average, genres, file_path, file_size_bytes FROM media WHERE %s = ANY(genres) ORDER BY vote_average DESC NULLS LAST LIMIT %s",
            (term, limit)
        )
    elif field == "path":
        results = query(
            "SELECT id, title, year, media_type, file_path, file_size_bytes FROM media WHERE file_path ILIKE %s LIMIT %s",
            (f"%{term}%", limit)
        )
    else:
        click.echo(f"Unknown field: {field}")
        return

    if not results:
        click.echo("No results found.")
        return

    click.echo(f"\n{'ID':>5}  {'Title':<40} {'Year':>4}  {'Type':<7} {'Rating':>6}  {'Size':>8}  {'Path'}")
    click.echo("─" * 120)
    for r in results:
        click.echo(
            f"{r['id']:>5}  {(r['title'] or '?')[:40]:<40} {r.get('year') or '?':>4}  "
            f"{r['media_type']:<7} {r.get('vote_average') or 0:>5.1f}★  "
            f"{_format_size(r.get('file_size_bytes')):>8}  {r['file_path']}"
        )
    click.echo(f"\n{len(results)} results\n")


@cli.command()
def dupes():
    """Show duplicate groups."""
    results = query("SELECT * FROM duplicate_candidates ORDER BY copy_count DESC")
    if not results:
        click.echo("No duplicates found!")
        return

    click.echo(f"\n{len(results)} duplicate groups:\n")
    for g in results:
        click.echo(f"  {g['norm_title'].title()} ({g['year'] or '?'}) — {g['copy_count']} copies")
        for i, path in enumerate(g['paths']):
            size = _format_size(g['sizes'][i]) if g['sizes'] else '?'
            res = g['resolutions'][i] if g['resolutions'] else '?'
            click.echo(f"    [{g['ids'][i]:>5}]  {size:>8}  {res or '?':>6}  {path}")
        click.echo()


@cli.command()
def stats():
    """Show catalog statistics."""
    s = query("SELECT * FROM catalog_stats")
    if not s:
        click.echo("Catalog is empty.")
        return
    s = s[0]
    click.echo(f"\n  📊 Catalog Statistics")
    click.echo(f"  {'─' * 30}")
    click.echo(f"  Total entries:    {s['total_entries']:,}")
    click.echo(f"  Movies:           {s['movies']:,}")
    click.echo(f"  Series:           {s['series']:,}")
    click.echo(f"  Enriched:         {s['enriched']:,}")
    click.echo(f"  Unenriched:       {s['unenriched']:,}")
    click.echo(f"  Duplicate groups: {s['duplicate_groups']:,}")
    click.echo(f"  Total size:       {s['total_size']}")
    click.echo()


@cli.command()
@click.argument("entry_id", type=int)
@click.argument("action", type=click.Choice(["add", "remove"]))
@click.argument("tag_name")
def tag(entry_id, action, tag_name):
    """Add or remove a tag from an entry."""
    if action == "add":
        execute(
            "UPDATE media SET tags = array_append(tags, %s), updated_at = NOW() WHERE id = %s AND NOT (%s = ANY(tags))",
            (tag_name, entry_id, tag_name)
        )
        click.echo(f"Added tag '{tag_name}' to entry {entry_id}")
    else:
        execute(
            "UPDATE media SET tags = array_remove(tags, %s), updated_at = NOW() WHERE id = %s",
            (tag_name, entry_id)
        )
        click.echo(f"Removed tag '{tag_name}' from entry {entry_id}")


@cli.command()
@click.argument("output_file", default="catalog.csv")
def export(output_file):
    """Export catalog to CSV."""
    results = query("""
        SELECT id, title, year, media_type, genres, vote_average, director,
               cast_names, overview, resolution, codec, source,
               file_path, file_size_bytes, tags, tmdb_id, imdb_id
        FROM media ORDER BY title
    """)
    if not results:
        click.echo("Nothing to export.")
        return

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        for r in results:
            # Convert lists to strings for CSV
            if r.get('genres'):
                r['genres'] = ', '.join(r['genres'])
            if r.get('cast_names'):
                r['cast_names'] = ', '.join(r['cast_names'])
            if r.get('tags'):
                r['tags'] = ', '.join(r['tags'])
            writer.writerow(r)

    click.echo(f"Exported {len(results)} entries to {output_file}")


if __name__ == "__main__":
    cli()
