#!/usr/bin/env python3
"""Optimized batch processor for enrichment failures."""

import json
import re
import sys
import time
from enricher.omdb import search, enrich_entry, update_entry
from db import execute

# Load failures
with open('.enrich_failures_batch.json') as f:
    failures = json.load(f)

print(f"Processing {len(failures)} failed entries\n")

stats = {'matched': 0, 'skipped': 0, 'unresolved': 0, 'errors': 0}
processed = set()

def should_skip(title, filename):
    """Quick skip check for non-media files."""
    combined = (title + ' ' + filename).lower()

    # Gibberish/hex codes
    clean_title = title.lower().replace('-', '').replace('_', '')
    if re.match(r'^[a-f0-9]{5,}$', clean_title) and len(clean_title) > 6:
        return True

    # Extras/special features
    skip_words = ['sample', 'trailer', 'deleted scene', 'behind the scene',
                  'making of', 'interview', 'featurette', 'dvd menu', 'tv spot',
                  'set tour', 'conversation with', 'celebration', 'category theory',
                  'lecture', 'tutorial', 'chapter', 'tour of']
    if any(w in combined for w in skip_words):
        return True

    # Episode parts (like s08e01p03)
    if re.search(r's\d+e\d+p\d+', combined):
        return True

    # Very short
    if len(title.strip()) < 2:
        return True

    return False

def extract_year(text):
    """Extract 4-digit year."""
    m = re.search(r'\b(19\d{2}|20\d{2})\b', text)
    return int(m.group(1)) if m else None

def clean_title(title):
    """Remove release tags and normalize."""
    # Remove scene group prefixes
    title = re.sub(r'^[a-z0-9]{2,6}[-_]', '', title, flags=re.I)

    # Remove quality/codec tags
    tags = ['SUJAIDR', 'YIFY', 'x264', 'x265', 'h264', 'h265', 'HEVC',
            '720p', '1080p', '2160p', '480p', '4K',
            'BRRip', 'DVDRip', 'BluRay', 'Bluray', 'BDRip', 'Bdrip',
            'WEB-DL', 'WEBDL', 'WEBRip', 'HDTV', 'HDRip',
            'Xvid', 'DivX', 'AAC', 'MP3', 'AC3',
            'hdtv-lol', 'ettv', 'rarbg']

    for tag in tags:
        title = re.sub(r'\b' + tag + r'\b', '', title, flags=re.I)

    # Remove brackets and contents
    title = re.sub(r'\[.*?\]', '', title)
    title = re.sub(r'\(.*?\)', '', title)

    # Remove year from title body
    title = re.sub(r'\b(19|20)\d{2}\b', '', title)

    # Convert separators to spaces
    title = re.sub(r'[_\.]', ' ', title)

    # Clean whitespace
    title = re.sub(r'\s+', ' ', title).strip(' -_.')

    return title

def apply_known_fixes(title_lower, title):
    """Apply known title corrections."""
    # TV shows with apostrophes
    if 'agents of shield' in title_lower or 'agents of s.h.i.e.l.d' in title_lower:
        return "Marvel's Agents of S.H.I.E.L.D."
    if 'always sunny' in title_lower and 'philadelphia' in title_lower:
        return "It's Always Sunny in Philadelphia"
    if 'grey' in title_lower and 'anatomy' in title_lower:
        return "Grey's Anatomy"
    if 'legends of tomorrow' in title_lower:
        return "DC's Legends of Tomorrow"
    if 'handmaid' in title_lower and 'tale' in title_lower:
        return "The Handmaid's Tale"
    if 'hell' in title_lower and 'kitchen' in title_lower:
        return "Hell's Kitchen"
    if 'ocean' in title_lower and 'eleven' in title_lower:
        return "Ocean's Eleven"
    if 'schindler' in title_lower and 'list' in title_lower:
        return "Schindler's List"

    # Movies with possessives
    if 'warrior' in title_lower and 'way' in title_lower:
        return "The Warrior's Way"

    # Director prefixes
    if 'fellini' in title_lower and 'satyricon' in title_lower:
        return 'Satyricon'
    if 'akira kurosawa' in title_lower and 'dreams' in title_lower:
        return "Dreams"  # Akira Kurosawa's Dreams
    if re.search(r'kurosawa.*wonderful.*create', title_lower):
        return "It Is Wonderful to Create"

    # Remove director prefixes
    title = re.sub(r'^(akira kurosawa|fellini|hitchcock|scorsese|kubrick)[:\s-]+', '', title, flags=re.I).strip()

    return title

def try_enrich(entry_id, title, year, media_type, filename):
    """Attempt to find and update entry."""
    if entry_id in processed:
        return False

    # Try search
    result = search(title, year, media_type) if year else None
    if not result:
        result = search(title, None, media_type)

    # Try alternate type
    if not result:
        alt = 'series' if media_type == 'movie' else 'movie'
        result = search(title, year, alt) if year else search(title, None, alt)
        if result:
            media_type = alt

    if result:
        try:
            entry = {
                'id': entry_id,
                'parsed_title': title,
                'year': year,
                'media_type': media_type,
                'file_name': filename
            }
            meta = enrich_entry(entry)
            if meta:
                execute('UPDATE media SET parsed_title = %s WHERE id = %s', (title, entry_id))
                update_entry(entry_id, meta)
                processed.add(entry_id)
                stats['matched'] += 1
                return True
        except Exception as e:
            print(f"Error {entry_id}: {e}")
            stats['errors'] += 1

    return False

def skip(entry_id):
    """Mark as skipped."""
    if entry_id in processed:
        return
    try:
        execute("UPDATE media SET enrich_skip_reason = %s WHERE id = %s", ('claude_skip', entry_id))
        processed.add(entry_id)
        stats['skipped'] += 1
    except Exception as e:
        print(f"Skip error {entry_id}: {e}")
        stats['errors'] += 1

# Main processing loop
for i, entry in enumerate(failures, 1):
    if i % 50 == 0:
        print(f"{i}/{len(failures)} | M:{stats['matched']} S:{stats['skipped']} U:{stats['unresolved']}")
        time.sleep(0.05)  # Rate limiting

    eid = entry['id']
    orig = entry['parsed_title']
    fname = entry['file_name']
    mtype = entry.get('media_type', 'movie')

    # Skip check
    if should_skip(orig, fname):
        skip(eid)
        continue

    # Clean title
    cleaned = clean_title(orig)
    if not cleaned or len(cleaned) < 2:
        skip(eid)
        continue

    # Apply fixes
    fixed = apply_known_fixes(cleaned.lower(), cleaned)
    year = extract_year(orig + ' ' + fname)

    # Try to enrich
    if not try_enrich(eid, fixed, year, mtype, fname):
        stats['unresolved'] += 1

# Final report
print("\n" + "=" * 70)
print("PROCESSING COMPLETE")
print("=" * 70)
print(f"Total:       {len(failures)}")
print(f"Matched:     {stats['matched']:>6} ({100*stats['matched']/len(failures):5.1f}%)")
print(f"Skipped:     {stats['skipped']:>6} ({100*stats['skipped']/len(failures):5.1f}%)")
print(f"Unresolved:  {stats['unresolved']:>6} ({100*stats['unresolved']/len(failures):5.1f}%)")
print(f"Errors:      {stats['errors']:>6}")
print("=" * 70)
