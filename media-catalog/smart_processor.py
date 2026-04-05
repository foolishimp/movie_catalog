#!/usr/bin/env python3
"""Smart processor for enrichment failures with pattern recognition."""

import json
import sys
import re
import time
from enricher.omdb import search, enrich_entry, update_entry
from db import execute

# Load failures
with open('.enrich_failures_batch.json') as f:
    failures = json.load(f)

print(f"Loaded {len(failures)} failed entries")

# Statistics
stats = {
    'matched': 0,
    'skipped': 0,
    'unresolved': 0,
    'errors': 0
}

processed_ids = set()

def should_skip(title, filename):
    """Determine if entry should be skipped."""
    combined = (title + ' ' + filename).lower()

    # Hex codes or gibberish
    if re.match(r'^[a-f0-9]{5,}$', title.lower().replace('-', '')):
        return True

    # Sample/trailer/extras
    skip_keywords = [
        'sample', 'trailer', 'deleted scene', 'behind the scene',
        'making of', 'interview', 'featurette', 'dvd menu',
        'tv spot', 'set tour', 'conversation with', 'celebration in',
        'category theory', 'lecture', 'tutorial'
    ]
    if any(kw in combined for kw in skip_keywords):
        return True

    # Very short or just numbers
    if len(title) < 3 or re.match(r'^\d+$', title):
        return True

    # Scene/chapter fragments
    if re.match(r'^s\d+e\d+p\d+', title.lower()):
        return True

    return False

def clean_title(title):
    """Clean title of release tags."""
    # Remove common release group patterns
    title = re.sub(r'\b(SUJAIDR|YIFY|x264|720p|1080p|BRRip|DVDRip|BluRay|WEB-DL|HDTV|hdtv-lol|ettv)\b', '', title, flags=re.I)
    title = re.sub(r'\[.*?\]', '', title)  # Remove brackets
    title = re.sub(r'\(.*?\)', '', title)  # Remove parentheses

    # Remove scene numbering prefixes like "7o9-"
    title = re.sub(r'^[a-z0-9]{2,6}[-_]', '', title, flags=re.I)

    # Convert dots/underscores to spaces
    title = re.sub(r'[_\.]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()

    return title

def extract_year(text):
    """Extract year from text."""
    match = re.search(r'\b(19\d{2}|20\d{2})\b', text)
    return int(match.group(1)) if match else None

def fix_title(title):
    """Apply common title fixes."""
    title_lower = title.lower()

    # Known title fixes
    if 'agents of shield' in title_lower or 'agents of s.h.i.e.l.d' in title_lower:
        return "Marvel's Agents of S.H.I.E.L.D."
    if 'always sunny' in title_lower:
        return "It's Always Sunny in Philadelphia"
    if 'grey' in title_lower and 'anatomy' in title_lower:
        return "Grey's Anatomy"
    if 'legends of tomorrow' in title_lower:
        return "DC's Legends of Tomorrow"
    if 'handmaid' in title_lower:
        return "The Handmaid's Tale"

    # Warriors Way cleanup
    if 'warriors way' in title_lower:
        return "The Warrior's Way"

    # Director prefix removal
    director_patterns = [
        (r'fellini[:\s]+satyricon', 'Satyricon'),
        (r'le\s+hasard\s+de\s+jacques\s+tati', 'Forza Bastia'),  # This is the actual film
    ]
    for pattern, replacement in director_patterns:
        if re.search(pattern, title_lower):
            return replacement

    return title

def try_match(entry_id, title, year, media_type, filename):
    """Try to match with OMDb and update if successful."""
    if entry_id in processed_ids:
        return False

    # Try primary type with year
    result = None
    if year:
        result = search(title, year, media_type)

    # Try without year
    if not result:
        result = search(title, None, media_type)

    # Try alternate type
    if not result:
        alt_type = 'series' if media_type == 'movie' else 'movie'
        if year:
            result = search(title, year, alt_type)
        if not result:
            result = search(title, None, alt_type)
        if result:
            media_type = alt_type

    if result:
        try:
            # Enrich and update
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
                processed_ids.add(entry_id)
                stats['matched'] += 1
                return True
        except Exception as e:
            print(f"Error updating {entry_id}: {e}")
            stats['errors'] += 1
            return False

    return False

def skip_entry(entry_id, reason='claude_skip'):
    """Mark entry as skipped."""
    if entry_id in processed_ids:
        return
    try:
        execute("UPDATE media SET enrich_skip_reason = %s WHERE id = %s", (reason, entry_id))
        processed_ids.add(entry_id)
        stats['skipped'] += 1
    except Exception as e:
        print(f"Error skipping {entry_id}: {e}")
        stats['errors'] += 1

# Process all entries
print("Processing entries...")
for i, entry in enumerate(failures, 1):
    if i % 100 == 0:
        print(f"Progress: {i}/{len(failures)} | Matched: {stats['matched']}, Skipped: {stats['skipped']}, Unresolved: {stats['unresolved']}")

    entry_id = entry['id']
    original_title = entry['parsed_title']
    filename = entry['file_name']
    media_type = entry.get('media_type', 'movie')

    # Check if should skip
    if should_skip(original_title, filename):
        skip_entry(entry_id)
        continue

    # Clean and process
    cleaned = clean_title(original_title)
    if not cleaned or len(cleaned) < 2:
        skip_entry(entry_id)
        continue

    fixed = fix_title(cleaned)
    year = extract_year(original_title + ' ' + filename)

    # Try to match
    if not try_match(entry_id, fixed, year, media_type, filename):
        stats['unresolved'] += 1

# Final report
print("\n" + "="*70)
print("FINAL REPORT")
print("="*70)
print(f"Total entries:        {len(failures)}")
print(f"Successfully matched: {stats['matched']:>6} ({100*stats['matched']/len(failures):>5.1f}%)")
print(f"Skipped (non-media):  {stats['skipped']:>6} ({100*stats['skipped']/len(failures):>5.1f}%)")
print(f"Unresolved:           {stats['unresolved']:>6} ({100*stats['unresolved']/len(failures):>5.1f}%)")
print(f"Errors:               {stats['errors']:>6}")
print("="*70)

# Show some unresolved for review
if stats['unresolved'] > 0:
    print("\nSample of unresolved entries:")
    count = 0
    for entry in failures:
        if entry['id'] not in processed_ids and count < 20:
            print(f"  ID {entry['id']}: {entry['parsed_title'][:50]}")
            count += 1
