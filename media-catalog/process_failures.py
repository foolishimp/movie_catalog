#!/usr/bin/env python3
"""Process enrichment failures and fix them systematically."""

import json
import sys
import re
from enricher.omdb import search, enrich_entry, update_entry
from db import execute

# Load failures
with open('.enrich_failures_batch.json') as f:
    failures = json.load(f)

print(f"Processing {len(failures)} failed entries...\n")

# Track statistics
stats = {
    'matched': 0,
    'skipped': 0,
    'unresolved': 0
}

# Common patterns to fix
def clean_title(title):
    """Clean and normalize title."""
    # Remove release group tags and quality indicators
    title = re.sub(r'\b(SUJAIDR|YIFY|x264|720p|1080p|BRRip|DVDRip|BluRay|WEB-DL|HDTV)\b', '', title, flags=re.I)
    title = re.sub(r'\b\d{3,4}p\b', '', title)  # Remove quality like 1080p
    title = re.sub(r'-[A-Z0-9]{3,}$', '', title)  # Remove trailing release codes

    # Remove scene numbering prefixes
    title = re.sub(r'^[a-z0-9]{3,6}-', '', title, flags=re.I)

    # Clean up spacing
    title = re.sub(r'[_\.]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()

    return title

def identify_year(title, filename):
    """Extract year from title or filename."""
    # Look for 4-digit year
    match = re.search(r'\b(19\d{2}|20\d{2})\b', title + ' ' + filename)
    return int(match.group(1)) if match else None

def should_skip(title, filename):
    """Determine if entry should be skipped (not a real media file)."""
    skip_patterns = [
        r'^[a-f0-9]{6,}$',  # Hex codes
        r'sample',
        r'trailer',
        r'deleted.?scene',
        r'behind.?the.?scene',
        r'making.?of',
        r'interview',
        r'featurette',
        r'tour\s+of',
        r'chapter\s+\d',
        r'dvd\s+menu',
        r'^test',
        r'^\d+$',  # Just numbers
        r'^[a-z]{1,3}\d+$',  # Very short with numbers
    ]

    combined = (title + ' ' + filename).lower()
    return any(re.search(pattern, combined, re.I) for pattern in skip_patterns)

def fix_common_titles(title):
    """Fix commonly misspelled or formatted titles."""
    fixes = {
        # TV Shows
        r'marvels?\s+agents?\s+of\s+s\.?h\.?i\.?e\.?l\.?d': "Marvel's Agents of S.H.I.E.L.D.",
        r'its?\s+always\s+sunny\s+in\s+philadelphia': "It's Always Sunny in Philadelphia",
        r'greys?\s+anatomy': "Grey's Anatomy",
        r'hells?\s+kitchen': "Hell's Kitchen",
        r'americas?\s+got\s+talent': "America's Got Talent",
        r'the\s+handmaids?\s+tale': "The Handmaid's Tale",

        # Movies with apostrophes
        r'oceans?\s+eleven': "Ocean's Eleven",
        r'schindlers?\s+list': "Schindler's List",
        r'boys?\s+dont\s+cry': "Boys Don't Cry",

        # Director prefixes (common pattern)
        r'fellini[:\s]+satyricon': 'Satyricon',
        r'kurosawa[:\s]+': '',
        r'hitchcock[:\s]+': '',
        r'scorsese[:\s]+': '',
    }

    title_lower = title.lower()
    for pattern, replacement in fixes.items():
        if re.search(pattern, title_lower):
            return replacement if replacement else re.sub(pattern, '', title, flags=re.I).strip()

    return title

# Process each entry
for i, entry in enumerate(failures, 1):
    entry_id = entry['id']
    original_title = entry['parsed_title']
    filename = entry['file_name']

    if i % 100 == 0:
        print(f"Progress: {i}/{len(failures)} - Matched: {stats['matched']}, Skipped: {stats['skipped']}, Unresolved: {stats['unresolved']}")

    # Check if should skip
    if should_skip(original_title, filename):
        try:
            execute("UPDATE media SET enrich_skip_reason = 'claude_skip' WHERE id = %s", (entry_id,))
            stats['skipped'] += 1
            continue
        except Exception as e:
            print(f"Error skipping {entry_id}: {e}")
            continue

    # Clean and fix title
    cleaned = clean_title(original_title)
    if not cleaned or len(cleaned) < 2:
        stats['skipped'] += 1
        execute("UPDATE media SET enrich_skip_reason = 'claude_skip' WHERE id = %s", (entry_id,))
        continue

    fixed_title = fix_common_titles(cleaned)
    year = identify_year(original_title, filename)

    # Try to find in OMDb
    media_type = entry.get('media_type', 'movie')
    result = None

    # Try with year if available
    if year:
        result = search(fixed_title, year, media_type)

    # Try without year
    if not result:
        result = search(fixed_title, None, media_type)

    # Try alternate type if failed
    if not result and media_type == 'movie':
        result = search(fixed_title, year, 'series')
        if result:
            media_type = 'series'
    elif not result and media_type == 'series':
        result = search(fixed_title, year, 'movie')
        if result:
            media_type = 'movie'

    if result:
        # Update entry
        entry['parsed_title'] = fixed_title
        entry['year'] = year
        entry['media_type'] = media_type

        try:
            meta = enrich_entry(entry)
            if meta:
                execute('UPDATE media SET parsed_title = %s WHERE id = %s', (fixed_title, entry_id))
                update_entry(entry_id, meta)
                stats['matched'] += 1
            else:
                stats['unresolved'] += 1
        except Exception as e:
            print(f"Error updating {entry_id}: {e}")
            stats['unresolved'] += 1
    else:
        stats['unresolved'] += 1

print("\n" + "="*60)
print("PROCESSING COMPLETE")
print("="*60)
print(f"Total processed: {len(failures)}")
print(f"Successfully matched: {stats['matched']}")
print(f"Skipped (non-media): {stats['skipped']}")
print(f"Unresolved: {stats['unresolved']}")
print("="*60)
