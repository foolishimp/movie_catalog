#!/usr/bin/env python3
"""Manual fixes for common patterns in needs_claude entries."""

import json
import re
from enricher.omdb import search, enrich_entry, update_entry
from db import execute, query

# Get entries that need fixing
data = json.load(open('.enrich_failures_batch.json'))
failure_ids = tuple(e['id'] for e in data)

entries = query('''
    SELECT id, parsed_title, file_name, year, media_type
    FROM media
    WHERE id IN %s
    AND (enrich_skip_reason = 'needs_claude' OR (imdb_id IS NULL AND enrich_skip_reason IS NULL))
    ORDER BY id
''', (failure_ids,))

print(f"Processing {len(entries)} entries that need manual attention\n")

stats = {'fixed': 0, 'skipped': 0, 'unresolved': 0}

# Manual title corrections
TITLE_FIXES = {
    # Missing apostrophes
    'mollys game': "Molly's Game",
    'philip k dicks electric dreams': "Philip K. Dick's Electric Dreams",
    'marvels luke cage': "Marvel's Luke Cage",
    'marvels daredevil': "Marvel's Daredevil",
    'greys anatomy': "Grey's Anatomy",
    'oceans eleven': "Ocean's Eleven",
    'schindlers list': "Schindler's List",

    # Spacing issues
    'bleakhouse': 'Bleak House',
    'mollywood': None,  # Not a real title most likely

    # Common movies
    'stan and ollie': 'Stan & Ollie',
    'fritz lang while the city sleeps': 'While the City Sleeps',
}

SKIP_PATTERNS = [
    'etrg', 'rarbg', 'yify', 'sample',
    r'^s\d+e\d+$',  # Just episode numbers
    r'^disc\s*\d+$',  # Disc numbers
    r'dscn\d+',  # Camera files
]

def should_skip(title):
    t = title.lower().strip()
    for pattern in SKIP_PATTERNS:
        if isinstance(pattern, str):
            if pattern in t:
                return True
        else:  # regex
            if re.match(pattern, t):
                return True
    if len(t) < 3:
        return True
    return False

def fix_title(title):
    """Apply manual fixes and return corrected title."""
    t_lower = title.lower().strip()

    # Check manual fixes
    for pattern, fixed in TITLE_FIXES.items():
        if pattern in t_lower:
            return fixed

    # Remove  director prefixes
    title = re.sub(r'^(fritz lang|alfred hitchcock|stanley kubrick|martin scorsese)[:\s-]+', '', title, flags=re.I).strip()

    return title

processed = 0
for entry in entries:
    eid = entry['id']
    title = entry['parsed_title']
    filename = entry['file_name']
    year = entry['year']
    mtype = entry['media_type'] or 'movie'

    # Skip check
    if should_skip(title):
        execute("UPDATE media SET enrich_skip_reason = 'claude_skip' WHERE id = %s", (eid,))
        stats['skipped'] += 1
        processed += 1
        if processed % 50 == 0:
            print(f"Progress: {processed}/{len(entries)} - Fixed: {stats['fixed']}, Skipped: {stats['skipped']}")
        continue

    # Try to fix
    fixed = fix_title(title)
    if not fixed or fixed == title:
        # Can't fix automatically
        stats['unresolved'] += 1
        processed += 1
        continue

    # Try to enrich with fixed title
    result = search(fixed, year, mtype)
    if not result and mtype == 'movie':
        result = search(fixed, year, 'series')
        if result:
            mtype = 'series'
    elif not result and mtype == 'series':
        result = search(fixed, year, 'movie')
        if result:
            mtype = 'movie'

    if result:
        try:
            entry_dict = {
                'id': eid,
                'parsed_title': fixed,
                'year': year,
                'media_type': mtype,
                'file_name': filename
            }
            meta = enrich_entry(entry_dict)
            if meta:
                execute('UPDATE media SET parsed_title = %s, enrich_skip_reason = NULL WHERE id = %s', (fixed, eid))
                update_entry(eid, meta)
                stats['fixed'] += 1
                print(f"Fixed {eid}: '{title}' -> '{fixed}' ({result.get('Year')})")
        except Exception as e:
            print(f"Error {eid}: {e}")
            stats['unresolved'] += 1
    else:
        stats['unresolved'] += 1

    processed += 1
    if processed % 50 == 0:
        print(f"Progress: {processed}/{len(entries)}")

print("\n" + "=" * 60)
print("MANUAL FIXES COMPLETE")
print("=" * 60)
print(f"Fixed:       {stats['fixed']}")
print(f"Skipped:     {stats['skipped']}")
print(f"Unresolved:  {stats['unresolved']}")
print("=" * 60)
