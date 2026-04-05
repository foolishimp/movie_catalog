#!/usr/bin/env python3
"""Targeted fixes for specific remaining entries."""

import json
from enricher.omdb import search, enrich_entry, update_entry
from db import execute, query

data = json.load(open('.enrich_failures_batch.json'))
failure_ids = tuple(e['id'] for e in data)

# Get unresolved
unresolved = query('''
    SELECT id, parsed_title, file_name, year, media_type
    FROM media
    WHERE id IN %s
    AND (enrich_skip_reason = 'needs_claude' OR (imdb_id IS NULL AND enrich_skip_reason IS NULL))
''', (failure_ids,))

print(f"Targeting {len(unresolved)} unresolved entries\n")

stats = {'fixed': 0, 'skipped': 0}

# Specific fixes based on the sample
FIXES = [
    # Episode titles that are actually series
    ('A Song for Portlandia', 'Portlandia', 'series'),
    ('Babel One', 'Star Trek: Enterprise', 'series'),
    ('In A Mirror Darkly', 'Star Trek: Enterprise', 'series'),
    ('Doug Becomes A Feminist', 'The Deug', 'series'),
    ('Cool Wedding', 'The Office', 'series'),
    ('One Moore Episode', 'The Unbreakable Kimmy Schmidt', None),
    ('The Fuzzy Boots Corollary', 'The Big Bang Theory', 'series'),

    # Typos
    ('Fasrcape', 'Farscape', 'series'),
    ('Smileys People', "Smiley's People", 'series'),
    ('Philip K Dicks Electric Dreams', "Philip K. Dick's Electric Dreams", 'series'),

    # Series with apostrophes
    ("Marvel's Luke Cage", "Marvel's Luke Cage", 'series'),

    # Movies with director prefix
    ('Religulous by Bill Maher', 'Religulous', 'movie'),
    ('Star Trek The Motion Picture The Directors Edition', 'Star Trek: The Motion Picture', 'movie'),

    # Abbreviations
    ('RPDR', "RuPaul's Drag Race", 'series'),
    ('Orac', 'Blake\'s 7', 'series'),  # Orac is a character/episode in Blake's 7
]

# Items to skip
SKIP_TITLES = [
    'DSCN', 'SaNple', 'Sample', 'DISC', 'Patreon',
    'Alan Cumming, Jennifer Lopez',  # Cast list
    'Tristan Taormino',  # Adult content
    'Atheist IQ', 'Autotune Andy', 'Mommy Meyer',  # YouTube/web content
]

def try_fix(entry_id, original_title, fixed_title, year, mtype, filename):
    """Try to enrich with fixed title."""
    result = search(fixed_title, year, mtype) if mtype else search(fixed_title, year, 'movie')

    if not result and mtype != 'series':
        result = search(fixed_title, year, 'series')
        if result:
            mtype = 'series'

    if result:
        try:
            entry = {
                'id': entry_id,
                'parsed_title': fixed_title,
                'year': year,
                'media_type': mtype or 'movie',
                'file_name': filename
            }
            meta = enrich_entry(entry)
            if meta:
                execute('UPDATE media SET parsed_title = %s, enrich_skip_reason = NULL WHERE id = %s', (fixed_title, entry_id))
                update_entry(entry_id, meta)
                print(f"✓ {entry_id}: '{original_title}' -> '{fixed_title}' ({result.get('Year')})")
                return True
        except Exception as e:
            print(f"✗ Error {entry_id}: {e}")
    return False

# Apply fixes
for orig, fixed, mtype in FIXES:
    matching = [e for e in unresolved if e['parsed_title'] == orig]
    for entry in matching:
        if try_fix(entry['id'], orig, fixed, entry['year'], mtype, entry['file_name']):
            stats['fixed'] += 1

# Skip items
for skip_pattern in SKIP_TITLES:
    matching = [e for e in unresolved if skip_pattern.lower() in e['parsed_title'].lower()]
    for entry in matching:
        execute("UPDATE media SET enrich_skip_reason = 'claude_skip' WHERE id = %s", (entry['id'],))
        print(f"⊗ Skipped {entry['id']}: {entry['parsed_title'][:50]}")
        stats['skipped'] += 1

print(f"\nFixed: {stats['fixed']}, Skipped: {stats['skipped']}")
