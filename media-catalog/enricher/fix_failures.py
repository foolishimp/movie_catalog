"""
Use Claude Code Agent to identify correct titles for hard-to-match entries,
then retry OMDb enrichment with corrected titles.

The agent uses its film knowledge + OMDb queries to work through failures
in batches, updating the database directly.

Usage:
    python -m enricher.fix_failures            # process all failures
    python -m enricher.fix_failures --batch 50 # process first 50
"""
import anyio
import json
import os
import sys
from pathlib import Path

from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
from dotenv import load_dotenv

load_dotenv()

ROOT          = Path(__file__).resolve().parent.parent
FAILURES_FILE = ROOT / ".enrich_failures.json"
DONE_FILE     = ROOT / ".enrich_failures_done.json"


AGENT_PROMPT = """\
You are fixing a media catalog database. Entries in .enrich_failures.json failed OMDb metadata matching.

For each entry you will:
1. Use your film/TV knowledge to identify the most likely correct title, year, and type
2. Verify by querying OMDb:
   python3 -c "
import sys; sys.path.insert(0,'.')
from enricher.omdb import search
import json
result = search('TITLE', YEAR_OR_NONE, 'movie')  # or 'series'
print(json.dumps(result, indent=2) if result else 'NOT FOUND')
"
3. If found, update the database:
   python3 -c "
import sys; sys.path.insert(0,'.')
from enricher.omdb import enrich_entry, update_entry
entry = {'id': ID, 'parsed_title': 'CORRECTED TITLE', 'year': YEAR, 'media_type': 'TYPE', 'file_name': 'FILENAME'}
meta = enrich_entry(entry)
if meta:
    # also fix parsed_title so it groups correctly
    from db import execute
    execute('UPDATE media SET parsed_title = %s WHERE id = %s', (entry['parsed_title'], entry['id']))
    update_entry(entry['id'], meta)
    print('Updated:', entry['id'])
else:
    print('Still not found')
"
4. If clearly not a real film/show (sample file, set tour, DVD chapter, abstract title), mark it:
   python3 -c "
import sys; sys.path.insert(0,'.')
from db import execute
execute(\"UPDATE media SET enrich_skip_reason = 'claude_skip' WHERE id = %s\", (ID,))
print('Skipped:', ID)
"

Common fixes to look for:
- Missing apostrophes: "marvels agents of shield" → "Marvel's Agents of S.H.I.E.L.D."
- Lowercase: "its always sunny in philadelphia" → "It's Always Sunny in Philadelphia"
- Director as prefix: "fellini satyricon" → "Satyricon" (1969)
- Partial/abbreviated titles
- Alternative title spellings

Work through ALL entries in the file. Be efficient — batch your OMDb calls where you can identify
titles confidently first, then verify. Write a brief summary when done with counts of
matched/skipped/unresolved.
"""


async def run(batch_size: int = 0):
    if not FAILURES_FILE.exists():
        print("No failures file found.")
        print("Run:  python -m enricher.categorize_failures --apply")
        sys.exit(1)

    entries = json.loads(FAILURES_FILE.read_text())

    # Skip already-processed entries
    done_ids = set()
    if DONE_FILE.exists():
        done_ids = set(json.loads(DONE_FILE.read_text()))

    remaining = [e for e in entries if e["id"] not in done_ids]

    if batch_size:
        remaining = remaining[:batch_size]

    if not remaining:
        print("All failures already processed.")
        return

    print(f"\n🤖 Claude Agent Failure Fixer")
    print(f"   {len(remaining)} entries to process\n")

    # Write the current batch to a temp file for the agent to read
    batch_file = ROOT / ".enrich_failures_batch.json"
    batch_file.write_text(json.dumps(remaining, indent=2))

    prompt = (
        f"Process the {len(remaining)} entries in .enrich_failures_batch.json. "
        + AGENT_PROMPT
    )

    async for message in query(
        prompt=prompt,
        options=ClaudeAgentOptions(
            cwd=str(ROOT),
            allowed_tools=["Read", "Bash"],
            permission_mode="bypassPermissions",
        ),
    ):
        if isinstance(message, ResultMessage):
            print(message.result)

    # Mark processed IDs as done
    done_ids.update(e["id"] for e in remaining)
    DONE_FILE.write_text(json.dumps(list(done_ids)))
    batch_file.unlink(missing_ok=True)
    print(f"\n   Progress saved. {len(done_ids)}/{len(entries)} total processed.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=0,
                        help="Process only N entries (default: all)")
    args = parser.parse_args()
    anyio.run(run, args.batch)
