"""
Categorize unenriched entries, auto-fix what we can, tag the rest.

Categories:
  deleted_scenes  — extras/deleted scenes/featurettes (not matchable on OMDb)
  dvd_chapter     — raw DVD chapter rips (not matchable)
  sample_file     — sample video files (remove from catalog)
  fixed           — title was auto-corrected (missing apostrophe, wrong type, etc.)
  needs_claude    — couldn't fix automatically; queued for AI matching

Usage:
    python -m enricher.categorize_failures          # dry run (report only)
    python -m enricher.categorize_failures --apply  # apply fixes and tags
"""
import os, re, sys, json
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import query, execute
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Category detectors
# ---------------------------------------------------------------------------

EXTRAS_PATTERNS = re.compile(
    r'\b(deleted.scene|deleted.scenes|extended.scene|outtake|outtakes|'
    r'featurette|featurettes|behind.the.scenes|making.of|bloopers|gag.reel|'
    r'bonus.feature|interview|trailer|teaser|clip|promo|sneak.peek|'
    r'set.tour|gallery|image.gallery)\b',
    re.IGNORECASE
)

DVD_CHAPTER_PATTERNS = re.compile(
    r'(title\d+.*chapter\d+|chapter\d+.*title\d+|disc\d+.*title\d+|'
    r'_disc\d+_|\.title\d+\.|chapter\d{2})',
    re.IGNORECASE
)

SAMPLE_PATTERNS = re.compile(r'(^sample[-._\s]|[-._\s]sample[-._\s]|^sample$)', re.IGNORECASE)

# Common missing-apostrophe fixes: canonical → corrected
APOSTROPHE_FIXES = {
    "marvels ":        "Marvel's ",
    "marvelss ":       "Marvel's ",
    "rupauls ":        "RuPaul's ",
    "its always ":     "It's Always ",
    "americas ":       "America's ",
    "greys ":          "Grey's ",
    "kellys ":         "Kelly's ",
    "dantes ":         "Dante's ",
    "sherlocks ":      "Sherlock's ",
    "neds ":           "Ned's ",
    "whos ":           "Who's ",
    "whats ":          "What's ",
    "hes ":            "He's ",
    "shes ":           "She's ",
    "thats ":          "That's ",
    "wont ":           "Won't ",
    "cant ":           "Can't ",
    "dont ":           "Don't ",
    "didnt ":          "Didn't ",
    "doesnt ":         "Doesn't ",
    "wouldnt ":        "Wouldn't ",
    "couldnt ":        "Couldn't ",
    "shouldnt ":       "Shouldn't ",
    "isnt ":           "Isn't ",
    "wasnt ":          "Wasn't ",
    "arent ":          "Aren't ",
    "havent ":         "Haven't ",
    "hasnt ":          "Hasn't ",
}

def fix_apostrophes(title: str) -> str | None:
    lower = title.lower() + " "
    result = title
    changed = False
    for wrong, right in APOSTROPHE_FIXES.items():
        if wrong in lower:
            # Replace case-insensitively at the right position
            idx = lower.index(wrong)
            result = result[:idx] + right + result[idx + len(wrong):]
            lower = result.lower() + " "
            changed = True
    return result if changed else None


def categorize(entry: dict) -> tuple[str, dict | None]:
    """
    Returns (category, fix_data).
    fix_data is a dict of fields to update in the DB, or None.
    """
    fname = entry.get("file_name", "") or ""
    title = entry.get("parsed_title", "") or entry.get("title", "") or ""
    mtype = entry.get("media_type", "")

    # Sample files
    if SAMPLE_PATTERNS.search(fname) or SAMPLE_PATTERNS.search(title):
        return "sample_file", None

    # DVD chapter rips
    if DVD_CHAPTER_PATTERNS.search(fname) or DVD_CHAPTER_PATTERNS.search(title):
        return "dvd_chapter", None

    # Extras / deleted scenes
    if EXTRAS_PATTERNS.search(title) or EXTRAS_PATTERNS.search(fname):
        return "deleted_scenes", None

    # Misclassified: has S##E## in filename but marked as movie
    if mtype == "movie" and re.search(r'[Ss]\d{1,2}[Ee]\d{1,2}', fname):
        return "fixed", {"media_type": "series"}

    # Missing apostrophes
    fixed_title = fix_apostrophes(title)
    if fixed_title:
        return "fixed", {"parsed_title": fixed_title, "title": fixed_title}

    # Lowercase title (guessit normalisation artefact)
    if title == title.lower() and len(title) > 3:
        titled = title.title()
        if titled != title:
            return "fixed", {"parsed_title": titled, "title": titled}

    return "needs_claude", None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(apply: bool = False):
    entries = query("""
        SELECT id, parsed_title, title, year, media_type, file_name, file_path
        FROM media
        WHERE enriched_at IS NULL
          AND (enrich_skip_reason IS NULL OR enrich_skip_reason = 'needs_claude')
        ORDER BY parsed_title
    """)

    counts = {"deleted_scenes": 0, "dvd_chapter": 0, "sample_file": 0,
              "fixed": 0, "needs_claude": 0}
    needs_claude_entries = []

    for entry in entries:
        cat, fix_data = categorize(entry)
        counts[cat] += 1

        if apply:
            if cat == "fixed" and fix_data:
                fix_data["enrich_skip_reason"] = None  # clear so enricher retries
                sets = ", ".join(f"{k} = %({k})s" for k in fix_data)
                fix_data["id"] = entry["id"]
                execute(f"UPDATE media SET {sets} WHERE id = %(id)s", fix_data)
            elif cat in ("deleted_scenes", "dvd_chapter", "sample_file"):
                execute(
                    "UPDATE media SET enrich_skip_reason = %s WHERE id = %s",
                    (cat, entry["id"])
                )
            elif cat == "needs_claude":
                execute(
                    "UPDATE media SET enrich_skip_reason = 'needs_claude' WHERE id = %s",
                    (entry["id"],)
                )
                needs_claude_entries.append({
                    "id": entry["id"],
                    "parsed_title": entry["parsed_title"],
                    "title": entry["title"],
                    "year": entry["year"],
                    "media_type": entry["media_type"],
                    "file_name": entry["file_name"],
                })

    print(f"\n📊 Failure Categorization Report")
    print(f"   {'Category':<25} {'Count':>6}  {'Action'}")
    print(f"   {'-'*55}")
    print(f"   {'deleted_scenes':<25} {counts['deleted_scenes']:>6}  tag & skip")
    print(f"   {'dvd_chapter':<25} {counts['dvd_chapter']:>6}  tag & skip")
    print(f"   {'sample_file':<25} {counts['sample_file']:>6}  tag & skip")
    print(f"   {'fixed (auto-corrected)':<25} {counts['fixed']:>6}  retry enrichment")
    print(f"   {'needs_claude':<25} {counts['needs_claude']:>6}  queue for AI")
    total = sum(counts.values())
    print(f"   {'-'*55}")
    print(f"   {'TOTAL':<25} {total:>6}")

    if apply:
        out = ROOT / ".enrich_failures.json"
        out.write_text(json.dumps(needs_claude_entries, indent=2))
        print(f"\n   ✅ Fixes applied.")
        print(f"   📄 Claude queue written to: {out.name}")
        print(f"   Run  python -m enricher.fix_failures  to process with AI.")
    else:
        print(f"\n   Dry run — pass --apply to apply fixes.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    run(apply=args.apply)
