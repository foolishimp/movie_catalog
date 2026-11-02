"""
Resolve stage: decide each book unit's title, authors and other fields from
the evidence the scanner stored. Reads and writes only the database, so it can
be re-run (and re-tuned) without touching the network share.

Evidence is grouped by independent provenance and each group gets one vote:

    calibre     Calibre's database row, its metadata.opf sidecar, and anything
                Calibre wrote into files (its OPF, MOBI conversions, HTMLZ).
                Folder and file names inside a Calibre library are generated
                from the same row, so they are not a separate vote.
    publisher   metadata embedded by whoever produced the file (EPUB OPF,
                MOBI EXTH, FB2, ComicInfo).
    document    PDF/DOCX document properties: often junk, weak weight.
    text        the book's own front matter: "by X" lines, copyright lines.
    filename    loose filenames and pre-import names Calibre kept inside
                archives; orientation of "A - B" is unknown.
    lookup      a catalogue record fetched by ISBN (books.lookup). It names
                authors only, and only when its title agrees with the book's.

Calibre sometimes stored titles and authors the wrong way round. Every
candidate whose orientation is not fixed by a labelled field is scored both
ways, using how name-like each side is and how often each side appears as an
author across the whole corpus; the stored orientation is kept unless the
evidence clearly favours the swap. Doubtful cases are flagged needs_review.

Usage:
    python -m books.resolve
"""
import hashlib
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from datetime import date

import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn
from books.awards import classify_award_tag
from books.formats import is_junk_title
from books.naming import (GIVEN_NAMES, clean_title, fix_mojibake, flip_last_first, is_corporate, looks_like_handle, name_score, norm_name,
                          norm_title, normalize_isbn, parse_filename, recase_name, split_authors, JUNK_AUTHOR,
                          SURE_NAME,
                          strip_author_from_title)

GROUP_WEIGHT = {"calibre_isfdb": 3.5, "calibre": 2.0, "publisher": 3.0, "lookup": 2.5, "document": 0.8,
                "text": 2.0, "filename": 1.2, "folder": 0.8}
CALIBRE_SWAP_MARGIN = 1.5      # evidence needed to overturn Calibre's stored orientation
FILENAME_AMBIGUOUS = 0.8       # |score| below this leaves an "A - B" filename undecided
HANDLE_WEIGHT = 0.25           # vote share for uploader/account names found in file metadata
FORMAT_PREFERENCE = ["epub", "kepub", "azw3", "mobi", "azw", "fb2", "pdf", "cbz", "docx", "htmlz", "lit", "rtf", "txt"]
LANG_3 = {"en": "eng", "fr": "fra", "de": "deu", "es": "spa", "it": "ita", "ru": "rus", "nl": "nld",
          "ja": "jpn", "zh": "zho", "pt": "por", "sv": "swe", "da": "dan", "no": "nor", "fi": "fin", "pl": "pol"}
GENERIC_FOLDERS = {"books", "ebooks", "fiction", "non fiction", "nonfiction", "misc", "lib", "library", "calibre",
                   "kindle", "my kindle content", "comics", "comic", "stage", "temp", "download", "downloads"}
# Tie-in franchises that file names put where the author goes ("Doctor Who - 041 - Logopolis").
TIE_IN = re.compile(r"^(?:star trek|doctor who|dr\.? who|babylon 5|star wars|stargate|battlestar galactica|"
                    r"(?:the )?x-files|buffy the vampire slayer)\b", re.I)
# "New Scientist 2009-01-17": a magazine issue, whose title-page names are its staff.
DATED_ISSUE = re.compile(r"\b(?:19|20)\d\d[-_. ](?:0[1-9]|1[0-2])(?:[-_. ](?:[0-2]\d|3[01]))?\b")


# ---------------------------------------------------------------------------
# Corpus context
# ---------------------------------------------------------------------------

class Context:
    """Corpus-wide statistics the per-unit decisions lean on."""

    def __init__(self):
        self.author_freq: Counter = Counter()
        self.given = set(GIVEN_NAMES)
        self.lookups: dict = {}       # isbn -> catalogue record (books.lookup)

    def author_weight(self, name: str) -> float:
        parts = split_authors(name) or [name]
        freqs = [self.author_freq.get(norm_name(p), 0) for p in parts]
        return sum(min(2.5, math.log2(1 + f)) for f in freqs) / len(freqs)

    def side(self, text: str) -> float:
        """Author-likeness of one side of a pair: surface form plus corpus frequency."""
        if text and text == text.lower():   # a lower-case record says nothing about which side is the name
            text = recase_name(text)
        return name_score(text, self.given) + 0.9 * self.author_weight(text)


def build_context(units: list[dict]) -> Context:
    ctx = Context()
    first_names = Counter()
    for u in units:
        seen = set()
        cal = u.get("calibre") or {}
        for a in cal.get("authors") or []:
            seen.add(norm_name(a))
        for f in u["files"]:
            ev = f.get("evidence") or {}
            meta = ev.get("meta") or {}
            if ev.get("meta_group") == "publisher":
                for a in meta.get("authors") or []:
                    seen.add(norm_name(a))
                    first = norm_name(a).split(" ")[0] if norm_name(a) else ""
                    if len(first) > 2:
                        first_names[first] += 1
            for name in (ev.get("text") or {}).get("bylines") or []:
                seen.add(norm_name(name))
        if cal.get("isfdb"):
            for a in cal.get("authors") or []:
                first = norm_name(a).split(" ")[0] if norm_name(a) else ""
                if len(first) > 2:
                    first_names[first] += 2
        for key in seen:
            if key:
                ctx.author_freq[key] += 1
    ctx.given |= {n for n, c in first_names.items() if c >= 3}
    return ctx


# ---------------------------------------------------------------------------
# Per-unit resolution (pure)
# ---------------------------------------------------------------------------

def _pick_files(files: list[dict]) -> list[dict]:
    present = [f for f in files if f.get("present", True)]
    return sorted(present or files, key=lambda f: (FORMAT_PREFERENCE.index(f["format"])
                                                     if f.get("format") in FORMAT_PREFERENCE else 99))


def _clean_authors(names) -> list[str]:
    """Split joined author strings and drop publishers; Calibre stores ',' inside a name as '|'."""
    out = []
    for name in names or []:
        out += split_authors(name.replace("|", ","))
    return [a for a in out if a and not is_corporate(a) and not is_junk_title(a) and not JUNK_AUTHOR.search(a)]


def _fix_text(value):
    """Repair mojibake everywhere in a unit's Calibre record and file metadata."""
    if isinstance(value, str):
        return fix_mojibake(value)
    if isinstance(value, dict):
        return {k: _fix_text(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_fix_text(v) for v in value]
    return value


def _candidates(unit: dict, files: list[dict]) -> tuple[list[dict], dict]:
    """Title/author candidates, one per independent group, plus loose clues."""
    cands = []
    clues = {"bylines": [], "copyright": [], "title_pages": [], "isbns": []}
    cal = unit.get("calibre") or {}
    if cal.get("title") or cal.get("authors"):
        group = "calibre_isfdb" if cal.get("isfdb") else "calibre"
        authors = _clean_authors(cal.get("authors"))
        title, raw = cal.get("title"), [a for a in cal.get("authors") or [] if a]
        dash = re.match(r"^(.+)(?:\s+-{1,2}\s*|-{1,2}\s+|--)(.+)$", title or "") if not authors else None
        if dash:
            # "Changes - Danielle Steel": the name is the author. The corpus check turns round
            # "Cornell Woolrich - Dancing Detective"; "Margaret Carter- Foxfire" is plainly that way round.
            left, right = dash.group(1).strip(), dash.group(2).strip()
            if name_score(right) > 0.5:
                title, authors = left, split_authors(right)
            elif name_score(left) >= SURE_NAME:
                title, authors = right, split_authors(left)
            raw = authors or raw
        cands.append({"group": group, "title": strip_author_from_title(title, authors), "authors": authors,
                      "raw_authors": raw,
                      "fixed": group == "calibre_isfdb", "prior": True, "source": f"calibre:{cal.get('library')}"})

    seen_groups = {c["group"] for c in cands}
    calibre_meta_used = bool(cands)
    for f in files:
        ev = f.get("evidence") or {}
        meta, group = ev.get("meta"), ev.get("meta_group")
        if meta and group:
            if group == "calibre":
                # Calibre's own embedded copy is the same vote as its database row.
                if not calibre_meta_used and (meta.get("title") or meta.get("authors")):
                    authors = _clean_authors(meta.get("authors"))
                    cands.append({"group": "calibre", "title": strip_author_from_title(meta.get("title"), authors),
                                  "authors": authors, "raw_authors": [a for a in meta.get("authors") or [] if a],
                                  "fixed": False, "prior": True,
                                  "source": f"calibre-embedded:{ev['format']}"})
                    calibre_meta_used = True
            elif group not in seen_groups and (meta.get("title") or meta.get("authors")):
                authors = _clean_authors(meta.get("authors"))
                if group == "document":   # one word in a PDF's author field is a login: "Morgan", "a.b.example"
                    authors = [a for a in authors if " " in a.strip()]
                title = None if is_junk_title(meta.get("title")) else strip_author_from_title(meta.get("title"), authors)
                if title or authors:
                    cands.append({"group": group, "title": title, "authors": authors, "fixed": False,
                                  "prior": False, "source": f"{group}:{ev['format']}"})
                    seen_groups.add(group)
        text = ev.get("text") or {}
        bylines = (re.sub(r"^[:;,.\s]*(?:by\s+)?|[:;,\s]+$", "", b, flags=re.I) for b in text.get("bylines") or [])
        clues["bylines"] += [b for b in bylines if b and not is_corporate(b) and not JUNK_AUTHOR.search(b)]
        clues["copyright"] += text.get("copyright") or []     # corporate holders still date the book
        if text.get("title_page"):
            clues["title_pages"].append(text["title_page"])
        clues["isbns"] += text.get("isbns") or []

    # Filenames are re-parsed here rather than read from stored evidence, so
    # naming improvements apply on the next resolve without re-reading files.
    filename_sources = []
    for f in files:
        ev = f.get("evidence") or {}
        if ev.get("inner"):
            filename_sources.append(parse_filename(ev["inner"].rsplit("/", 1)[-1]))
        if not unit.get("calibre_dir") and f.get("file_path"):
            filename_sources.append(parse_filename(os.path.basename(f["file_path"])))
    clues["filename_isbns"] = [fn["isbn"] for fn in filename_sources if fn.get("isbn")]
    for fn in filename_sources[:1]:
        cands.append({"group": "filename", "first": fn.get("first"), "second": fn.get("second"),
                      "ambiguous": fn.get("ambiguous"), "series": fn.get("series"),
                      "series_index": fn.get("series_index"), "pattern": fn.get("pattern"), "source": "filename"})

    if not unit.get("calibre_dir"):
        folder = os.path.basename(unit["directory"])
        if folder.lower() not in GENERIC_FOLDERS and name_score(folder) > 1.0:
            clues["folder_author"] = folder

    # A file whose metadata reads "Philip Jose Farmer - The Book of ..." credits
    # another candidate's author in its title: strip it, and drop type tags.
    known = [a for c in cands for a in c.get("authors") or [] if not looks_like_handle(a)]
    for c in cands:
        if c.get("title") and not c.get("fixed"):
            c["title"] = clean_title(strip_author_from_title(c["title"], known))

    if any(DATED_ISSUE.search(c.get("title") or c.get("first") or "") for c in cands):
        clues["bylines"] = []
    return cands, clues


_TITLE_FILLER = {"the", "a", "an", "of", "and", "for", "to", "in", "on", "with", "s", "vol", "volume", "book",
                 "part", "no", "edition", "ed"}
_CATALOGUE_BODY = re.compile(r"\b(?:team|staff|editors|editorial board)$", re.I)
_NUMBER_WORDS = {w: str(i) for i, w in enumerate("zero one two three four five six seven eight nine ten".split())}


def _title_words(title: str | None) -> set:
    """Words that identify a title: no filler, no plural s, numbers as plain digits ('09', 'Book One')."""
    words = re.sub(r"_|(?<=[a-z])(?=\d)|(?<=\d)(?=[a-z])", " ", norm_title(title)).split()
    words = (_NUMBER_WORDS.get(w, w.lstrip("0") or "0") if w.isdigit() or w in _NUMBER_WORDS else w for w in words)
    return {re.sub(r"(?<=\w{3})s$", "", w) for w in words if w not in _TITLE_FILLER}


def _title_agrees(found: str | None, titles) -> bool:
    """A catalogue title against the book's own titles: most words of the shorter one appear in the
    other, so a file name's run-on subtitle still matches ('Dungeon master guide for the AD&D game'
    / 'Dungeon Masters Guide'), while numbers and punctuation are free to differ ('C♯ 3.0 unleashed')."""
    a = _title_words(found)
    for t in titles:
        b = _title_words(t)
        short, long = sorted((a, b), key=len)
        if short and 3 * len(short & long) >= 2 * len(short):
            return True
    return False


def _lookup_candidate(unit: dict, files: list[dict], cands: list[dict], clues: dict, ctx: Context) -> dict | None:
    """Authors from the catalogue record of one of the unit's ISBNs, when the record's title agrees."""
    if not ctx.lookups:
        return None
    metas = [(f.get("evidence") or {}).get("meta") or {} for f in files]
    ids = [(unit.get("calibre") or {}).get("identifiers") or {}] + [m.get("identifiers") or {} for m in metas]
    isbns = ([normalize_isbn(i.get("isbn")) for i in ids] + [normalize_isbn(m.get("isbn")) for m in metas]
             + clues["isbns"] + clues["filename_isbns"])
    titles = [t for c in cands for t in (c.get("title"), c.get("first"), c.get("second")) if t]
    titles += clues["title_pages"]
    for isbn in dict.fromkeys(i for i in isbns if i):
        rec = ctx.lookups.get(isbn)
        if not rec:
            continue
        found = " ".join(t for t in (rec.get("title"), rec.get("subtitle")) if t)
        # Records list one person more than once, inverted the second time ("Robin Becker", "Becker, Robin, M."),
        # and name organisations "LearningExpress (Organization)", "Adobe Creative Team".
        names = [a.get("name") or "" for a in rec.get("authors") or []]
        names = [flip_last_first(", ".join(n.split(",")[:2])) for n in names
                 if "(" not in n and not _CATALOGUE_BODY.search(n)]
        authors = []
        for a in _clean_authors(names):
            if not any(_same_person(a, b) for b in authors):
                authors.append(a)
        if authors and _title_agrees(found, titles):
            return {"group": "lookup", "title": None, "authors": authors, "fixed": True, "prior": False,
                    "source": f"openlibrary:{isbn}"}
    return None


def _orient(cand: dict, ctx: Context, support: set) -> dict:
    """Return the candidate as {title, authors, swapped, ambiguous, margin}."""
    if cand["group"] == "filename":
        first, second = cand.get("first"), cand.get("second")
        if not second:
            return {"title": first, "authors": [], "swapped": False, "ambiguous": False, "margin": 0.0}
        if not cand.get("ambiguous"):   # "Title by Author", "Title (Author)"
            return {"title": first, "authors": split_authors(second), "swapped": False, "ambiguous": False, "margin": 9.0}
        a_first = ctx.side(first) - ctx.side(second) + _support_bonus(first, support) - _support_bonus(second, support)
        if a_first >= 0:
            return {"title": second, "authors": split_authors(first), "swapped": False,
                    "ambiguous": a_first < FILENAME_AMBIGUOUS, "margin": a_first}
        return {"title": first, "authors": split_authors(second), "swapped": False,
                "ambiguous": -a_first < FILENAME_AMBIGUOUS, "margin": -a_first}

    title, authors = cand.get("title"), cand.get("authors") or []
    if cand["fixed"] or not title or not authors:
        return {"title": title, "authors": authors, "swapped": False, "ambiguous": False, "margin": 9.0}
    author_text = " & ".join(authors)
    # How much more the stored author looks like an author than the stored title does.
    keep = (ctx.side(author_text) - ctx.side(title)
            + _support_bonus(author_text, support) - _support_bonus(title, support))
    if keep < -CALIBRE_SWAP_MARGIN:
        # Rebuild the title from the stored author field as Calibre split it on '&'; the
        # cleaned list has commas split and "Last, First" flipped, which mangles a title.
        swapped_title = " & ".join(cand.get("raw_authors") or authors)
        return {"title": swapped_title, "authors": split_authors(title), "swapped": True, "ambiguous": False,
                "margin": -keep}
    return {"title": title, "authors": authors, "swapped": False, "ambiguous": keep < 0, "margin": keep}


SOURCE_ORDER = ["calibre_isfdb", "calibre", "publisher", "lookup", "text", "filename", "document", "folder"]


def _cluster(votes, groups, forms, similar, pick, text_of):
    """Merge votes for near-identical keys (typos, subtitles); pick() names the surviving key."""
    reps, members = [], {}
    for k in sorted(votes, key=lambda key: -votes[key]):
        rep = next((r for r in reps if similar(text_of(k), text_of(r))), None)
        if rep is None:
            reps.append(k)
            members[k] = [k]
        else:
            members[rep].append(k)
    out_votes, out_groups, out_forms = {}, {}, {}
    for r in reps:
        keep = pick(members[r])
        out_votes[keep] = sum(votes[k] for k in members[r])
        out_groups[keep] = set().union(*(groups[k] for k in members[r]))
        out_forms[keep] = forms[keep]
    return out_votes, defaultdict(set, out_groups), out_forms


def _same_person(a: str, b: str) -> bool:
    """Same author written differently: 'Damon Night' / 'Damon Knight', 'E. E. Smith' / 'Edward E. Smith'."""
    x, y = norm_name(a).split(), norm_name(b).split()
    if not x or not y:
        return False
    if x[-1] == y[-1] and x[0][:1] == y[0][:1]:
        return True
    return SequenceMatcher(None, " ".join(x), " ".join(y)).ratio() >= 0.88


_SUBTITLE = re.compile(r"\s*(?::|\s[-–—]\s|[(\[]).*$")


def _main_title(title: str) -> str:
    """Title without subtitle, edition or bracketed part: 'Subspace Survivors: The Classic…' → 'subspace survivors'."""
    return norm_title(_SUBTITLE.sub("", title or "")) or norm_title(title)


def _similar_title(a: str, b: str) -> bool:
    """Same work despite a subtitle, a '[Anthology]' tag or a small typo, but never
    across volume numbers ('Orbit 4' / 'Orbit 5') or sequels ('Dune' / 'Dune Messiah')."""
    x, y = _main_title(a), _main_title(b)
    if not x or not y or set(re.findall(r"\d+", x)) != set(re.findall(r"\d+", y)):
        return False
    tx, ty = x.split(), y.split()
    if tx == ty:
        return True
    short, long = sorted((tx, ty), key=len)
    if len(short) >= 2 and long[:len(short)] == short:      # "God Bless You, Mr. Rosewater or Pearls…"
        return True
    return SequenceMatcher(None, x, y).ratio() >= 0.9


def _similar_name(a: str, b: str) -> bool:
    """Same person despite initials, middle names or small typos ('Damon Night')."""
    x, y = norm_name(a), norm_name(b)
    if not x or not y:
        return False
    if x.split()[-1] == y.split()[-1]:
        return True
    return SequenceMatcher(None, x, y).ratio() >= 0.85


def _authors_overlap(a: list, b: list) -> bool:
    return any(_same_person(x, y) for x in a or [] for y in b or [])


def _support_bonus(text: str, support: set) -> float:
    """Independent front-matter evidence that this string is the author."""
    if not text:
        return 0.0
    parts = split_authors(text) or [text]
    return 2.5 if any(norm_name(p) in support for p in parts) else 0.0


def _year(value) -> int | None:
    if not value:
        return None
    m = re.match(r"^\s*(\d{4})", str(value))
    if not m:
        return None
    y = int(m.group(1))
    return y if 1450 <= y <= date.today().year + 1 else None


def _date(value) -> date | None:
    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})", str(value or ""))
    if m and _year(m.group(1)):
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def _author_sort(name: str) -> str:
    tokens = name.split()
    if len(tokens) < 2:
        return name
    suffix = ""
    if tokens[-1].lower().strip(".,") in {"jr", "sr", "ii", "iii", "iv"} and len(tokens) > 2:
        suffix, tokens = " " + tokens[-1], tokens[:-1]
    return f"{tokens[-1]}, {' '.join(tokens[:-1])}{suffix}"


def _dedupe(values) -> list:
    out, seen = [], set()
    for v in values:
        if not v:
            continue
        k = v.strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(v.strip())
    return out


def resolve_unit(unit: dict, ctx: Context) -> dict:
    """Decide one unit's fields. unit = {calibre, calibre_dir, directory, files, cover_path}."""
    unit = dict(unit, calibre=_fix_text(unit.get("calibre")),
                files=[dict(f, evidence=_fix_text(f.get("evidence"))) for f in unit["files"]])
    files = _pick_files(unit["files"])
    cal = unit.get("calibre") or {}
    cands, clues = _candidates(unit, files)
    lookup = _lookup_candidate(unit, files, cands, clues, ctx)
    if lookup:
        cands.append(lookup)
    # A title-page line that repeats the title ("SPELL LAW") is not a byline, unless it is plainly a
    # person ("Roger Zelazny- Three Descents").
    titles = [c.get("title") or (None if c.get("second") else c.get("first")) for c in cands]
    titles = [f" {norm_title(t)} " for t in titles if t]
    clues["bylines"] = [b for b in clues["bylines"] if name_score(b, ctx.given) >= SURE_NAME
                        or not any(f" {norm_title(b)} " in t for t in titles)]
    # Names stored by earlier readings may run into an ISBN or stop at a middle initial ("Hunter S").
    holders = [re.sub(r"\s+ISBN\b.*$", "", c["name"]) for c in clues["copyright"]
               if c.get("name") and not is_corporate(c["name"])]
    holders = [h for h in holders if h and not re.search(r"\s[A-Z]$", h) and not JUNK_AUTHOR.search(h)]
    support = {norm_name(n) for n in clues["bylines"]} | {norm_name(n) for n in holders}
    if clues.get("folder_author"):
        support.add(norm_name(clues["folder_author"]))

    oriented = []
    for cand in cands:
        o = _orient(cand, ctx, support)
        # A filename that is only an ISBN names the edition, not the title: keep it as a last resort.
        weight = GROUP_WEIGHT[cand["group"]] * (0.05 if cand.get("pattern") == "isbn" else 1.0)
        o.update(group=cand["group"], source=cand["source"], weight=weight)
        oriented.append(o)

    title_votes, author_votes = defaultdict(float), defaultdict(float)
    title_forms, author_forms = {}, {}
    title_groups, author_groups = defaultdict(set), defaultdict(set)
    for o in oriented:
        w = o["weight"] * (0.5 if o["ambiguous"] else 1.0)
        if o["title"]:
            k = norm_title(o["title"])
            if k:
                title_votes[k] += w
                title_groups[k].add(o["group"])
                # Keep the first spelling, unless it shouts or whispers and this one does not.
                old = title_forms.get(k, (None, None))[1]
                if old is None or (old.isupper() or old.islower()) and not (o["title"].isupper() or o["title"].islower()):
                    title_forms[k] = (o["group"], o["title"])
        if o["authors"]:
            k = norm_name(o["authors"][0])
            if k:
                a = o["authors"][0]
                wa = w * (HANDLE_WEIGHT if looks_like_handle(a) else 1.0)
                # The franchise, or a file-name part that is no kind of name, gives way to any real writer.
                if TIE_IN.match(a):
                    wa *= 0.3
                elif o["group"] == "filename" and name_score(a, ctx.given) <= 0:
                    wa *= 0.5
                author_votes[k] += wa
                author_groups[k].add(o["group"])
                author_forms.setdefault(k, (o["group"], o["authors"]))
    for name in clues["bylines"][:2] + holders[:2]:
        k = norm_name(name)
        if k:
            author_votes[k] += GROUP_WEIGHT["text"] / 2
            author_groups[k].add("text")
            author_forms.setdefault(k, ("text", split_authors(name) or [name]))
    if clues.get("folder_author"):
        k = norm_name(clues["folder_author"])
        author_votes[k] += GROUP_WEIGHT["folder"]
        author_groups[k].add("folder")
        author_forms.setdefault(k, ("folder", [clues["folder_author"]]))
    for tp in clues["title_pages"][:1]:
        k = norm_title(tp)
        if k and k not in author_votes:
            title_votes[k] += 1.0
            title_groups[k].add("text")
            title_forms.setdefault(k, ("text", tp.title() if tp.isupper() else tp))

    title_votes, title_groups, title_forms = _cluster(
        title_votes, title_groups, title_forms, _similar_title,
        pick=lambda ks: min(ks, key=lambda k: (SOURCE_ORDER.index(title_forms[k][0]), -title_votes[k])),
        text_of=lambda k: title_forms[k][1])
    author_votes, author_groups, author_forms = _cluster(
        author_votes, author_groups, author_forms, _same_person,
        pick=lambda ks: max(ks, key=lambda k: (ctx.author_freq.get(k, 0), -SOURCE_ORDER.index(author_forms[k][0]),
                                               author_votes[k])),
        text_of=lambda k: author_forms[k][1][0])

    def best(votes, forms, groups):
        if not votes:
            return None, None, 0.0, set()
        k = max(votes, key=lambda key: (votes[key], len(groups[key])))
        total = sum(votes.values())
        return k, forms[k][1], votes[k] / total if total else 0.0, groups[k]

    tkey, title, t_conf, t_groups = best(title_votes, title_forms, title_groups)
    akey, authors, a_conf, a_groups = best(author_votes, author_forms, author_groups)

    # Calibre is the curated record: when its orientation-checked authors agree
    # with the evidence, keep its title and author list (editors of an anthology
    # rather than every contributor, its spelling of the title). A swapped record
    # never qualifies, because its "authors" are a title.
    curated = next((o for o in oriented if o["group"].startswith("calibre") and o["title"] and o["authors"]), None)
    if curated and authors and _authors_overlap(curated["authors"], authors):
        tkey, title = norm_title(curated["title"]), curated["title"]
        akey, authors = norm_name(curated["authors"][0]), curated["authors"]
        t_groups = title_groups.get(tkey) or {curated["group"]}
        a_groups = author_groups.get(akey) or {curated["group"]}
    if tkey and akey and tkey == norm_title(" ".join(authors or [])):
        authors, akey, a_conf, a_groups = None, None, 0.0, set()
    # A catalogue record often names only the first author, or another spelling of them: when the book's
    # own record names the same person, its list stands unless the catalogue's is longer.
    if authors and author_forms.get(akey, ("",))[0] == "lookup":
        own = next((o["authors"] for o in oriented if o["group"] != "lookup" and o["authors"]
                    and len(o["authors"]) >= len(authors) and _authors_overlap(o["authors"], authors)), None)
        authors = own or authors

    # Prefer the best-capitalized spelling of the chosen title and authors.
    for o in oriented:
        if o["group"] in ("publisher", "calibre_isfdb", "calibre") and o["title"] and norm_title(o["title"]) == tkey:
            title = o["title"]
            break

    if authors and " " in authors[0] and (authors[0].islower() or authors[0].isupper()):
        k0 = norm_name(authors[0])
        forms = [o["authors"][0] for o in oriented if o["authors"] and norm_name(o["authors"][0]) == k0]
        forms += [b for b in clues["bylines"] if norm_name(b) == k0]
        well = next((f for f in forms if not f.islower() and not f.isupper()), None)
        authors = [well or recase_name(authors[0])] + list(authors[1:])
    if authors:   # "GORDON EKLUND and GREGORY BENFORD" from a title page
        authors = [recase_name(a) if " " in a and (a.isupper() or a.islower()) else a for a in authors]
    # "Fire Baby, The" → "The Fire Baby": Calibre's sort form stored as the title.
    if title:
        title = re.sub(r"^(.+?),\s+(The|A|An)$", r"\2 \1", title.lstrip("_ ")) or title

    cal_title_key = norm_title(cal.get("title")) if cal.get("title") else None
    cal_author_key = norm_name((cal.get("authors") or [None])[0]) if cal.get("authors") else None
    swapped = bool(cal_title_key and akey and cal_title_key == norm_title(" ".join(authors or []))
                   and cal_author_key and tkey and cal_author_key == norm_name(title or ""))
    swapped = swapped or any(o["swapped"] for o in oriented if o["group"].startswith("calibre")
                             and o["title"] and norm_title(o["title"]) == tkey)

    reasons = []
    deciding = [o for o in oriented if o["title"] and norm_title(o["title"]) == tkey]
    if not title:
        reasons.append("no title found")
    if deciding and all(o["ambiguous"] for o in deciding) and len(t_groups | a_groups) <= 1:
        reasons.append("title/author order uncertain")
    if swapped and len((a_groups | t_groups) - {"calibre", "calibre_isfdb"}) == 0:
        reasons.append("swap rests on name heuristics only")
    pub = next((o for o in oriented if o["group"] == "publisher"), None)
    calo = next((o for o in oriented if o["group"].startswith("calibre")), None)
    if pub and calo and pub["title"] and calo["title"] and pub["authors"] and calo["authors"] \
            and not looks_like_handle(pub["authors"][0]):
        pt, ct = pub["title"], calo["title"]
        crossed = _similar_title(pt, calo["authors"][0]) and _similar_name(pub["authors"][0], ct)
        if not crossed and not _similar_title(pt, ct) and not _authors_overlap(pub["authors"], calo["authors"]):
            reasons.append("Calibre and the file disagree")

    # Remaining fields by source precedence.
    metas = [(f.get("evidence") or {}) for f in files]
    pub_metas = [e["meta"] for e in metas if e.get("meta") and e.get("meta_group") == "publisher"]
    cal_metas = [e["meta"] for e in metas if e.get("meta") and e.get("meta_group") == "calibre"]
    any_metas = pub_metas + cal_metas + [e["meta"] for e in metas if e.get("meta") and e.get("meta_group") == "document"]
    fn = next((c for c in cands if c["group"] == "filename"), {})

    def first_of(*values):
        return next((v for v in values if v not in (None, "", [])), None)

    series = first_of(cal.get("series"), *[m.get("series") for m in pub_metas + cal_metas], fn.get("series"))
    series_index = None
    if series:
        series_index = first_of(cal.get("series_index") if cal.get("series") else None,
                                *[m.get("series_index") for m in pub_metas + cal_metas if m.get("series")],
                                fn.get("series_index"))

    tags = list(cal.get("tags") or [])
    for m in pub_metas + cal_metas:
        tags += m.get("subjects") or []
    awards, subjects = [], []
    for t in _dedupe(tags):
        found = classify_award_tag(t)
        if found:
            awards += [dict(a, raw=t) for a in found]
        elif not t.startswith("_"):
            subjects.append(t)

    cal_pub = cal.get("pubdate")
    if cal_pub and cal.get("timestamp") and str(cal.get("timestamp"))[:10] == cal_pub:
        cal_pub = None            # pubdate defaulted to the import day
    pubdate = _date(cal_pub)
    pub_year = _year(cal_pub)
    year_source = "calibre" if pub_year else None
    if not pub_year:
        for m in pub_metas:
            pub_year = _year(m.get("date"))
            if pub_year:
                pubdate, year_source = _date(m.get("date")), "publisher"
                break
    if not pub_year:
        years = [c["year"] for c in clues["copyright"] if c.get("year")]
        if years:
            pub_year, year_source = min(years), "copyright"

    identifiers = {}
    for m in any_metas:
        identifiers.update({k: v for k, v in (m.get("identifiers") or {}).items() if v})
    identifiers.update({k: v for k, v in (cal.get("identifiers") or {}).items() if v})
    isbn = first_of(normalize_isbn(identifiers.get("isbn")), *[normalize_isbn(m.get("isbn")) for m in pub_metas + cal_metas],
                    *(clues["isbns"][:1]), *(clues["filename_isbns"][:1]))
    if isbn:
        identifiers["isbn"] = isbn
    isfdb_url = None
    url = identifiers.get("url") or ""
    if "isfdb" in url:
        isfdb_url = url
    elif identifiers.get("isfdb"):
        isfdb_url = f"https://www.isfdb.org/cgi-bin/pl.cgi?{identifiers['isfdb']}"

    languages = _dedupe(cal.get("languages") or [LANG_3.get((m.get("language") or "")[:2].lower(), m.get("language"))
                                                for m in pub_metas + cal_metas if m.get("language")][:1])
    rating = (cal.get("rating") / 2.0) if cal.get("rating") else None
    description = first_of(cal.get("description"), *[m.get("description") for m in pub_metas + cal_metas])

    present = [f for f in unit["files"] if f.get("present", True)]
    formats = _dedupe([f["format"].upper() for f in present if f.get("format")])
    if not formats and not unit["files"]:
        formats = _dedupe([(x.get("format") or "").upper() for x in cal.get("formats") or []])
    cover_member = None
    if not unit.get("cover_path"):
        for f in present:
            cov = (f.get("evidence") or {}).get("cover")
            if cov and cov.get("member"):
                cover_member = f"{f['file_path']}::{cov['member']}"
                break

    first_author = (authors or [None])[0]
    work_key = hashlib.md5(f"{tkey or ''}|{norm_name(first_author) if first_author else ''}".encode()).hexdigest()[:12]
    # Hide loose units whose files are all gone, and loose archives with no ebook inside.
    not_books = [f for f in unit["files"]
                 if (f.get("evidence") or {}).get("inner_names") == [] and not (f.get("evidence") or {}).get("meta")]
    hidden = not cal and bool(unit["files"]) and (not present or len(not_books) == len(unit["files"]))

    def source_of(groups):
        return next((g for g in SOURCE_ORDER if g in groups), None)

    return {
        "title": title,
        "sort_title": norm_title(title) if title else None,
        "authors": authors or [],
        "author_sort": _author_sort(authors[0]) if authors else None,
        "series": series,
        "series_index": series_index,
        "subjects": subjects[:60],
        "publisher": first_of(cal.get("publisher"), *[m.get("publisher") for m in pub_metas + cal_metas]),
        "languages": [l for l in languages if l],
        "pubdate": pubdate,
        "pub_year": pub_year,
        "rating": rating,
        "description": description,
        "isbn": isbn,
        "identifiers": identifiers,
        "goodreads_id": identifiers.get("goodreads"),
        "isfdb_url": isfdb_url,
        "formats": formats,
        "file_count": len(present),
        "size_bytes": sum(f.get("size_bytes") or 0 for f in present),
        "available": bool(present),
        "cover_member": cover_member,
        "provenance": {
            "title": {"source": source_of(t_groups), "groups": sorted(t_groups), "confidence": round(t_conf, 2)},
            "authors": {"source": source_of(a_groups), "groups": sorted(a_groups), "confidence": round(a_conf, 2)},
            "pub_year": year_source,
            "candidates": [{k: o[k] for k in ("source", "title", "authors", "swapped", "ambiguous")}
                           | {"margin": round(o["margin"], 2)} for o in oriented],
        },
        "confidence": round(min(t_conf, a_conf) if authors else t_conf * 0.5, 2),
        "needs_review": bool(reasons),
        "review_reason": "; ".join(reasons) or None,
        "swapped": swapped,
        "hidden": hidden,
        "work_key": work_key,
        "awards": awards,
    }


# ---------------------------------------------------------------------------
# Database driver
# ---------------------------------------------------------------------------

def _load_units(cur) -> list[dict]:
    cur.execute("SELECT id, unit_key, directory, calibre_dir, calibre, cover_path FROM books")
    units = {r[0]: {"id": r[0], "unit_key": r[1], "directory": r[2], "calibre_dir": r[3],
                    "calibre": r[4], "cover_path": r[5], "files": []} for r in cur.fetchall()}
    cur.execute("SELECT book_id, file_path, format, size_bytes, present, evidence, mtime FROM book_files")
    for book_id, path, fmt, size, present, evidence, mtime in cur.fetchall():
        if book_id in units:
            units[book_id]["files"].append({"file_path": path, "format": fmt, "size_bytes": size,
                                            "present": present, "evidence": evidence, "mtime": mtime})
    return list(units.values())


def _load_lookups(cur) -> dict:
    cur.execute("SELECT to_regclass('isbn_lookups') IS NOT NULL")
    if not cur.fetchone()[0]:
        return {}
    cur.execute("SELECT isbn, response FROM isbn_lookups WHERE response IS NOT NULL")
    return dict(cur.fetchall())


def resolve():
    started = time.time()
    print("\n📚 Resolving book titles and authors")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            units = _load_units(cur)
            lookups = _load_lookups(cur)
        print(f"   {len(units):,} book units loaded", flush=True)
        ctx = build_context(units)
        ctx.lookups = lookups
        print(f"   {len(ctx.author_freq):,} distinct author names in corpus, {len(ctx.given):,} given names", flush=True)

        rows, award_rows = [], []
        swapped = review = 0
        for u in units:
            r = resolve_unit(u, ctx)
            cal = u.get("calibre") or {}
            added = cal.get("timestamp")
            if not added:
                mtimes = [f["mtime"] for f in u["files"] if f.get("mtime")]
                added = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(min(mtimes))) if mtimes else None
            swapped += r["swapped"]
            review += r["needs_review"]
            rows.append((
                r["title"], r["sort_title"], r["authors"], r["author_sort"], r["series"], r["series_index"],
                r["subjects"], r["publisher"], r["languages"], r["pubdate"], r["pub_year"], r["rating"],
                r["description"], r["isbn"], psycopg2.extras.Json(r["identifiers"]), r["goodreads_id"],
                r["isfdb_url"], r["formats"], r["file_count"], r["size_bytes"], r["available"],
                r["cover_member"], psycopg2.extras.Json(r["provenance"]), r["confidence"], r["needs_review"],
                r["review_reason"], r["swapped"], r["hidden"], r["work_key"], added, u["id"],
            ))
            for a in r["awards"]:
                award_rows.append((u["id"], a["award"], a["category"], a["year"], a["result"], a["raw"]))

        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, """
                UPDATE books SET
                    title = %s, sort_title = %s, authors = %s, author_sort = %s, series = %s, series_index = %s,
                    subjects = %s, publisher = %s, languages = %s, pubdate = %s, pub_year = %s, rating = %s,
                    description = %s, isbn = %s, identifiers = %s, goodreads_id = %s, isfdb_url = %s,
                    formats = %s, file_count = %s, size_bytes = %s, available = %s, cover_member = %s,
                    provenance = %s, confidence = %s, needs_review = %s, review_reason = %s, swapped = %s,
                    hidden = %s, work_key = %s, added_at = COALESCE(%s::timestamptz, added_at),
                    resolved_at = NOW(), updated_at = NOW()
                WHERE id = %s
            """, rows, page_size=500)
            cur.execute("DELETE FROM book_awards")
            psycopg2.extras.execute_values(cur, """
                INSERT INTO book_awards (book_id, award, category, year, result, raw) VALUES %s
                ON CONFLICT DO NOTHING
            """, award_rows, page_size=1000)
        conn.commit()
    finally:
        conn.close()
    print(f"   ✅ Resolved {len(units):,} units: {swapped:,} title/author swaps corrected, "
          f"{review:,} flagged for review, {len(award_rows):,} awards ({time.time() - started:,.0f}s)\n")


if __name__ == "__main__":
    resolve()
