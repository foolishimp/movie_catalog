"""
Classify award tags into structured awards.

Calibre libraries carry awards as free-form tags in several dialects:

    ISFDB plugin     "Win of 1976 Hugo Award for Best Novel", "3 of 1985 Locus Award for Best SF Novel",
                     "Preliminary Nominees of 2008 BSFA Award for Best Short Fiction"
    Award plugin     "hugo award for best novel winner", "philip k dick award finalist"
    Hand tagged      "Hugo Award Winner", "2009 Nebula Award Nominee", "Hugo 1992 Winner",
                     "Newbery Medal", "honorable mention dozois years best"

classify_award_tag() maps one tag to zero or more {award, category, year, result}
dicts. result is one of RESULTS, or None when the tag names an award without
saying how the book placed. Subject headings such as "Lottery winners" or
"Nobel Prize winners" are not awards and return [].
"""
import re

WINNER, NOMINEE, LONGLIST = "winner", "nominee", "longlist"
RESULTS = (WINNER, NOMINEE, LONGLIST)

# Normalized-name pattern → canonical award name. First match wins, so more
# specific names come before ones they contain.
AWARD_ALIASES = [
    (r"\bhugo\b", "Hugo Award"),
    (r"\bnebula\b", "Nebula Award"),
    (r"\b(?:bsfa|british science fiction)\b", "BSFA Award"),
    (r"\b(?:bfa|british fantasy)\b|\baugust derleth\b", "British Fantasy Award"),
    (r"\bstoker\b", "Bram Stoker Award"),
    (r"\blocus\b", "Locus Award"),
    (r"\bworld fantasy\b|\bwfa\b", "World Fantasy Award"),
    (r"\bphilip k dick\b|\bpkd\b|^dick award\b", "Philip K. Dick Award"),
    (r"\bclarke\b", "Arthur C. Clarke Award"),
    (r"\bcampbell memorial\b", "John W. Campbell Memorial Award"),
    (r"\bcampbell\b", "John W. Campbell Award"),
    (r"\btiptree\b", "James Tiptree Jr. Award"),
    (r"\bprometheus\b", "Prometheus Award"),
    (r"\bsidewise\b", "Sidewise Award"),
    (r"\bendeavour\b", "Endeavour Award"),
    (r"\bshirley jackson\b", "Shirley Jackson Award"),
    (r"\binternational horror guild\b|^ihg\b", "International Horror Guild Award"),
    (r"\bandre norton\b|^norton award\b", "Andre Norton Award"),
    (r"\bbfs\b", "British Fantasy Award"),
    (r"\bcompton crook\b|^crook award\b", "Compton Crook Award"),
    (r"\bkurd la(?:ß|ss)witz\b", "Kurd Laßwitz Award"),
    (r"\bnewbery\b", "Newbery Medal"),
    (r"\bdozois\b", "Dozois Year's Best"),
    (r"\bdatlow\b", "Datlow/Windling Year's Best"),
    (r"\bmythopoeic\b", "Mythopoeic Award"),
    (r"\bsturgeon\b", "Theodore Sturgeon Award"),
    (r"\bedgar\b", "Edgar Award"),
    (r"\bbooker\b", "Booker Prize"),
    (r"\bpulitzer\b", "Pulitzer Prize"),
    (r"\bditmar\b", "Ditmar Award"),
    (r"\baurealis\b", "Aurealis Award"),
    (r"\bseiun\b", "Seiun Award"),
    (r"\bsunburst\b", "Sunburst Award"),
    (r"\baurora\b", "Aurora Award"),
    (r"\bgemmell\b", "David Gemmell Legend Award"),
    (r"\bhemming\b", "Norma K. Hemming Award"),
    (r"\banalog\b", "Analog Award"),
    (r"\basimovs?\b", "Asimov's Readers' Award"),
]
_ALIASES = [(re.compile(p), name) for p, name in AWARD_ALIASES]

_AWARD_WORD = re.compile(r"\b(?:awards?|prizes?|medals?|preis)\b", re.I)

# "Win of 1976 Hugo Award for Best Novel", "Nomination for 2000 HOMer for Short Story",
# "#7 for 1952 Analog Award for All-Time Best Book"
_ISFDB = re.compile(r"^(?P<res>.+?) (?:of|for) (?P<year>\d{4}) (?P<name>.+?)(?: for (?P<cat>.+))?$", re.I)
_ISFDB_RESULTS = {
    "win": WINNER,
    "nomination": NOMINEE,
    "finalists": NOMINEE,
    "finalist": NOMINEE,
    "made first ballot": LONGLIST,
    "preliminary nominees": LONGLIST,
    "nominations below cutoff": LONGLIST,
    "honorable mentions": LONGLIST,
    "honourable mentions": LONGLIST,
}

# "hugo award for best novel winner", "2009 Nebula Award Nominee", "Hugo Award Winner"
_PLUGIN = re.compile(
    r"^(?:(?P<year>\d{4}) )?(?P<name>.+?\b(?:awards?|prizes?|medals?))"
    r"(?: for (?P<cat>.+?))? (?P<res>winner|finalist|nominee|nominated|nomination)$",
    re.I,
)
# "Hugo 1992 Winner", "Hugo 2006 Nominee Novel", "Clarke 1993 Nominated"
_SHORT_YEAR = re.compile(r"^(?P<name>[a-z][\w.' -]*?) (?P<year>\d{4}) (?P<res>winner|nominee|nominated)\b ?(?P<cat>.*)$", re.I)
# "Locus Best Collection Winner 1985"
_SHORT_TRAIL = re.compile(r"^(?P<name>\w+) (?P<cat>.+?) (?P<res>winner|nominee) (?P<year>\d{4})$", re.I)
# "Hugo Winner", "august derleth nominee"
_SHORT_BARE = re.compile(r"^(?P<name>[a-z][\w.' -]*?) (?P<res>winner|nominee)$", re.I)
# "honorable mention dozois years best"
_HONORABLE = re.compile(r"^honou?rable mentions? (?P<name>.+)$", re.I)

_PLAIN_RESULTS = {
    "winner": WINNER, "finalist": NOMINEE, "nominee": NOMINEE,
    "nominated": NOMINEE, "nomination": NOMINEE, "nom": NOMINEE,
}


def _norm(text: str) -> str:
    text = text.lower().replace("’", "'")
    text = re.sub(r"[.\-_/]", " ", text)
    text = text.replace("'", "")
    return re.sub(r"\s+", " ", text).strip()


def canonical_award(name: str) -> str | None:
    """Canonical award name for an alias, or None if it is not a known award."""
    n = _norm(name)
    for pattern, canonical in _ALIASES:
        if pattern.search(n):
            return canonical
    return None


def _fallback_award(name: str) -> str:
    words = re.sub(r"\s+", " ", name).strip(" .-")
    titled = " ".join(w if w.isupper() or any(c.isupper() for c in w[1:]) else w.capitalize() for w in words.split())
    return re.sub(r"\bAwards\b", "Award", titled)


def normalize_category(cat: str | None) -> str | None:
    if not cat:
        return None
    c = re.sub(r"\s+", " ", cat).strip(" .-")
    c = re.sub(r"^(?:best|(?:for )?superior achievement in an?|for)\s+", "", c, flags=re.I)
    if not c:
        return None
    return " ".join(_title_word(w) for w in c.split())


def _title_word(word: str) -> str:
    """Upper-case the first letter, keep the rest ('(best' → '(Best', 'SF' → 'SF')."""
    for i, ch in enumerate(word):
        if ch.isalpha():
            return word[:i] + ch.upper() + word[i + 1:]
    return word


def _split_names(name: str) -> list[str]:
    """'hugo and nebula award' → ['hugo award', 'nebula award']."""
    m = re.match(r"^(.+?) and (.+?) ((?:awards?|prizes?|medals?))$", name.strip(), re.I)
    if m:
        return [f"{m.group(1)} {m.group(3)}", f"{m.group(2)} {m.group(3)}"]
    return [name]


def _award(name: str, category, year, result, require_alias: bool) -> dict | None:
    canonical = canonical_award(name)
    if canonical is None:
        if require_alias or not _AWARD_WORD.search(name):
            return None
        canonical = _fallback_award(name)
    if canonical == "Newbery Medal" and result is None:
        result = WINNER
    return {
        "award": canonical,
        "category": normalize_category(category),
        "year": int(year) if year else None,
        "result": result,
    }


def classify_award_tag(tag: str) -> list[dict]:
    """Return the awards a tag records (usually zero or one)."""
    if not tag:
        return []
    t = re.sub(r"\s+", " ", tag.replace("\\", "")).strip().strip('"').strip()
    t = re.sub(r"^acclaimed\.", "", t, flags=re.I)

    m = _ISFDB.match(t)
    if m:
        res_raw = m.group("res").strip().lower()
        if res_raw in _ISFDB_RESULTS:
            result = _ISFDB_RESULTS[res_raw]
        elif res_raw.isdigit():
            result = WINNER if res_raw == "1" else NOMINEE
        elif re.fullmatch(r"#\d+", res_raw):
            result = LONGLIST
        else:
            result = "unknown"
        if result != "unknown":
            a = _award(m.group("name"), m.group("cat"), m.group("year"), result, require_alias=False)
            return [a] if a else []

    m = _PLUGIN.match(t)
    if m:
        result = _PLAIN_RESULTS[m.group("res").lower()]
        out = []
        for name in _split_names(m.group("name")):
            a = _award(name, m.group("cat"), m.group("year"), result, require_alias=False)
            if a:
                out.append(a)
        return out

    for pattern in (_SHORT_YEAR, _SHORT_TRAIL, _SHORT_BARE):
        m = pattern.match(t)
        if m:
            groups = m.groupdict()
            a = _award(groups["name"], groups.get("cat"), groups.get("year"),
                       _PLAIN_RESULTS[groups["res"].lower()], require_alias=True)
            return [a] if a else []

    # "locus award for best sf novel" — names an award and category, not a result.
    m = re.match(r"^(?P<name>.+?\b(?:awards?|prizes?|medals?)) for (?P<cat>.+)$", t, re.I)
    if m:
        a = _award(m.group("name"), m.group("cat"), None, None, require_alias=True)
        return [a] if a else []

    m = _HONORABLE.match(t)
    if m:
        a = _award(m.group("name"), None, None, LONGLIST, require_alias=True)
        return [a] if a else []

    # "Kurd Laßwitz-Preis Award: Best Foreign Novel", "Acclaimed.World Fantasy Award.Nom"
    if ":" in t:
        name, cat = t.split(":", 1)
        a = _award(name, cat, None, None, require_alias=True)
        return [a] if a else []
    parts = t.rsplit(".", 1)
    if len(parts) == 2 and parts[1].lower() in _PLAIN_RESULTS:
        a = _award(parts[0], None, None, _PLAIN_RESULTS[parts[1].lower()], require_alias=True)
        return [a] if a else []

    # A bare known award name ("Hugo Award", "Newbery Medal", "1995 Prometheus Award").
    # Plural subject headings ("Pulitzer Prizes", "award winners") are not awards.
    if re.search(r"\b(?:winners|prizes|awards)$", t, re.I) and not re.search(r"\b\w+ awards$", t, re.I):
        return []
    m = re.match(r"^(?:(?P<year>\d{4}) )?(?P<name>.+)$", t)
    name = m.group("name")
    if _AWARD_WORD.search(name) and len(name.split()) <= 5:
        a = _award(name, None, m.group("year"), None, require_alias=True)
        return [a] if a else []
    return []


def is_award_tag(tag: str) -> bool:
    return bool(classify_award_tag(tag))
