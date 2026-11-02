"""
Pure text heuristics for identifying books: filename parsing, person-name
likeness, author splitting, ISBN validation and front-matter bylines.

Nothing here touches the filesystem or the database, so it is unit-tested in
tests/test_book_naming.py and re-run cheaply when the resolver is tuned.
"""
import re
import unicodedata

# File extensions the books scanner treats as ebooks, longest first so
# ".fb2.zip" wins over ".zip".
EBOOK_EXTENSIONS = (
    ".fb2.zip", ".epub", ".kepub", ".mobi", ".azw", ".azw3", ".azw4", ".kfx", ".prc", ".pdb",
    ".pdf", ".djvu", ".djv", ".fb2", ".lit", ".lrf", ".lrx", ".cbz", ".cbr", ".cb7", ".rtf",
    ".txt", ".doc", ".docx", ".odt", ".htmlz", ".chm", ".zip", ".rar", ".mbp", ".tcr", ".snb",
)

# Extensions that are only books inside a Calibre book folder; loose they are
# usually readmes, image packs or web pages.
CALIBRE_ONLY_EXTENSIONS = {".rar", ".html", ".htm", ".mbp"}

_PARTICLES = {"de", "da", "di", "del", "della", "der", "den", "van", "von", "le", "la", "du", "st", "mac", "ibn", "bin", "al", "y", "dos", "das", "ten", "ter", "vander"}
_FUNCTION_WORDS = {"the", "of", "and", "a", "an", "in", "on", "to", "for", "with", "from", "at", "is", "it",
                   "into", "over", "under", "my", "your", "our", "their", "his", "her", "its", "who", "what",
                   "when", "where", "why", "how", "not", "no", "all", "be", "are", "was", "were", "this", "that"}
_NAME_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "phd", "md"}
_TITLE_WORDS = {"book", "volume", "vol", "part", "chronicles", "saga", "trilogy", "series", "collection", "anthology",
                "stories", "tales", "complete", "edition", "omnibus", "novel", "guide", "history", "introduction",
                "handbook", "manual", "magazine", "issue", "annual", "best", "war", "world", "star", "trek", "wars"}

# Common given names. The resolver extends this with first names seen on
# high-confidence evidence, so it only needs to seed the obvious cases.
GIVEN_NAMES = set("""
aaron adam adrian alan alastair albert alec alex alexander alexandra alfred alice alison allen amanda amy andre
andrea andrew andy angela ann anna anne annie anthony arthur ashley barbara barry ben benjamin bernard beth betty
bill billy bob bobby brad bram brandon brenda brendan brett brian bruce bryan carl carol caroline carolyn catherine
cecilia charles charlie charlotte cheryl chris christian christina christine christopher cindy clare clark claude
clifford colin connie craig cynthia dale dan daniel danielle david dean deborah debra denise dennis derek diana diane
dick don donald donna doris dorothy doug douglas earl ed eddie edgar edmund edward elaine eleanor elizabeth ellen emily
emma eric erica erik ernest eugene evelyn frances frank fred frederick fritz gail gary gene geoffrey george gerald
glen glenn gordon grace graham greg gregory guy hal harlan harold harry heather helen henry herbert howard hugh ian
isaac jack jacqueline james jamie jane janet janice jason jay jean jeff jeffrey jennifer jeremy jerry jessica jim joan
joanne joe joel john jon jonathan jordan joseph josh joshua joyce judith judy julia julian julie justin karen karl kate
katherine kathleen kathryn kathy keith kelly ken kenneth kevin kim kristine larry laura lauren lawrence lee leigh leo
leonard leslie lewis linda lisa lois lori louis louise lucy lynn margaret maria marie marion mark martha martin mary
matthew maureen max megan melissa michael michelle mike mildred murray nancy natalie neal neil nicholas nick nicola
nora norman olivia orson pamela pat patricia patrick paul paula peter philip phyllis piers poul rachel ralph randall
ray raymond rebecca richard rick rita rob robert robin roger ronald rose roy ruth ryan sally samuel sandra sara sarah
scott sean sharon sheila sheri shirley simon stanley stephanie stephen steve steven stuart susan suzanne ted terry
theodore thomas tim timothy tina todd tom tony tracy ursula valerie vernor victor victoria vincent virginia walter
wayne wendy william willis zane
""".split())

_INITIAL = re.compile(r"^(?:[A-Z]\.){1,3}$|^[A-Z]$|^[A-Z]\.[A-Z]\.?$")
_DIGIT = re.compile(r"\d")
_DASHES = re.compile(r"\s+[-–—]+\s+")
_ROLE_PREFIX = re.compile(r"^(?:(?:edited|compiled|selected|translated|introduced)\s+by|eds?\.)\s+", re.I)
_ROLE_SUFFIX = re.compile(r"\s*[,(\[]\s*(?:eds?|editors?|translator|trans)\.?\s*[)\]]?\s*$", re.I)
_CORPORATE = re.compile(
    r"\b(?:publish(?:ing|ers?)|publications?|press|education|inc|llc|ltd|limited|media|corp(?:oration)?|"
    r"company|gmbh|editions|enterprises|clearance cent(?:er|re)|pictures|studios?|entertainment|books|paramount|"
    r"lucasfilm|bbc|photo ?disc|licensing agency|global|graphics|packt|apress|elsevier|mcgraw[- ]hill|"
    r"addison[- ]wesley|prentice[- ]hall)\b\.?", re.I)
# Publishers whose names are also surnames: "Wiley" and "Springer-Verlag" are the publisher, while
# "Nancy Springer", "Ridley Pearson" and "Kathleen O'Reilly" are writers.
_PUBLISHER_SURNAME = re.compile(r"\b(?:o'?reilly|wiley|springer|pearson|manning)\b", re.I)
_WRITER_SURNAMED = re.compile(r"^(?!John\s+Wiley$)(?:[A-Z][\w.'-]*\s+){1,3}(?i:o'?reilly|wiley|springer|pearson|manning)$"
                              r"|^(?i:o'?reilly|springer|pearson|manning),\s+[A-Z][\w.'-]*(?:\s+[A-Z][\w.'-]*){0,2}$")
_NAME_LIST = re.compile(r"\s*(?:&|;|\band\b)\s*")


def _publisher_named(name: str) -> bool:
    """'Wiley', 'Springer-Verlag', 'John Wiley & Sons'; not 'Bill O'Reilly & Martin Dugard' or 'O'Reilly, Bill'."""
    return any(_PUBLISHER_SURNAME.search(p) and not _WRITER_SURNAMED.match(p) for p in _NAME_LIST.split(name))

# Role-playing game lines, settings and their publishers' imprints. In "D&D 3.5 - Forge of Fury" or
# "Ars Magica - Saga Pack" the game is the series, never the author.
GAME_LINE = re.compile(
    r"^(?:A?D&D|(?:Advanced\s+)?Dungeons\s*(?:&|and)\s*Dragons|Ravenloft|Forgotten Realms|Dragonlance|Greyhawk|"
    r"Spelljammer|Planescape|Dark Sun|Mystara|Birthright|Eberron|Al-Qadim|Oriental Adventures|GURPS|Rolemaster|"
    r"MERP|Middle[- ]earth Role ?playing|Shadow World|Ars Magica|Warhammer|WFRP|Pathfinder|RMFRP|RMSS|HARP|"
    r"Spacemaster|Transhuman Space|In Nomine|Delta Green|Traveller|Rifts|RuneQuest|Pendragon|Alternity|"
    r"Mutants (?:&|and) Masterminds|d20 Modern|Kingdoms of Kalamar|Scarred Lands|Necromunda|Mordheim|"
    r"Blood Bowl|Dark Heresy|Rogue Trader|"
    r"GameMastery|Call of Cthulhu|Shadowrun|Star Wars (?:Saga|d20|RPG|Roleplaying)|World of Darkness|"
    r"(?:Vampire|Werewolf|Mage): The|Palladium|Battletech|Mechwarrior|Deadlands|Savage Worlds|13th Age|"
    r"Dungeon Crawl Classics|Iron Kingdoms|Earthdawn|Legend of the Five Rings|L5R|Gamma World|Star Frontiers|"
    r"Marvel Super Heroes|DC Heroes|Hero System|Numenera|DDAL|Adventurers League|Dungeon Magazine|"
    r"Dragon Magazine|OSRIC|Labyrinth Lord|Basic Fantasy|TSR|Wizards of the Coast|White Wolf|Games Workshop|"
    r"Steve Jackson Games|Iron Crown|Chaosium|Paizo|Atlas Games|Fantasy Flight|Green Ronin|Mongoose Publishing|"
    r"Necromancer Games|Goodman Games|Kenzer|Malhavoc|Alderac|Privateer Press|FASA|West End Games|Pinnacle|"
    r"Troll Lord|Hero Games|R\. Talsorian|Catalyst Game Labs|Pelgrane|Evil Hat|Kobold Press|Frog God|"
    r"Judges Guild)\b", re.I)
# Game publishers' product codes ("WOC 1150", "ICE 5805", "wwp 0204") and the kind of game book
# ("Campaigns", "Rules", "Supplement") label a file; they are neither its title nor its author.
# Calibre's placeholders for a missing author.
NO_AUTHOR = {"unknown", "unknown author", "author"}
# Scanners and formatters credited in a file's author field: "Scanned and PDF by ExampleUser",
# "Formatting By: x@hotmail.com", "Bracers CERT v1.01".
JUNK_AUTHOR = re.compile(r"@|\bscan(?:ned|ner)?\b|\bformatt|\bpdf\b|\bocr\b|\bproofed\b|"
                         r"\.(?:com|net|org|opd|doc|pdf)\b|\bv\d+\.\d|"
                         r"^(?:chapter|prologue|epilogue|contents|preface|foreword|acknowledg)|^ed materials?$|^e-?book$|\bcrew$|^team ling$", re.I)
# Beside a game line, a part scoring below this is more likely a two-word title ("Deep Beyond",
# "Night Below") than a person with an unfamiliar given name.
SURE_NAME = 1.5
PRODUCT_CODE = re.compile(r"^(?:TSR|WOC|WotC|WTC|ICE|WW|WWP|SJG|GW|FFG|PZO|AEG|AG|ATG|GRR|MGP|FASA|WEG|PEG|CAT|GMG|KEN|NG)"
                          r"[ -]?\d{3,5}[A-Za-z]?$", re.I)
# "ICE6503P Rolemaster Classic": a product code leading a part of the name.
_CODE_LEAD = re.compile(PRODUCT_CODE.pattern[:-1] + r"\s+(?=\S)", re.I)
GAME_BOOK_KIND = re.compile(r"^(?:campaigns?|rules?|core rules|core rulebook|supp?l?ements?|genre(?: book)?|"
                            r"adventures?|modules?|source ?books?|accessor(?:y|ies)|expansions?|scenarios?|(?:char(?:acter)? )?sheets?)$", re.I)


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def norm_text(text: str | None) -> str:
    """Lower-case, accent-free, punctuation-free form used for matching keys."""
    t = strip_accents(text or "").lower().replace("&", " and ")
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def norm_title(title: str | None) -> str:
    t = norm_text(title)
    return re.sub(r"^(?:the|a|an) ", "", t)


def flip_last_first(name: str) -> str:
    """'Asimov, Isaac' → 'Isaac Asimov'; leaves other strings alone."""
    m = re.match(r"^\s*([^,]+),\s*([^,]+?)\s*$", name or "")
    if m and len(m.group(1).split()) <= 3 and len(m.group(2).split()) <= 3:
        last, first = m.group(1), m.group(2)
        if last.lower().rstrip(".") in _NAME_SUFFIXES or first.lower().rstrip(".") in _NAME_SUFFIXES:
            return name.strip()
        return f"{first} {last}"
    return (name or "").strip()


def norm_name(name: str | None) -> str:
    return norm_text(flip_last_first(name or ""))


_TYPE_TAG = re.compile(
    r"\s*(?:[-–—:]\s*)?[\[(]\s*(?:an?\s+)?(?:anthology|collection|novel|novella|novelette|short stor(?:y|ies)|ss|"
    r"omnibus|o-\d+|unabridged|retail|e-?book)\s*[\])]\s*$|\s+[-–—]\s+(?:anthology|collection|omnibus)\s*$", re.I)


def clean_title(title: str | None) -> str | None:
    """Drop trailing work-type tags: 'Orbit 5 - [Anthology]' → 'Orbit 5', 'A Pail of Air (ss)' → 'A Pail of Air'."""
    if not title:
        return title
    prev = None
    t = re.sub(r"^also published as\s+", "", title.strip(), flags=re.I)
    while prev != t:
        prev, t = t, _TYPE_TAG.sub("", t).strip()
    return t or title


def strip_author_from_title(title: str | None, authors: list) -> str | None:
    """'Metaphysics - Aristotle' by Aristotle → 'Metaphysics' (a naming-template artefact)."""
    if not title or not authors:
        return title
    keys = {norm_name(a) for a in authors if a}
    parts = [p for p in _DASHES.split(title) if p.strip()]
    if len(parts) >= 2:
        if norm_name(parts[-1]) in keys:
            return " - ".join(parts[:-1]).strip()
        if norm_name(parts[0]) in keys:
            return " - ".join(parts[1:]).strip()
    return title


_MOJIBAKE = re.compile(r"[\u00c2-\u00f4][\u0080-\u00bf\u2018-\u203a\u20ac\u0152\u0153\u0160\u0161\u017d\u017e\u0178]")


def fix_mojibake(text):
    """Undo UTF-8 read as Latin-1/CP1252 ('JosÃ©' → 'José'); leave other text alone."""
    if not isinstance(text, str) or not _MOJIBAKE.search(text):
        return text
    for enc in ("cp1252", "latin-1"):
        try:
            return text.encode(enc).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return text


def looks_like_handle(name: str | None) -> bool:
    """Uploader or account names left in file metadata: 'casey ostrander', 'Casey.Ostrander'."""
    if not name:
        return False
    n = name.strip()
    return (not any(c.isupper() for c in n) or bool(re.search(r"[A-Za-z]\.[A-Za-z]{2,}", n) and " " not in n)
            or bool(re.search(r"[_@\d]", n)))


def is_corporate(name: str | None) -> bool:
    """Publisher or company names (and game lines) that PDF tools and file names put in the author field."""
    n = (name or "").strip()
    return bool(n and (_CORPORATE.search(n) or _publisher_named(n)
                       or GAME_LINE.match(n) or PRODUCT_CODE.match(n) or GAME_BOOK_KIND.match(n)))


def strip_role(name: str) -> str:
    """'Edited by Damon Knight' / 'Damon Knight (ed.)' → 'Damon Knight'."""
    return _ROLE_SUFFIX.sub("", _ROLE_PREFIX.sub("", name.strip())).strip()


def _is_name_list(part: str) -> bool:
    """'Matt Drance, Paul Warren' is a list; 'Le Guin, Ursula K.' is one surname-first name."""
    pieces = [p.strip() for p in part.split(",") if p.strip()]
    if len(pieces) < 2 or any(len(p.split()) < 2 for p in pieces):
        return False
    if len(pieces) == 2 and pieces[0].split()[0].lower() in _PARTICLES:
        return False
    return not any(p.lower().rstrip(".") in _NAME_SUFFIXES for p in pieces)


def split_authors(text: str | None) -> list[str]:
    """Split 'Larry Niven & Jerry Pournelle' / 'A; B' / 'A and B' / 'A B, C D' into names."""
    if not text:
        return []
    t = re.sub(r"\s+", " ", text).strip(" ,;")
    # CHM metadata with the spaces lost: "byBrian KnightandErik Veerman".
    t = re.sub(r"(?<=[a-z])and(?=[A-Z])", " and ", re.sub(r"^by(?=[A-Z])", "", t))
    # Degrees after a name are not a first name: "Edward E. Smith, Ph.D", "Lita Epstein, MBA".
    t = re.sub(r",\s*(?:Ph\.?\s?D|MBA|M\.?Sc|B\.?Sc|M\.D|F\.R\.S|F\.S\.A)\.?(?=\s*(?:[,;&]|\band\b|$))", "", t,
               flags=re.I)
    t = re.sub(r"\b(A?D)&(D)\b", "\\1\ue000\\2", t, flags=re.I)     # "D&D" and "AD&D" are one word
    parts = [p.replace("\ue000", "&") for p in re.split(r"\s*(?:&|;|\s/\s|\bwith\b)\s*", t)]
    out = []
    for part in parts:
        part = strip_role(part)
        # A credits list cut off at the end of a line: "Keith Baker, Rich Burlew, C. Robert Cargill, Michael".
        pieces = [p.strip() for p in part.split(",")]
        if len(pieces) >= 3 and len(pieces[-1].split()) == 1 and all(len(p.split()) >= 2 for p in pieces[:-1]) \
                and pieces[-1].lower().rstrip(".") not in _NAME_SUFFIXES:
            part = ", ".join(pieces[:-1])
        sub = re.split(r"\s+and\s+", part)
        if len(sub) == 2 and all(name_score(s) > 0.5 for s in sub):
            out.extend(sub)
        elif _is_name_list(part):
            out.extend(p.strip() for p in part.split(","))
        else:
            out.append(part)
    return [flip_last_first(p).strip(" ,.") if not _INITIAL.match(p.strip()) else p.strip()
            for p in out if p.strip(" ,.")]


def name_score(text: str | None, given_names: set | None = None) -> float:
    """How much a string looks like a person's name: >0 name-like, <0 title-like."""
    if not text:
        return -3.0
    given = given_names or GIVEN_NAMES
    if is_corporate(text):
        return -2.5
    s = flip_last_first(strip_role(text))
    if re.search(r"\s(?:&|and)\s", s) or ";" in s:
        parts = split_authors(s)
        if len(parts) > 1:
            return sum(name_score(p, given) for p in parts) / len(parts) + 0.3
    tokens = s.replace(",", " ").split()
    if not tokens:
        return -3.0
    score = 0.0
    n = len(tokens)
    if 2 <= n <= 4:
        score += 0.6
    elif n == 1:
        score -= 1.0
    elif n == 5:
        score -= 0.5
    else:
        score -= 1.5
    if any(_INITIAL.match(t) for t in tokens[:-1]):
        score += 1.2
    first = tokens[0].lower().strip(".")
    if first in given:
        score += 1.5
    lower_tokens = [t.lower() for t in tokens]
    if lower_tokens[0] in {"the", "a", "an"}:
        score -= 1.5
    if any(t in _FUNCTION_WORDS for t in lower_tokens[1:]) or (n > 1 and lower_tokens[0] in _FUNCTION_WORDS):
        score -= 1.5
    if any(t.strip(".:") in _TITLE_WORDS for t in lower_tokens):
        score -= 1.0
    if _DIGIT.search(s):
        score -= 2.0
    if re.search(r"[:!?()\[\]#]", s):
        score -= 1.0
    if _DASHES.search(s):         # "Metaphysics - Aristotle": a joined name, not one person
        score -= 1.5
    uncapitalized = [t for t in tokens if t[:1].islower() and t.lower() not in _PARTICLES]
    score -= 0.6 * len(uncapitalized)
    if lower_tokens[-1].strip(",") in _NAME_SUFFIXES:
        score += 0.5
    return score


# ---------------------------------------------------------------------------
# Filenames
# ---------------------------------------------------------------------------

_JUNK_BRACKET = re.compile(
    r"[\[\(\{]\s*(?:v?\d+(?:\.\d+)+|v\d+|epub|e?pub3?|mobi|pdf|azw3?|lit|rtf|txt|retail|e-?books?|scan(?:ned)?|ocr|"
    r"html?|digital|eng(?:lish)?|unabridged|kindle|illustrated|proofed|calibre|nook|sony|"
    r"(?:ebook|e-book|novel)(?:\s*-\s*[\w ]+)?|[\w ]*edition|ed\.?|\d+(?:st|nd|rd|th) ed\.?|"
    r"[\w.]+\.(?:com|org|net)|www\.[^\]\)]+|[\w ]*(?:pdf|epub|mobi)[\w ,]*)\s*[\]\)\}]",
    re.I,
)
_TRAILING_TAG = re.compile(r"\s*\[[^\[\]\s]+\]\s*$")
_YEAR_GROUP = re.compile(r"[\[\(]\s*((?:1[5-9]|20)\d{2})\s*[\]\)]")
_BOOK_PREFIX = re.compile(r"^[\[\(]\s*(?:book|bk|vol(?:ume)?|#)\.?\s*(\d+(?:\.\d+)?)\s*[\]\)]\s*", re.I)
_SERIES_PART = re.compile(
    r"^[\[\(]?\s*(?P<series>.*?\D)\s*(?:#|book|bk\.?|vol\.?|volume|no\.?)?\s*(?P<index>\d{1,3}(?:\.\d{1,2})?)\s*[\]\)]?$",
    re.I,
)
_CALIBRE_ID = re.compile(r"\s+\(\d+\)$")
# "pro-hadoop 9781430219422 46458": an ebook-site slug, its ISBN and a site id, sometimes with an
# "ipad_" prefix or a " (2)" copy number.
_SLUG_ISBN = re.compile(r"^(?:ipad_)?(?P<slug>[A-Za-z0-9]+(?:-[A-Za-z0-9]+)+)[ ._](?P<isbn>97[89]\d{10}|\d{9}[\dXx])"
                        r"(?:[ ._]\d{1,8})?(?:\s*\(\d{1,3}\))?$")
# "41674.programming-microsoft-sql-server-2008-pro-developer": the same site's id, then the slug.
_SITE_SLUG = re.compile(r"^(?:ipad_)?\d{4,6}\.(?P<slug>[a-z0-9]+(?:-[a-z0-9]+)+?)(?:-\d{4,6})?$")
_LIST_NUMBER = re.compile(r"^(\d{1,2})\s*[.)]\s+(?=\S)")
_SMALL_WORDS = {"a", "an", "and", "as", "at", "but", "by", "for", "in", "nor", "of", "on", "or", "the", "to", "vs",
                "with", "via"}


def deslug(slug: str) -> str:
    """'introduction-to-genetic-algorithms' → 'Introduction to Genetic Algorithms'."""
    words = [w for w in slug.split("-") if w]
    return " ".join(w if i and w in _SMALL_WORDS else w[:1].upper() + w[1:] for i, w in enumerate(words))


def file_stem(filename: str) -> tuple[str, str]:
    """Split 'Name.fb2.zip' into ('Name', '.fb2.zip')."""
    lower = filename.lower()
    for ext in EBOOK_EXTENSIONS:
        if lower.endswith(ext):
            return filename[: -len(ext)], ext
    if "." in filename:
        stem, ext = filename.rsplit(".", 1)
        return stem, "." + ext.lower()
    return filename, ""


def clean_stem(stem: str) -> tuple[str, int | None]:
    """Remove release junk from a filename stem. Returns (clean, year)."""
    s = stem
    if " " not in s and s.count(".") >= 2:
        s = s.replace(".", " ")
    if s.count("_") >= 1 and s.count(" ") <= 1:
        s = s.replace("_", " ")
    year = None
    m = _YEAR_GROUP.search(s)
    if m:
        year = int(m.group(1))
        s = s[: m.start()] + " " + s[m.end():]
    prev = None
    while prev != s:
        prev = s
        s = _JUNK_BRACKET.sub(" ", s)
        s = _TRAILING_TAG.sub("", s)
    s = re.sub(r"\s+", " ", s).strip(" -_.,")
    return s, year


def parse_filename(filename: str) -> dict:
    """Parse a book filename into a title/author candidate.

    'first'/'second' are the two sides of an 'A - B' name. The scanner does not
    know which side is the author; ambiguous=True leaves that to the resolver.
    """
    stem, _ext = file_stem(filename)
    out = {"first": None, "second": None, "series": None, "series_index": None,
           "year": None, "pattern": "plain", "ambiguous": False}
    m = _SLUG_ISBN.match(stem)
    if m and normalize_isbn(m.group("isbn")):
        out.update(first=deslug(m.group("slug")), isbn=normalize_isbn(m.group("isbn")), pattern="slug_isbn")
        return out
    m = _SITE_SLUG.match(stem)
    if m:
        out.update(first=deslug(m.group("slug")), pattern="slug")
        return out
    if re.fullmatch(r"[\dXx][\dXx\- ]{8,16}[\dXx]", stem) and normalize_isbn(stem):
        out.update(first=stem, isbn=normalize_isbn(stem), pattern="isbn")
        return out

    stem = _CALIBRE_ID.sub("", stem)
    s, out["year"] = clean_stem(stem)

    m = _BOOK_PREFIX.match(s) or _LIST_NUMBER.match(s)
    if m:
        out["series_index"] = float(m.group(1))
        s = s[m.end():].strip()

    m = re.match(r"^(?P<title>.+?)\s+by\s+(?P<author>[A-Z][^()\[\]]+)$", s)
    if m and name_score(m.group("author")) > 0:
        out.update(first=m.group("title").strip(), second=m.group("author").strip(), pattern="title_by_author")
        return out

    if not _DASHES.search(s):
        # "Charles L. Harness-The Alchemist": a bare hyphen after a full name.
        for m in re.finditer(r"(?<=\S)-(?=[A-Z])", s):
            left = s[:m.start()]
            if " " in left and name_score(left) >= SURE_NAME:
                out.update(first=left, second=s[m.end():], pattern="a_b", ambiguous=True)
                return out
    parts = []
    for p in (_CODE_LEAD.sub("", p.strip(), count=1) for p in _DASHES.split(s)):
        if p and (not parts or p.lower() != parts[-1].lower()):   # "Title - Author - Author"
            parts.append(p)
    labels = [p for p in parts if PRODUCT_CODE.match(p) or GAME_BOOK_KIND.match(p) or p.lower() in NO_AUTHOR]
    if labels and len(labels) < len(parts):
        parts = [p for p in parts if p not in labels]
        if len(parts) == 1:
            out["first"] = parts[0]
            return out
    # "3 - The Mammoth Hunters - Jean M. Auel": a bare number is the series index. In
    # "War of the Spider Queen - 1 - Dissolution - R. A. Salvatore" the part before it is the series;
    # with three parts ("Gentry Lee - 01 - Bright Messengers") the other two are left to orientation.
    num = next((i for i, p in enumerate(parts) if re.fullmatch(r"\d{1,3}", p)), None)
    if num is not None and len(parts) >= 3:
        out["series_index"] = float(parts[num])
        if num > 0 and len(parts) >= 4 and name_score(parts[num - 1]) < SURE_NAME:
            out["series"] = parts[num - 1]
            parts = parts[:num - 1] + parts[num + 1:]
        else:
            parts = parts[:num] + parts[num + 1:]
        if len(parts) == 1:
            out["first"] = parts[0]
            return out
    if len(parts) >= 3:
        def is_series_part(i, p):
            bracketed = p.startswith(("[", "("))
            middle = 0 < i < len(parts) - 1
            return (bracketed or middle) and bool(_SERIES_PART.match(p))

        series_i = next((i for i, p in enumerate(parts) if is_series_part(i, p)), None)
        # "Kushiel 04 - Kushiel's Scion - Jacqueline Carey": a numbered series leading the name.
        lead = _SERIES_PART.match(parts[0])
        if series_i is None and lead and float(lead.group("index")) < 100 and len(lead.group("series").strip()) > 2:
            series_i = 0
        if series_i is None:
            games = [p for p in parts if GAME_LINE.match(p)]
            rest = [p for p in parts if not GAME_LINE.match(p)]
            if games and len(rest) == 1 and name_score(rest[0]) < SURE_NAME:
                # "Rolemaster - Shadow World - Nomads of the Nine Nations": the setting is the series.
                out.update(first=rest[0], series=games[-1], pattern="game_title")
                return out
            if games and rest:
                # "Forgotten Realms - Homeland - R. A. Salvatore": the setting is the series.
                out.update(first=rest[0], second=" - ".join(rest[1:]) or None, series=games[-1],
                           pattern="game_a_b", ambiguous=len(rest) > 1)
                return out
        if series_i is not None:
            sm = _SERIES_PART.match(parts[series_i])
            out["series"] = sm.group("series").strip(" [](#-,")
            out["series_index"] = float(sm.group("index"))
            rest = parts[:series_i] + parts[series_i + 1:]
            if len(rest) >= 2:
                out.update(first=rest[0], second=" - ".join(rest[1:]), pattern="a_series_b", ambiguous=True)
                return out
            out.update(first=rest[0] if rest else s, pattern="series_title")
            return out
        out.update(first=parts[0], second=" - ".join(parts[1:]), pattern="a_b_c", ambiguous=True)
        return out
    if len(parts) == 2:
        game = next((i for i, p in enumerate(parts) if GAME_LINE.match(p)), None)
        if game is not None and name_score(parts[1 - game]) < SURE_NAME:
            # "D&D 3.0 Level 3 Adventure - A Tale Manifesting": the game is the series, the rest the title.
            out.update(first=parts[1 - game], series=parts[game], pattern="game_title")
            return out
        out.update(first=parts[0], second=parts[1], pattern="a_b", ambiguous=True)
        return out

    m = re.match(r"^(?P<title>.+?)\s*[\(\[](?P<author>[^()\[\]]+)[\)\]]$", s)
    if m and name_score(m.group("author")) > 1:
        out.update(first=m.group("title").strip(), second=m.group("author").strip(), pattern="title_paren_author")
        return out

    out["first"] = s or stem
    return out


# ---------------------------------------------------------------------------
# ISBNs
# ---------------------------------------------------------------------------

def _isbn10_ok(d: str) -> bool:
    if not re.fullmatch(r"\d{9}[\dX]", d):
        return False
    total = sum((10 - i) * (10 if c == "X" else int(c)) for i, c in enumerate(d))
    return total % 11 == 0


def _isbn13_ok(d: str) -> bool:
    if not re.fullmatch(r"97[89]\d{10}", d):
        return False
    total = sum((1 if i % 2 == 0 else 3) * int(c) for i, c in enumerate(d[:12]))
    return (10 - total % 10) % 10 == int(d[12])


def isbn10_to_13(d: str) -> str:
    core = "978" + d[:9]
    total = sum((1 if i % 2 == 0 else 3) * int(c) for i, c in enumerate(core))
    return core + str((10 - total % 10) % 10)


def normalize_isbn(text: str | None) -> str | None:
    """Return a checksum-valid ISBN-13 for any ISBN-10/13 spelling, else None."""
    if not text:
        return None
    d = re.sub(r"[^\dXx]", "", str(text)).upper()
    if len(d) == 13 and _isbn13_ok(d):
        return d
    if len(d) == 10 and _isbn10_ok(d):
        return isbn10_to_13(d)
    return None


# Unlabelled ISBNs may only use hyphens as separators, so neighbouring numbers
# ("1990 0-441-...") are not glued together; spaces are allowed after a label.
_ISBN_BARE = re.compile(r"(?<![\d\-])(\d[\d\-]{8,16}[\dXx])(?![\dXx\-])")
_ISBN_LABELLED = re.compile(r"ISBN(?:-1[03])?[:\s]*([\dXx](?:[\- ]?[\dXx]){9,12})", re.I)


def find_isbns(text: str | None) -> list[dict]:
    """Checksum-valid ISBNs in text, labelled ones ('ISBN 0-441-...') first."""
    if not text:
        return []
    found = {}
    for labelled, pattern in ((True, _ISBN_LABELLED), (False, _ISBN_BARE)):
        for m in pattern.finditer(text):
            isbn = normalize_isbn(m.group(1))
            if isbn and isbn not in found:
                found[isbn] = {"isbn": isbn, "labelled": labelled, "pos": m.start()}
    return sorted(found.values(), key=lambda f: (not f["labelled"], f["pos"]))


# ---------------------------------------------------------------------------
# Front-matter text
# ---------------------------------------------------------------------------

_COPYRIGHT = re.compile(
    r"copyright(?!ed\b)\s*(?:©|\(c\)|&copy;)?\s*(?P<year>(?:1[5-9]|20)\d{2})?[,\s]*(?:by\s+)?"
    r"(?P<name>[A-Z][A-Za-z.'\- ]{2,60}?)(?:(?<!\b[A-Z])(?=\s*\.)|(?=\s*(?:,|;|\n|$|all rights|\d)|\s+isbn))",
    re.I,
)

# "THE EMPIRE OF GLASS by ANDY LANE" on one title-page line.
_TITLE_BY_NAME = re.compile(r"^(?P<title>.{2,80}?)\s+by\s+(?P<name>[A-Z][^,;:()\[\]]{2,60})$", re.I)
_NOT_A_TITLE = re.compile(r"^(?:copyright|written|translated|edited|published|illustrated|illustrations|read|cover|"
                          r"introduction|foreword|preface|afterword|figures|maps|photographs|photos|drawings|art|"
                          r"artwork|design(?:ed)?|composed|produced|printed|jacket|interior|typeset(?:ting)?|layout|"
                          r"images?|graphics|music|lyrics|narrated)\b", re.I)


_NOT_THE_BOOKS = re.compile(r"\b(?:maps?|illustrations?|cover|photo(?:graph)?s?|art(?:work)?|drawings?|jacket|"
                            r"design|excerpts?|introduction|foreword|afterword|translation|music|lyrics)\b[^.]*$", re.I)


# Game and technical books credit their writers instead of a "by" line:
# "Design: Bruce R. Cordell", "Written by Jeff Grubb and Kate Novak", "Authors: ...", "Lead Designers: ...".
_CREDIT_ROLE = r"(?:written by|writ(?:ing|ten|ers?)|authors?|(?:game |lead |original )?design(?:ers?)?|created by)"
_CREDIT = re.compile(r"^" + _CREDIT_ROLE + r"\s*(?:[:\-–]\s*|\s+)(?P<names>[A-Z].{2,160})$", re.I)
_CREDIT_LABEL = re.compile(r"^[A-Za-z][A-Za-z &/]{1,30}:$")
# "Design:", "D&D Lead Designers:"; never "Graphic Designers:", even with "Graphic" on the line above.
_CREDIT_ROLE_LABEL = re.compile(r"^(?!graphic\b)(?:\S+ )?" + _CREDIT_ROLE + r":$", re.I)
_NOT_A_WRITER = re.compile(r"^(?:graphic|art|cover|interior|layout|web|production)$", re.I)


def _credit_columns(lines: list[str]) -> list[str]:
    """Credits set as two columns come out as all the roles, then all the names:
    "Design:", "Editing:", "Typesetting:", "Eric Cagle", "Penny Williams", "Nancy Walker".
    Pair them up and return the writers' and designers' names."""
    names = []
    i = 0
    while i < len(lines):
        j = i
        while j < len(lines) and _CREDIT_LABEL.match(lines[j]):
            j += 1
        if j - i >= 3 and j + (j - i) <= len(lines):
            for label, value in zip(lines[i:j], lines[j:j + (j - i)]):
                if _CREDIT_ROLE_LABEL.match(label) and not _CREDIT_LABEL.match(value):
                    names += [_unshout(p) for p in split_authors(value)[:4]]
            i = j + (j - i)
        else:
            i = j + 1
    return names


def _title_like(text: str) -> bool:
    """Title Case or capitals, the way a title page sets a title (not a sentence)."""
    words = [w for w in re.findall(r"[A-Za-z']+", text) if len(w) > 3]
    return bool(words) and sum(w[0].isupper() for w in words) >= 0.75 * len(words)


# More bylines than this are a magazine's or anthology's contributors, not the book's authors.
MAX_BYLINES = 4


def recase_name(name: str) -> str:
    """'hunter s thompson' → 'Hunter S Thompson', 'J.R.R. TOLKIEN' → 'J.R.R. Tolkien', "O'BRIEN" → "O'Brien".
    Words already in mixed case are kept."""
    def fix(word):
        if not (word.islower() or word.isupper()):
            return word
        return re.sub(r"(^|[.\-'])([a-z])", lambda m: m.group(1) + m.group(2).upper(), word.lower())
    return " ".join(fix(w) for w in name.split())


def _unshout(name: str) -> str:
    """'DEREK PEARCY' → 'Derek Pearcy', 'S. JOHN Ross' → 'S. John Ross'; mixed-case words are kept."""
    return " ".join(recase_name(w) if w.isupper() and sum(c.isalpha() for c in w) > 1 else w for w in name.split())


def front_matter_clues(text: str | None) -> dict:
    """Title-page and copyright-page clues from the start of a book's text.

    Returns {"bylines": [names], "copyright": [{"name", "year"}], "title_page": title or None}, plus
    "contributors": True when the bylines were dropped as a magazine's contributors.
    """
    clues = {"bylines": [], "copyright": [], "title_page": None}
    if not text:
        return clues
    credited = False
    lines = [re.sub(r"\s+", " ", l).strip() for l in text.splitlines()]
    lines = [l for l in lines if l]
    for i, line in enumerate(lines[:120]):
        name = None
        if line.lower() == "by" and i + 1 < len(lines):
            name = lines[i + 1]
        else:
            m = re.match(r"^by\s+(.{3,80})$", line, re.I)
            if m:
                name = m.group(1)
            elif i < 60:
                m = _TITLE_BY_NAME.match(line)
                if m and _title_like(m.group("title")) and not _NOT_A_TITLE.match(m.group("title")) \
                        and name_score(m.group("name")) > 0.5:
                    name = m.group("name")
                    if clues["title_page"] is None:
                        clues["title_page"] = m.group("title").strip(" .")
        credit = _CREDIT.match(line) if not name and i < 120 else None
        names = credit.group("names") if credit else None
        # "Design:" on a line of its own, its names on the next.
        if not name and _CREDIT_ROLE_LABEL.match(line) and i + 1 < len(lines) and not _CREDIT_LABEL.match(lines[i + 1]) \
                and not (i and _NOT_A_WRITER.match(lines[i - 1])):
            names, credited = lines[i + 1], True
        if names:
            credited = True
            for person in split_authors(names)[:4]:
                person = _unshout(person)
                if name_score(person) > 0.5 and not is_corporate(person) and person not in clues["bylines"]:
                    clues["bylines"].append(person)
        if name and name_score(name) > 0.5 and _unshout(name.strip(" .")) not in clues["bylines"]:
            clues["bylines"].append(_unshout(name.strip(" .")))
            if clues["title_page"] is None and i > 0:
                prev = lines[i - 1]
                if 1 <= len(prev) <= 90 and name_score(prev) < 0.5 and not prev.lower().startswith(("copyright", "isbn")):
                    clues["title_page"] = prev.strip(" .")
    for person in _credit_columns(lines[:120]):
        credited = True
        if name_score(person) > 0.5 and not is_corporate(person) and person not in clues["bylines"]:
            clues["bylines"].append(person)
    # Writers named in a credits block: stronger than any other name on the page.
    clues["credited"] = credited
    if len(clues["bylines"]) > MAX_BYLINES:
        clues["bylines"], clues["contributors"] = [], True
    for m in _COPYRIGHT.finditer(text[:20000]):
        name = m.group("name").strip(" .")
        before = text[max(0, m.start() - 60):m.start()].rsplit("\n", 1)[-1]
        if _NOT_THE_BOOKS.search(before):
            continue
        if name_score(name) > 0.5:
            clues["copyright"].append({"name": name, "year": int(m.group("year")) if m.group("year") else None})
    return clues


def html_to_text(html: str | None) -> str:
    """Crude HTML → text that keeps paragraph breaks (for descriptions and spines)."""
    if not html:
        return ""
    import html as _html
    t = re.sub(r"(?is)<(script|style|head)[^>]*>.*?</\1>", " ", html)
    t = re.sub(r"(?i)<br\s*/?>|</(p|div|h\d|li|tr)>", "\n", t)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    t = _html.unescape(t)
    t = re.sub(r"[ \t\r\f\v]+", " ", t)
    t = re.sub(r"\n\s*\n+", "\n\n", t)
    return t.strip()
