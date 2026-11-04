"""Fuzzy-match unfound books against files in folders named after their authors.

Catches spelling variants (Colored/Coloured), series prefixes ('Exiles 1 - '), and punctuation
that exact title|author keys miss. Each file is credited to at most one record.
"""
import json, os, re, sys
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT)
from books.naming import EBOOK_EXTENSIONS, file_stem, norm_name, norm_title

DATA = os.path.join(ROOT, "tools", "data", "lost")
os.makedirs(DATA, exist_ok=True)
EXTS = set(EBOOK_EXTENSIONS) | {".zip", ".rar", ".7z", ".pdf", ".txt", ".doc", ".rtf", ".lit"}
STOP = {"the", "a", "an", "of", "and"}
SPELLING = [(re.compile(r"our(ed|s|ing|ful)?\b"), r"or\1"), (re.compile(r"\bgrey\b"), "gray"),
            (re.compile(r"tre\b"), "ter"), (re.compile(r"ise(d|s)?\b"), r"ize\1")]


def tokens(text, drop=()):
    t = norm_title(text or "")
    for rx, rep in SPELLING:
        t = rx.sub(rep, t)
    return [w for w in t.split() if w not in STOP and w not in drop]


missing = json.load(open(os.path.join(DATA, "missing.json")))
found = {r["id"] for r in json.load(open(os.path.join(DATA, "recovered.json")))}   # Calibre id+library
found_keys = {(r["library"], r["id"]) for r in json.load(open(os.path.join(DATA, "recovered.json")))}
unfound = [(i, r) for i, r in enumerate(missing) if (r["library"], r["id"]) not in found_keys]
by_author = {}
for i, r in unfound:
    for a in r["authors"][:1]:
        by_author.setdefault(norm_name(a), []).append(i)

folders = []
for name in ("found.jsonl", "found_local.jsonl"):
    for line in open(os.path.join(DATA, name)):
        r = json.loads(line)
        if r["kind"] == "author-folder" and norm_name(os.path.basename(r["path"])) in by_author:
            folders.append(r["path"])
folders = sorted(set(folders))


LISTING_CACHE = os.path.join(DATA, "author_folder_listing.json")
cache = json.load(open(LISTING_CACHE)) if os.path.exists(LISTING_CACHE) else {}


def files_under(folder):
    if folder in cache:
        return cache[folder]
    out = []
    try:
        entries = list(os.scandir(folder))
    except OSError:
        return out
    for e in entries:
        try:
            if e.is_dir(follow_symlinks=False):
                for sub in os.scandir(e.path):
                    if sub.is_file(follow_symlinks=False):
                        out.append(sub.path)
            elif e.is_file(follow_symlinks=False):
                out.append(e.path)
        except OSError:
            continue
    cache[folder] = out
    return out


DIGITS = re.compile(r"\d+")


def score(rec_tokens, whole, last):
    """Match a record title against a file's whole stem and its book-title part (last ' - ' segment).

    The record must cover the book-title part, so 'Corum 3 - The King of the Swords' does not match
    'The Swords of Corum', and numbers must agree, so 'Cantata 141' does not match 'Cantata-140'.
    """
    if not rec_tokens or not whole:
        return 0.0
    if rec_tokens in (whole, last):
        return 1.0
    rs = set(rec_tokens)
    if len(rs) >= 2 and rs <= set(whole) and len(set(whole) - rs) <= 3 and last and set(last) <= rs:
        return 0.95
    digits = lambda ts: {int(d) for t in ts for d in DIGITS.findall(t)}
    best = 0.0
    for ft in (whole, last):
        if ft and digits(ft) == digits(rec_tokens):
            best = max(best, SequenceMatcher(None, " ".join(rec_tokens), " ".join(ft)).ratio())
    return best


def work(folder):
    author_key = norm_name(os.path.basename(folder))
    author_words = set(author_key.split())
    recs = [(i, tokens(missing[i]["title"])) for i in by_author.get(author_key, [])]
    hits = []
    for path in files_under(folder):
        stem, ext = file_stem(os.path.basename(path))
        if ext.lower() not in EXTS:
            continue
        # The file's own title: the stem (and its parent folder name for Calibre-style dirs) minus the author's name.
        candidates = [stem, re.sub(r"\s*\(\d+\)$", "", os.path.basename(os.path.dirname(path)))]
        best = (0.0, None)
        for cand in candidates:
            parts = [p for p in re.split(r"\s+-\s+", cand) if p.strip()]
            parts = [p for p in parts if not set(tokens(p)) <= author_words] or parts
            whole = tokens(" ".join(parts), drop=author_words)
            last = tokens(parts[-1], drop=author_words) if parts else []
            for i, rt in recs:
                s = score(rt, whole, last)
                if s > best[0]:
                    best = (s, i)
        if best[0] >= 0.9:
            hits.append({"kind": "fuzzy-author-folder", "path": path, "ids": [best[1]], "score": round(best[0], 3),
                         "root": folder})
    return hits


with ThreadPoolExecutor(12) as pool:
    results = list(pool.map(work, folders))
with open(os.path.join(DATA, "fuzzy_matches.jsonl"), "w") as out:
    for hits in results:
        for h in hits:
            out.write(json.dumps(h) + "\n")
json.dump(cache, open(LISTING_CACHE, "w"))
ids = {h["ids"][0] for hits in results for h in hits}
print(f"{len(folders)} author folders checked; {sum(map(len, results))} files matched to {len(ids)} more books")
