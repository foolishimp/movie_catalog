"""
Read the first pages of books that still lack an author or a confident title,
for title-page clues the regular scan cannot get:

- PDFs: text of the first pages (PyMuPDF), with the lines set in the largest
  type, and OCR (poppler's pdftoppm + tesseract) when a PDF has no text layer;
- HTML, TXT, RTF and DOC: their opening text;
- formats the scan cannot read itself (LRF, CHM, LIT, PDB, damaged MOBI/AZW...):
  Calibre's ebook-meta for embedded metadata and ebook-convert for the text.

An opening line, or a large-type line, that is exactly a known author's name (a
person with two or more books in the library) counts as a byline, next to the
usual "by ..." and copyright lines. Failing those, the first line that reads as
a person's full name (strongly: given name and surname, no digits, no publisher
words) is taken, which is how technical books set their title pages. These two
weaker kinds of byline only bring in a new author for a book that has none;
for a book with an author already, they count only when they confirm a name in
its current title or author (deciding which side of "A - B" is the author), so
an epigraph's poet never displaces the novelist. Titles
come only from title-page patterns: large type alone splits titles and picks up
publisher series names. The
clues are merged into each file's stored evidence (evidence["text"], and
evidence["meta"] when the file had none) and the file is marked
evidence["frontpages"] so it is read once; the resolver then runs as usual.

Usage:
    python -m books.frontpages                   # read, then resolve
    python -m books.frontpages --limit 40 --dry-run
    python -m books.frontpages --reread-credits      # game-book PDFs again, for two-column credits
"""
import argparse
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2.extras
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn, query
from books.formats import _read_text_file, sanitize_json, split_member
from books.naming import find_isbns, front_matter_clues, html_to_text, is_corporate, name_score, norm_name

EBOOK_META = (os.environ.get("EBOOK_META") or shutil.which("ebook-meta")
              or "/Applications/calibre.app/Contents/MacOS/ebook-meta")
EBOOK_CONVERT = (os.environ.get("EBOOK_CONVERT") or shutil.which("ebook-convert")
                 or "/Applications/calibre.app/Contents/MacOS/ebook-convert")
PDFTOPPM = shutil.which("pdftoppm") or "/opt/homebrew/bin/pdftoppm"
TESSERACT = shutil.which("tesseract") or "/opt/homebrew/bin/tesseract"
TEXTUTIL = shutil.which("textutil") or "/usr/bin/textutil"
RPG_DIRS = "%/" + os.getenv("GAME_BOOK_FOLDER", "rpg") + "/%"  # where game books are kept: all of them get their credits read
PAGES = 5                      # PDF pages read for text
OCR_PAGES = 3                  # PDF pages OCR'd when there is no text layer
TEXT_CHARS = 20000
SKIP_FORMATS = {"ZIP", "RAR", "7Z", "CBR", "CBZ", "CB7", "DJVU", "DJV"}
NO_TEXT_LAYER = 200            # fewer characters than this over the first pages means a scan
FULL_NAME_SCORE = 2.0          # name_score of a line taken as the author with no other clue
NOT_A_PERSON = re.compile(r"(?i)\btitle\b|press|publish|edition|sales|copyright|library|universit|department|"
                          r"chapter|contents|series|volume|\d")


def targets(limit: int | None = None, reread_credits: bool = False) -> list[dict]:
    """Files of on-disk ebooks with no author or an uncertain title/author, and every game book (their
    document info often names a layout artist or uploader), not read here before. reread_credits: the
    game-book PDFs already read from their text layer, read again for credits set in two columns."""
    if reread_credits:
        return [dict(r) for r in query(f"""
            SELECT f.id, f.file_path, upper(f.format) AS format, f.size_bytes, f.evidence, b.title, b.authors
            FROM book_files f JOIN books b ON b.id = f.book_id
            WHERE f.present AND b.available AND NOT b.hidden AND b.directory LIKE %s
              AND f.evidence->'frontpages'->>'source' = 'pdf-text'
            ORDER BY f.id {'LIMIT %s' if limit else ''}
        """, [RPG_DIRS] + ([limit] if limit else []))]
    rows = query(f"""
        SELECT f.id, f.file_path, upper(f.format) AS format, f.size_bytes, f.evidence, b.title, b.authors
        FROM book_files f JOIN books b ON b.id = f.book_id
        WHERE f.present AND b.available AND NOT b.hidden
          AND (cardinality(b.authors) = 0 OR b.needs_review OR b.directory LIKE %s)
          AND NOT (b.formats <@ ARRAY['CBR','CBZ','CB7'])
          AND NOT upper(coalesce(f.format, '')) = ANY(%s)
          AND NOT coalesce(f.evidence ? 'frontpages', false)
        ORDER BY f.id {'LIMIT %s' if limit else ''}
    """, [RPG_DIRS, sorted(SKIP_FORMATS)] + ([limit] if limit else []))
    return [dict(r) for r in rows]


def known_authors() -> set:
    """Normalized names of authors with two or more books on disk."""
    rows = query("""
        SELECT a FROM (SELECT DISTINCT COALESCE(work_key, 'id' || id) AS w, authors[1] AS a FROM books
                       WHERE available AND NOT hidden AND cardinality(authors) > 0) t
        GROUP BY a HAVING count(*) >= 2
    """)
    return {norm_name(r["a"]) for r in rows
            if " " in norm_name(r["a"]) and name_score(r["a"]) > 0.5 and not is_corporate(r["a"])}


def _run(cmd: list[str], timeout: int) -> str:
    try:
        res = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        return ""
    return res.stdout.decode("utf-8", "replace") if res.returncode == 0 else ""


def _pdf(path: str, tmp: str) -> tuple[str, list[str], str]:
    """(text, largest-type lines, source) for the first pages of a PDF."""
    import fitz
    with fitz.open(path) as doc:
        pages = [doc[i] for i in range(min(PAGES, len(doc)))]
        text = "\n".join(p.get_text() for p in pages)
        lines = []
        for p in pages[:2]:
            for block in p.get_text("dict")["blocks"]:
                for line in block.get("lines", []):
                    s = re.sub(r"\s+", " ", " ".join(x["text"] for x in line["spans"])).strip()
                    if s:
                        lines.append((max(x["size"] for x in line["spans"]), s))
    headlines = []
    if lines:
        body = statistics.median(size for size, _ in lines)
        headlines = [s for size, s in sorted(lines, key=lambda x: -x[0])
                     if size >= 1.3 * body and 2 <= len(s) <= 90][:4]
    if len(text.strip()) >= NO_TEXT_LAYER:
        return text, headlines, "pdf-text"
    _run([PDFTOPPM, "-r", "150", "-gray", "-f", "1", "-l", str(OCR_PAGES), "-png", path, os.path.join(tmp, "p")], 180)
    pages = sorted(n for n in os.listdir(tmp) if n.startswith("p") and n.endswith(".png"))
    ocr = "\n".join(_run([TESSERACT, os.path.join(tmp, n), "stdout"], 120) for n in pages)
    return ocr, [], "ocr"


def _ebook_meta(path: str) -> dict | None:
    out = _run([EBOOK_META, path], 60)
    fields = dict(re.findall(r"^(Title|Author\(s\))\s*:\s*(.+)$", out, re.M))
    title = fields.get("Title", "").strip()
    authors = [re.sub(r"\s*\[.*?\]\s*$", "", a).strip() for a in fields.get("Author(s)", "").split("&")]
    authors = [a for a in authors if a and a.lower() not in ("unknown", "author")]
    if not title and not authors:
        return None
    return {"title": title or None, "authors": authors, "subjects": [], "isbn": None, "series": None}


def read_front(path: str, fmt: str) -> dict:
    """Opening text and clues for one file (a book inside a zip is copied out first). Never raises."""
    archive, member = split_member(path)
    name = (member or path).rsplit("/", 1)[-1]
    fmt = (fmt or "").upper()
    out = {"text": "", "headlines": [], "source": None, "meta": None}
    with tempfile.TemporaryDirectory() as tmp:
        local = path
        try:
            if member is not None:
                local = os.path.join(tmp, name)
                with zipfile.ZipFile(archive) as zf, zf.open(member) as src, open(local, "wb") as dst:
                    shutil.copyfileobj(src, dst)
            if fmt == "PDF":
                out["text"], out["headlines"], out["source"] = _pdf(local, tmp)
            elif fmt in ("HTM", "HTML", "XHTML"):
                with open(local, "rb") as f:
                    out["text"], out["source"] = html_to_text(f.read(400_000).decode("utf-8", "replace")), "html"
            elif fmt in ("TXT", "RTF"):
                out["text"], out["source"] = _read_text_file(local, rtf=fmt == "RTF")["text_raw"], "text"
            elif fmt == "DOC":
                txt = os.path.join(tmp, "doc.txt")
                _run([TEXTUTIL, "-convert", "txt", "-output", txt, local], 120)
                if os.path.exists(txt):
                    out["text"], out["source"] = open(txt, errors="replace").read(TEXT_CHARS), "textutil"
            else:
                out["meta"] = _ebook_meta(local)
                txt = os.path.join(tmp, "book.txt")
                _run([EBOOK_CONVERT, local, txt], 240)
                if os.path.exists(txt):
                    out["text"] = open(txt, errors="replace").read(TEXT_CHARS)
                out["source"] = "calibre"
        except Exception as e:                 # unreadable, truncated, not what its name claims
            out["error"] = f"{type(e).__name__}: {e}"[:200]
    out["text"] = (out["text"] or "")[:TEXT_CHARS]
    return out


def clues_from(text: str, headlines: list[str], known: set, current: list[str] | None = None) -> dict:
    """front_matter_clues(), plus the first opening or large-type line that is exactly a known author or,
    failing that, reads as a person's full name. current: the book's title and authors when it has an
    author, which such a line must then confirm."""
    clues = front_matter_clues(text)
    clues["isbns"] = [f["isbn"] for f in find_isbns(text)[:3]]
    if clues.pop("contributors", False):        # a magazine's or anthology's contributors: no author here
        return clues
    if clues["credited"] and clues["bylines"]:  # a credits block named the writers: the other names are crew
        return clues
    lines = [re.sub(r"\s+", " ", l).strip(" .") for l in text.splitlines()]
    # Quoted or dashed lines are blurb attributions ("—Stephen King"), not the book's author.
    lines = [l for l in lines if 2 <= len(l) <= 90 and not l.startswith(("—", "–", "-", '"', "“", "'", "‘"))][:25]
    held = [norm_name(c) for c in current or [] if c]

    def fits(line):
        return not held or any(norm_name(line) and norm_name(line) in h for h in held)

    weak = next((l for l in headlines + lines if norm_name(l) in known and not is_corporate(l)), None)
    if weak is None and not clues["bylines"]:
        weak = next((l for l in headlines + lines
                     if 2 <= len(l.split()) <= 5 and name_score(l) >= FULL_NAME_SCORE and not is_corporate(l)
                     and not NOT_A_PERSON.search(l)), None)
    if weak and fits(weak) and weak not in clues["bylines"]:
        clues["bylines"].append(weak)
    return clues


def merge(evidence: dict, found: dict, known: set, current: list[str] | None = None,
          credits_win: bool = False) -> dict:
    """The file's evidence with the new clues added (never replacing what the scan stored). With
    credits_win, writers named in a credits block replace the bylines read before."""
    ev = dict(evidence or {})
    text = found["text"]
    new = clues_from(text, found["headlines"], known, current) if text.strip() else \
        {"bylines": [], "copyright": [], "title_page": None, "isbns": []}
    old = ev.get("text") or {}
    keep = [] if credits_win and new.get("credited") and new["bylines"] else old.get("bylines") or []
    merged = {
        "bylines": list(dict.fromkeys(keep + new["bylines"])),
        "copyright": (old.get("copyright") or []) + [c for c in new["copyright"] if c not in (old.get("copyright") or [])],
        "title_page": old.get("title_page") or new["title_page"],
        "isbns": list(dict.fromkeys((old.get("isbns") or []) + new["isbns"])),
        "chars": max(old.get("chars") or 0, len(text)),
    }
    if any(merged[k] for k in ("bylines", "copyright", "title_page", "isbns")):
        ev["text"] = merged
    if found.get("meta") and not ev.get("meta"):
        ev["meta"], ev["meta_group"] = found["meta"], "document"
    ev["frontpages"] = {"source": found["source"], "chars": len(text),
                        **({"error": found["error"]} if found.get("error") else {})}
    return sanitize_json(ev)


def run(limit: int | None = None, workers: int = 4, dry_run: bool = False, resolve_after: bool = True,
        reread_credits: bool = False):
    print("\n📖 Reading the first pages of books with no author or an uncertain title")
    files = targets(limit, reread_credits)
    known = known_authors()
    # Copies of one file (same name and size, in two libraries) are read once.
    copies = {}
    for f in files:
        copies.setdefault((re.split(r"[/:]", f["file_path"])[-1], f["size_bytes"]), []).append(f)
    print(f"   {len(files):,} files to read ({len(copies):,} distinct); {len(known):,} known authors to recognise",
          flush=True)
    sources, gained, rows = {}, 0, []
    conn = None if dry_run else get_conn()
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(read_front, group[0]["file_path"], group[0]["format"]): group
                       for group in copies.values()}
            done = ((fut.result(), f) for fut in tqdm(as_completed(futures), total=len(futures), desc="   Reading",
                                                    mininterval=2) for f in futures[fut])
            for found, f in done:
                ev = merge(f["evidence"], found, known, [f["title"]] + f["authors"] if f["authors"] else None,
                           credits_win=reread_credits)
                sources[found["source"] or "none"] = sources.get(found["source"] or "none", 0) + 1
                before = f["evidence"] or {}
                if (ev.get("text") or {}) != (before.get("text") or {}) or ev.get("meta") != before.get("meta"):
                    gained += 1
                if dry_run:
                    t = ev.get("text") or {}
                    print(f"   {f['file_path'].rsplit('/', 1)[-1][:60]!r}: {found['source']}, "
                          f"bylines={t.get('bylines')}, title={t.get('title_page')!r}, meta={ev.get('meta') and ev['meta'].get('authors')}")
                    continue
                rows.append((psycopg2.extras.Json(ev), f["id"]))
                if len(rows) >= 50:
                    _write(conn, rows)
                    rows = []
        if rows:
            _write(conn, rows)
    finally:
        if conn:
            conn.close()
    print(f"   Read by: {sources}")
    print(f"   {gained:,} of {len(files):,} files gave new clues")
    if resolve_after and not dry_run:
        from books.resolve import resolve
        resolve()


def _write(conn, rows):
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, "UPDATE book_files SET evidence = %s WHERE id = %s", rows)
    conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read the first pages of books lacking metadata")
    parser.add_argument("--limit", type=int, help="Read at most this many files")
    parser.add_argument("--workers", type=int, default=4, help="Files read in parallel")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be found; change nothing")
    parser.add_argument("--no-resolve", action="store_true", help="Skip the resolve stage")
    parser.add_argument("--reread-credits", action="store_true",
                        help="Read game-book PDFs again for credits set in two columns")
    args = parser.parse_args()
    run(args.limit, args.workers, args.dry_run, not args.no_resolve, args.reread_credits)
