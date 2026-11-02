"""
Prepare book files for the in-browser reader.

foliate-js (web/static/foliate) reads EPUB, MOBI/AZW/AZW3/PRC, FB2 and CBZ
directly, and PDFs open in the browser's own viewer. Comic RARs are repacked
to CBZ with libarchive (bsdtar), and other formats are converted to EPUB once
with Calibre's ebook-convert. A book stored inside a zip ("archive.zip::member")
is first copied out of the zip into the cache. Prepared files live in a local
cache keyed by file id, size and modification time; nothing is ever written to
the share.
"""
import os
import re
import shutil
import signal
import struct
import subprocess
import tempfile
import threading
import zipfile
from pathlib import Path

from books.formats import split_member

ROOT_DIR = Path(__file__).resolve().parent.parent
CACHE_DIR = Path(os.environ.get("BOOKS_CACHE_DIR") or ROOT_DIR / ".books_cache")
EBOOK_CONVERT = (os.environ.get("EBOOK_CONVERT") or shutil.which("ebook-convert")
                 or "/Applications/calibre.app/Contents/MacOS/ebook-convert")
BSDTAR = shutil.which("bsdtar") or "/usr/bin/bsdtar"
TEXTUTIL = shutil.which("textutil") or "/usr/bin/textutil"
CONVERT_TIMEOUT = 300

# Served as-is: format → extension foliate-js needs in the URL (it detects CBZ and FB2 by name).
NATIVE = {"EPUB": "epub", "KEPUB": "epub", "AZW3": "azw3", "MOBI": "mobi", "PRC": "prc", "AZW": "azw",
          "FB2": "fb2", "CBZ": "cbz"}
ARCHIVES = {"CBR", "CB7", "ZIP", "RAR", "7Z"}          # comics → CBZ, anything else → EPUB
CONVERTIBLE = {"HTMLZ", "LIT", "RTF", "DOCX", "ODT", "PDB", "LRF", "CHM", "TXT", "TXTZ", "HTML", "HTM",
               "RB", "SNB", "TCR", "PML", "FBZ", "DOC"}
# Reflowable native formats first, then comics and PDF (instant), then formats needing conversion.
READ_ORDER = ["EPUB", "KEPUB", "AZW3", "MOBI", "PRC", "AZW", "FB2", "CBZ", "CBR", "CB7", "PDF", "HTMLZ",
              "DOCX", "ODT", "RTF", "LIT", "FBZ", "PDB", "LRF", "CHM", "TXT", "TXTZ", "HTML", "HTM", "RB",
              "SNB", "TCR", "PML", "ZIP", "RAR", "7Z", "DOC"]
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")
# EPUB font obfuscation is not DRM; any other encryption algorithm is.
FONT_OBFUSCATION = {"http://www.idpf.org/2008/embedding", "http://ns.adobe.com/pdf/enc#RC"}
MOBI_FORMATS = {"MOBI", "PRC", "AZW", "AZW3"}


class NotReadable(Exception):
    """The file cannot be shown in the browser (unsupported format, conversion failed, DRM…)."""


class TimedOut(NotReadable):
    """A converter ran too long; worth retrying later, so it is not remembered."""


_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()
_prepared: dict[tuple, tuple[str, str, str] | NotReadable] = {}


def readable(fmt: str | None) -> bool:
    f = (fmt or "").upper()
    return f in NATIVE or f == "PDF" or f in ARCHIVES or f in CONVERTIBLE


def how(fmt: str | None) -> str | None:
    """'native', 'pdf' or 'convert' — what opening this format in the browser involves."""
    f = (fmt or "").upper()
    if f in NATIVE:
        return "native"
    if f == "PDF":
        return "pdf"
    if f in ARCHIVES or f in CONVERTIBLE:
        return "convert"
    return None


def read_rank(fmt: str | None) -> int:
    f = (fmt or "").upper()
    return READ_ORDER.index(f) if f in READ_ORDER else len(READ_ORDER)


def _magic(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(8)


def _members(path: str, is_zip: bool) -> list[str]:
    if is_zip:
        with zipfile.ZipFile(path) as z:
            return [n for n in z.namelist() if not n.endswith("/")]
    res = subprocess.run([BSDTAR, "-tf", path], capture_output=True, text=True, timeout=120)
    if res.returncode and not res.stdout:
        raise NotReadable(f"cannot list archive: {res.stderr.strip()[:200]}")
    return [n for n in res.stdout.splitlines() if not n.endswith("/")]


def _is_comic(names: list[str]) -> bool:
    pages = [n for n in names if n.lower().endswith(IMAGE_EXTS)]
    return bool(pages) and len(pages) >= 0.8 * len(names)


def _drm(path: str, f: str) -> bool:
    """True when the file is DRM-protected (Adobe/Readium EPUB or encrypted Mobipocket)."""
    try:
        if f in MOBI_FORMATS:
            with open(path, "rb") as fh:
                head = fh.read(86)
                if len(head) < 86 or head[60:68] not in (b"BOOKMOBI", b"TEXtREAd"):
                    return False
                fh.seek(struct.unpack(">I", head[78:82])[0] + 12)
                return struct.unpack(">H", fh.read(2))[0] != 0
        if f in ("EPUB", "KEPUB"):
            with zipfile.ZipFile(path) as z:
                names = set(z.namelist())
                if "META-INF/rights.xml" in names:
                    return True
                if "META-INF/encryption.xml" in names:
                    xml = z.read("META-INF/encryption.xml").decode("utf-8", "replace")
                    return bool(set(re.findall(r'EncryptionMethod[^>]*Algorithm="([^"]+)"', xml)) - FONT_OBFUSCATION)
    except (OSError, zipfile.BadZipFile, struct.error):
        return False
    return False


def _cached(file_id: int, path: str, ext: str) -> Path:
    st = os.stat(path)
    return CACHE_DIR / f"{file_id}-{st.st_size}-{int(st.st_mtime)}.{ext}"


def _run(cmd: list[str], timeout: int = CONVERT_TIMEOUT):
    """Run a converter in its own process group so a timeout kills all of it."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        raise TimedOut(f"conversion took longer than {timeout // 60} minutes")
    if proc.returncode:
        tail = out.decode("utf-8", "replace").strip().splitlines()[-3:]
        raise NotReadable("conversion failed: " + " / ".join(tail)[:300])


def _build(target: Path, make):
    """Create target once: make(tmp_path) writes a temp file that is renamed into place."""
    with _locks_guard:
        lock = _locks.setdefault(target.name, threading.Lock())
    with lock:
        if target.exists():
            return target
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=CACHE_DIR) as tmp:
            out = Path(tmp) / f"out{target.suffix}"
            make(out, Path(tmp))
            if not out.exists() or out.stat().st_size == 0:
                raise NotReadable("conversion produced no output")
            os.replace(out, target)
    return target


def unpack_member(file_id: int, path: str) -> str:
    """Copy a book stored inside a zip ("archive.zip::member") into the cache once; returns the copy."""
    archive, member = split_member(path)
    st = os.stat(archive)
    ext = os.path.splitext(member)[1].lstrip(".").lower() or "bin"
    target = CACHE_DIR / f"{file_id}-{st.st_size}-{int(st.st_mtime)}-member.{ext}"

    def make(out, tmp):
        with zipfile.ZipFile(archive) as zf:
            try:
                src = zf.open(member)
            except KeyError:
                raise NotReadable(f"{member} is no longer inside {os.path.basename(archive)}")
            with src, open(out, "wb") as dst:
                shutil.copyfileobj(src, dst)
    return str(_build(target, make))


def _to_epub(src: str, out: Path, tmp: Path, fmt: str):
    if not os.path.exists(EBOOK_CONVERT):
        raise NotReadable("Calibre's ebook-convert is not installed (set EBOOK_CONVERT)")
    if fmt == "DOC":
        # Calibre has no .doc input; macOS textutil turns it into HTML first.
        html = tmp / "book.html"
        _run([TEXTUTIL, "-convert", "html", "-output", str(html), src], timeout=120)
        src = str(html)
    _run([EBOOK_CONVERT, src, str(out)])


def prepare(file_id: int, path: str, fmt: str | None) -> tuple[str, str, str]:
    """Return (local path, extension, mode) for the reader; mode is 'foliate' or 'pdf'.

    Raises NotReadable when the file cannot be shown in the browser.
    """
    f = (fmt or os.path.splitext(path)[1].lstrip(".")).upper()
    archive, member = split_member(path)
    try:
        st = os.stat(archive)
    except OSError:
        raise NotReadable("file is not on disk")
    key = (file_id, path, st.st_size, st.st_mtime)
    done = _prepared.get(key)
    if done is None or (isinstance(done, tuple) and not os.path.exists(done[0])):
        try:
            done = _prepare(file_id, unpack_member(file_id, path) if member else path, f)
        except TimedOut:
            raise
        except NotReadable as e:
            done = e                     # remembered so a failed conversion is not retried on every request
        _prepared[key] = done
    if isinstance(done, NotReadable):
        raise done
    return done


def _prepare(file_id: int, path: str, f: str) -> tuple[str, str, str]:
    if _drm(path, f):
        raise NotReadable(f"this {f} is DRM-protected")
    if f == "PDF":
        return path, "pdf", "pdf"
    if f in NATIVE and f != "CBZ":
        return path, NATIVE[f], "foliate"
    if f in ARCHIVES or f == "CBZ":
        is_zip = _magic(path).startswith(b"PK\x03\x04")
        names = _members(path, is_zip)
        if _is_comic(names):
            if is_zip:                       # a CBZ, or a .cbr/.zip that is really a zip of pages
                return path, "cbz", "foliate"
            target = _cached(file_id, path, "cbz")
            _build(target, lambda out, tmp: _run([BSDTAR, "-cf", str(out), "--format", "zip", f"@{path}"]))
            return str(target), "cbz", "foliate"
        if f == "CBZ":
            raise NotReadable("archive holds no comic pages")
    elif f not in CONVERTIBLE:
        raise NotReadable(f"{f or 'this format'} cannot be shown in the browser")
    target = _cached(file_id, path, "epub")
    _build(target, lambda out, tmp: _to_epub(path, out, tmp, f))
    return str(target), "epub", "foliate"
