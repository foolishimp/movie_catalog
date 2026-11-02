"""
Read identifying evidence out of ebook files.

extract(path) returns one evidence dict per file:

    {
      "format": "epub",
      "meta": {...} | None,        # metadata embedded in the file
      "meta_group": "publisher" | "calibre" | "document" | None,
      "text": {...} | None,        # clues found in the book's own front-matter text
      "filename": {...},           # naming.parse_filename() of the file (or inner file)
      "cover": {...} | None,       # where an embedded cover lives
      "error": "..."               # present when the file could not be read
    }

meta_group records provenance so the resolver can vote by independent source:
"calibre" marks metadata Calibre wrote (its OPF, its MOBI conversions, PDFs it
produced) because that is the same record as the Calibre database, not a
second opinion. Every read is bounded so one bad file cannot stall a scan.

A book stored inside a zip (a Calibre library archived one zip per author) is
addressed as "archive.zip::member/path" and read from a temporary copy.
"""
import io
import logging
import math
import os
import re
import shutil
import struct
import tempfile
import zipfile
import xml.etree.ElementTree as ET

from books.naming import file_stem, find_isbns, front_matter_clues, html_to_text, normalize_isbn, parse_filename

TEXT_SAMPLE_CHARS = 16000      # front matter only: back matter lists other books' ISBNs
MAX_SPINE_ITEMS = 6
MAX_ITEM_BYTES = 300_000
MAX_INNER_BYTES = 25 * 1024 * 1024
PDF_TEXT_PAGES = 3

MEMBER_SEP = "::"

logging.getLogger("pypdf").setLevel(logging.ERROR)


def split_member(path: str) -> tuple[str, str | None]:
    """(archive, member) for a book stored inside a zip, else (path, None)."""
    archive, sep, member = path.partition(MEMBER_SEP)
    return (archive, member) if sep else (path, None)


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower() if isinstance(tag, str) else ""


def _attr(el, name: str):
    for key, value in el.attrib.items():
        if key.rsplit("}", 1)[-1].lower() == name:
            return value
    return None


def _clean(value) -> str | None:
    if value is None:
        return None
    v = re.sub(r"\s+", " ", str(value)).strip()
    return v or None


def _empty_meta() -> dict:
    return {"title": None, "authors": [], "series": None, "series_index": None, "subjects": [],
            "publisher": None, "date": None, "language": None, "isbn": None, "identifiers": {},
            "description": None, "rating": None}


# ---------------------------------------------------------------------------
# OPF (EPUB package documents, Calibre sidecars, HTMLZ)
# ---------------------------------------------------------------------------

def parse_opf(data: bytes) -> tuple[dict, bool, str | None]:
    """Parse an OPF package document. Returns (meta, written_by_calibre, cover_href)."""
    root = ET.fromstring(data)
    meta = _empty_meta()
    by_calibre = False
    refines: dict[str, dict] = {}
    creators = []
    cover_id = None
    for el in root.iter():
        name = _local(el.tag)
        if name == "meta":
            prop, refine = _attr(el, "property"), _attr(el, "refines")
            if prop and refine:
                refines.setdefault(refine.lstrip("#"), {})[prop.lower()] = _clean(el.text)
            mname, content = (_attr(el, "name") or "").lower(), _attr(el, "content")
            if mname.startswith("calibre:"):
                by_calibre = True
            if mname == "calibre:series":
                meta["series"] = _clean(content)
            elif mname == "calibre:series_index":
                try:
                    meta["series_index"] = float(content)
                except (TypeError, ValueError):
                    pass
            elif mname == "calibre:rating":
                try:
                    meta["rating"] = float(content)
                except (TypeError, ValueError):
                    pass
            elif mname == "cover":
                cover_id = content
            if prop == "belongs-to-collection" and not meta["series"]:
                meta["series"] = _clean(el.text)
            if prop == "group-position" and meta["series_index"] is None:
                try:
                    meta["series_index"] = float(el.text)
                except (TypeError, ValueError):
                    pass
    for el in root.iter():
        name, text = _local(el.tag), _clean(el.text)
        if not text and name not in ("item",):
            continue
        if name == "title" and not meta["title"]:
            meta["title"] = text
        elif name == "creator":
            role = (_attr(el, "role") or refines.get(el.get("id", ""), {}).get("role") or "aut").lower()
            if role == "aut":
                creators.append(text)
        elif name == "contributor":
            if "calibre" in (text or "").lower():
                by_calibre = True
        elif name == "subject":
            meta["subjects"].append(text)
        elif name == "date" and not meta["date"]:
            event = (_attr(el, "event") or "").lower()
            if event in ("", "publication", "original-publication", "issued"):
                meta["date"] = text
        elif name == "publisher" and not meta["publisher"]:
            meta["publisher"] = text
        elif name == "language" and not meta["language"]:
            meta["language"] = text
        elif name == "description" and not meta["description"]:
            meta["description"] = html_to_text(el.text)[:8000] or None
        elif name == "identifier":
            scheme = (_attr(el, "scheme") or "").lower()
            value = text or ""
            if scheme == "calibre" or value.lower().startswith("calibre:"):
                by_calibre = True
                continue
            if value.lower().startswith("urn:"):
                parts = value.split(":", 2)
                if len(parts) == 3:
                    scheme, value = scheme or parts[1].lower(), parts[2]
            isbn = normalize_isbn(value) if scheme in ("isbn", "") or "isbn" in scheme else None
            if isbn:
                meta["isbn"] = meta["isbn"] or isbn
                meta["identifiers"]["isbn"] = isbn
            elif scheme and scheme not in ("uuid", "uid", "bookid"):
                meta["identifiers"][scheme] = value
        elif name == "item":
            props = (_attr(el, "properties") or "").lower()
            if "cover-image" in props or (cover_id and el.get("id") == cover_id):
                meta.setdefault("_cover_href", _attr(el, "href"))
    meta["authors"] = creators
    cover_href = meta.pop("_cover_href", None)
    return meta, by_calibre, cover_href


def _zip_text(zf: zipfile.ZipFile, names: list[str]) -> str:
    chunks, total = [], 0
    for name in names[:MAX_SPINE_ITEMS]:
        try:
            with zf.open(name) as f:
                raw = f.read(MAX_ITEM_BYTES)
        except KeyError:
            continue
        text = html_to_text(raw.decode("utf-8", errors="replace"))
        chunks.append(text)
        total += len(text)
        if total >= TEXT_SAMPLE_CHARS:
            break
    return "\n".join(chunks)[:TEXT_SAMPLE_CHARS]


def _read_epub(zf: zipfile.ZipFile) -> dict:
    container = ET.fromstring(zf.read("META-INF/container.xml"))
    opf_path = next(_attr(el, "full-path") for el in container.iter() if _local(el.tag) == "rootfile")
    opf_bytes = zf.read(opf_path)
    meta, by_calibre, cover_href = parse_opf(opf_bytes)
    base = opf_path.rsplit("/", 1)[0] + "/" if "/" in opf_path else ""

    root = ET.fromstring(opf_bytes)
    manifest = {el.get("id"): _attr(el, "href") for el in root.iter() if _local(el.tag) == "item"}
    spine = [manifest.get(_attr(el, "idref")) for el in root.iter() if _local(el.tag) == "itemref"]
    spine = [base + href for href in spine if href]
    out = {"meta": meta, "meta_group": "calibre" if by_calibre else "publisher",
           "text_raw": _zip_text(zf, spine)}
    if cover_href:
        out["cover"] = {"kind": "zip", "member": base + cover_href}
    return out


# ---------------------------------------------------------------------------
# MOBI / AZW / AZW3 / PRC
# ---------------------------------------------------------------------------

EXTH_FIELDS = {100: "author", 101: "publisher", 103: "description", 104: "isbn", 105: "subject",
               106: "date", 108: "contributor", 112: "source", 113: "asin", 503: "title", 524: "language"}


def palmdoc_decompress(data: bytes) -> bytes:
    out = bytearray()
    i, n = 0, len(data)
    while i < n:
        c = data[i]
        i += 1
        if 1 <= c <= 8:
            out += data[i:i + c]
            i += c
        elif c < 0x80:
            out.append(c)
        elif c >= 0xC0:
            out.append(0x20)
            out.append(c ^ 0x80)
        elif i < n:
            pair = (c << 8) | data[i]
            i += 1
            dist, length = (pair >> 3) & 0x7FF, (pair & 7) + 3
            for _ in range(length):
                out.append(out[-dist] if 0 < dist <= len(out) else 0x20)
    return bytes(out)


def _read_mobi_bytes(head: bytes, read_at) -> dict:
    """head is the start of the file; read_at(offset, size) reads further records."""
    if len(head) < 86 or head[60:68] not in (b"BOOKMOBI", b"TEXtREAd"):
        raise ValueError("not a MOBI/PalmDOC file")
    num_records = struct.unpack(">H", head[76:78])[0]
    want = min(num_records, 8)
    table = head[78:78 + 8 * want] if len(head) >= 78 + 8 * want else read_at(78, 8 * want)
    offsets = [struct.unpack(">I", table[i * 8:i * 8 + 4])[0] for i in range(want)]
    rec0 = read_at(offsets[0], min(offsets[1] - offsets[0], 128 * 1024) if want > 1 else 64 * 1024)
    # PalmDOC header: compression, unused, text length, record count, record size, encryption
    compression, _, _, _, _, encryption = struct.unpack(">HHIHHH", rec0[:14])
    meta = _empty_meta()
    by_calibre = False
    encoding = "cp1252"
    if rec0[16:20] == b"MOBI":
        header_len, = struct.unpack(">I", rec0[20:24])
        text_encoding, = struct.unpack(">I", rec0[28:32])
        encoding = "utf-8" if text_encoding == 65001 else "cp1252"
        name_off, name_len = struct.unpack(">II", rec0[84:92])
        meta["title"] = _clean(rec0[name_off:name_off + name_len].decode(encoding, errors="replace"))
        exth_flags, = struct.unpack(">I", rec0[128:132]) if len(rec0) >= 132 else (0,)
        pos = 16 + header_len
        if exth_flags & 0x40 and rec0[pos:pos + 4] == b"EXTH":
            count, = struct.unpack(">I", rec0[pos + 8:pos + 12])
            p = pos + 12
            for _ in range(count):
                if p + 8 > len(rec0):
                    break
                rtype, rlen = struct.unpack(">II", rec0[p:p + 8])
                value = rec0[p + 8:p + rlen].decode(encoding, errors="replace").strip("\x00 ")
                p += rlen
                field = EXTH_FIELDS.get(rtype)
                if not field or not value:
                    continue
                if field == "author":
                    meta["authors"].append(_clean(value))
                elif field == "subject":
                    meta["subjects"].append(_clean(value))
                elif field == "title":
                    meta["title"] = _clean(value)
                elif field == "isbn":
                    meta["isbn"] = normalize_isbn(value) or meta["isbn"]
                elif field == "asin":
                    meta["identifiers"]["asin"] = value
                elif field == "description":
                    meta["description"] = html_to_text(value)[:8000] or None
                elif field in ("contributor", "source"):
                    if "calibre" in value.lower():
                        by_calibre = True
                else:
                    meta[field] = _clean(value)
    else:
        meta["title"] = _clean(head[:32].split(b"\x00", 1)[0].decode("cp1252", errors="replace"))

    text = ""
    if compression in (1, 2) and encryption == 0 and want > 2:
        chunks = []
        for i in range(1, want - 1):
            raw = read_at(offsets[i], min(offsets[i + 1] - offsets[i], 8192))
            chunks.append(palmdoc_decompress(raw) if compression == 2 else raw)
            if sum(len(c) for c in chunks) >= TEXT_SAMPLE_CHARS:
                break
        text = html_to_text(b"".join(chunks).decode(encoding, errors="replace"))
    return {"meta": meta, "meta_group": "calibre" if by_calibre else "publisher", "text_raw": text}


def _read_mobi(path: str) -> dict:
    with open(path, "rb") as f:
        head = f.read(4096)

        def read_at(offset, size):
            f.seek(offset)
            return f.read(size)

        return _read_mobi_bytes(head, read_at)


# ---------------------------------------------------------------------------
# PDF
# ---------------------------------------------------------------------------

_JUNK_TITLE = re.compile(
    r"^(?:untitled|unknown|title|document|microsoft word|book|ebook|cover|contents|table of contents|"
    r"copyright|title page|\d+|default|administrator|admin|owner|user|"
    r"[\w\-. ]+\.(?:docx?|pdf|rtf|indd|qxd|qxp|p65|pm[d6]|tex|dvi|ps|tmp|ai|psd|eps|cdr))$"
    r"|^(?:djvu|postscript)\b.*\bdocument$"
    r"|^microsoft (?:word|powerpoint) - |^(?:[a-f0-9]{8,}|scan\d*|doc\d+|document\d+|(?:\w ){2,}\w)$",
    re.I,
)


def is_junk_title(title: str | None) -> bool:
    return not title or bool(_JUNK_TITLE.search(title.strip())) or len(title.strip()) < 2


def _read_pdf_stream(stream) -> dict:
    from pypdf import PdfReader

    reader = PdfReader(stream, strict=False)
    info = reader.metadata or {}
    meta = _empty_meta()
    title = _clean(info.get("/Title"))
    if title:
        title = re.sub(r"^Microsoft (?:Word|PowerPoint) - ", "", title)
        title = re.sub(r"\.(?:docx?|pdf|rtf)$", "", title, flags=re.I)
    meta["title"] = None if is_junk_title(title) else title
    author = _clean(info.get("/Author"))
    if author and not is_junk_title(author):
        meta["authors"] = [author]
    for key in ("/Subject", "/Keywords"):
        value = _clean(info.get(key))
        if value and len(value) < 200:
            meta["subjects"].extend(s.strip() for s in re.split(r"[;,]", value) if s.strip())
    producer = " ".join(str(info.get(k) or "") for k in ("/Producer", "/Creator")).lower()
    texts = []
    try:
        for page in reader.pages[:PDF_TEXT_PAGES]:
            texts.append(page.extract_text() or "")
    except Exception:
        pass
    return {"meta": meta, "meta_group": "calibre" if "calibre" in producer else "document",
            "text_raw": "\n".join(texts)[:TEXT_SAMPLE_CHARS]}


def _read_pdf(path: str) -> dict:
    with open(path, "rb") as f:
        return _read_pdf_stream(f)


# ---------------------------------------------------------------------------
# FB2, comics, DOCX, plain text
# ---------------------------------------------------------------------------

def _read_fb2_bytes(data: bytes) -> dict:
    head = data[:512 * 1024].decode("utf-8", errors="replace")
    m = re.search(r"<description>.*?</description>", head, re.S)
    meta = _empty_meta()
    if m:
        desc = ET.fromstring(re.sub(r"<(/?)[\w-]+:", r"<\1", m.group(0)))
        title_info = desc.find("title-info")
        if title_info is not None:
            meta["title"] = _clean(title_info.findtext("book-title"))
            for a in title_info.findall("author"):
                parts = [a.findtext(k) for k in ("first-name", "middle-name", "last-name")]
                name = _clean(" ".join(p for p in parts if p)) or _clean(a.findtext("nickname"))
                if name:
                    meta["authors"].append(name)
            meta["subjects"] = [g.text.strip() for g in title_info.findall("genre") if g.text]
            meta["date"] = _clean(title_info.findtext("date"))
            meta["language"] = _clean(title_info.findtext("lang"))
            seq = title_info.find("sequence")
            if seq is not None:
                meta["series"] = _clean(seq.get("name"))
                try:
                    meta["series_index"] = float(seq.get("number"))
                except (TypeError, ValueError):
                    pass
            ann = title_info.find("annotation")
            if ann is not None:
                meta["description"] = _clean(" ".join(ann.itertext()))
        publish = desc.find("publish-info")
        if publish is not None:
            meta["isbn"] = normalize_isbn(publish.findtext("isbn"))
            meta["publisher"] = _clean(publish.findtext("publisher"))
            meta["date"] = meta["date"] or _clean(publish.findtext("year"))
    body = head[m.end():] if m else head
    return {"meta": meta, "meta_group": "publisher", "text_raw": html_to_text(body)[:TEXT_SAMPLE_CHARS]}


def _read_comicinfo(zf: zipfile.ZipFile) -> dict | None:
    name = next((n for n in zf.namelist() if n.lower().endswith("comicinfo.xml")), None)
    if not name:
        return None
    root = ET.fromstring(zf.read(name))
    get = lambda k: _clean(root.findtext(k))
    meta = _empty_meta()
    meta.update(title=get("Title"), series=get("Series"), publisher=get("Publisher"),
                description=get("Summary"), date=get("Year"), language=get("LanguageISO"))
    meta["authors"] = [a.strip() for a in (get("Writer") or "").split(",") if a.strip()]
    meta["subjects"] = [g.strip() for g in (get("Genre") or "").split(",") if g.strip()]
    try:
        meta["series_index"] = float(get("Number"))
    except (TypeError, ValueError):
        pass
    return {"meta": meta, "meta_group": "publisher", "text_raw": ""}


def _read_docx(zf: zipfile.ZipFile) -> dict:
    root = ET.fromstring(zf.read("docProps/core.xml"))
    meta = _empty_meta()
    for el in root.iter():
        name, text = _local(el.tag), _clean(el.text)
        if name == "title" and not is_junk_title(text):
            meta["title"] = text
        elif name == "creator" and text and not is_junk_title(text):
            meta["authors"] = [text]
    body = ""
    if "word/document.xml" in zf.namelist():
        with zf.open("word/document.xml") as f:
            body = html_to_text(f.read(MAX_ITEM_BYTES).decode("utf-8", errors="replace"))
    return {"meta": meta, "meta_group": "document", "text_raw": body[:TEXT_SAMPLE_CHARS]}


def _read_text_file(path: str, rtf: bool = False) -> dict:
    with open(path, "rb") as f:
        raw = f.read(64 * 1024)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("cp1252", errors="replace")
    if rtf:
        text = re.sub(r"\\'[0-9a-f]{2}", "", text)
        text = re.sub(r"\\par[d]?\b", "\n", text)
        text = re.sub(r"\\[a-z]+-?\d* ?|[{}]", "", text)
    return {"meta": None, "meta_group": None, "text_raw": text[:TEXT_SAMPLE_CHARS]}


# ---------------------------------------------------------------------------
# Archives
# ---------------------------------------------------------------------------

_INNER_BOOK = re.compile(r"\.(?:epub|mobi|azw3?|prc|pdf|fb2|lit|rtf|txt|docx?)$", re.I)


def _read_zip_like(zf: zipfile.ZipFile, fmt: str) -> dict:
    names = zf.namelist()
    lower = {n.lower() for n in names}
    if "meta-inf/container.xml" in lower:
        return _read_epub(zf)
    if fmt == ".htmlz" or "metadata.opf" in lower:
        opf = next((n for n in names if n.lower().endswith(".opf")), None)
        html = next((n for n in names if n.lower().endswith((".html", ".xhtml", ".htm"))), None)
        out = {"meta": None, "meta_group": None, "text_raw": _zip_text(zf, [html] if html else [])}
        if opf:
            out["meta"], _, _ = parse_opf(zf.read(opf))
            out["meta_group"] = "calibre"
        if html:
            # Calibre keeps the pre-import filename as the HTML member name.
            out["inner"] = html
        return out
    if "docprops/core.xml" in lower:
        return _read_docx(zf)
    comic = _read_comicinfo(zf)
    if comic:
        return comic
    inner = [i for i in zf.infolist() if _INNER_BOOK.search(i.filename) and not i.filename.startswith("__MACOSX")]
    fb2 = [n for n in names if n.lower().endswith(".fb2")]
    if fb2:
        with zf.open(fb2[0]) as f:
            out = _read_fb2_bytes(f.read(MAX_INNER_BYTES))
        out["inner"] = fb2[0]
        return out
    if len(inner) == 1 and inner[0].file_size <= MAX_INNER_BYTES:
        info = inner[0]
        data = zf.read(info.filename)
        _, ext = file_stem(info.filename.rsplit("/", 1)[-1])
        out = _extract_bytes(data, ext)
        out["inner"] = info.filename
        return out
    return {"meta": None, "meta_group": None, "text_raw": "",
            "inner_names": [i.filename for i in inner[:20]]}


def _extract_bytes(data: bytes, ext: str) -> dict:
    if ext in (".epub", ".kepub", ".zip", ".cbz", ".docx", ".htmlz"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            return _read_zip_like(zf, ext)
    if ext in (".mobi", ".azw", ".azw3", ".prc"):
        return _read_mobi_bytes(data[:4096], lambda off, size: data[off:off + size])
    if ext == ".pdf":
        return _read_pdf_stream(io.BytesIO(data))
    if ext == ".fb2":
        return _read_fb2_bytes(data)
    return {"meta": None, "meta_group": None, "text_raw": ""}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_UNSTORABLE = re.compile(r"[\x00\ud800-\udfff]")


def sanitize_json(value):
    """Make a value storable as Postgres JSONB: no NUL characters, no lone
    surrogates (from bad UTF-16 in old files), no NaN/Infinity floats."""
    if isinstance(value, str):
        return _UNSTORABLE.sub("", value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {sanitize_json(k): sanitize_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_json(v) for v in value]
    return value


def _finish(evidence: dict) -> dict:
    """Turn raw front-matter text into compact clues; drop the text itself."""
    text = evidence.pop("text_raw", "") or ""
    if text.strip():
        clues = front_matter_clues(text)
        clues.pop("credited", None)
        clues["isbns"] = [f["isbn"] for f in find_isbns(text)[:3]]
        clues["chars"] = len(text)
        if any(clues[k] for k in ("bylines", "copyright", "title_page", "isbns")):
            evidence["text"] = clues
    meta = evidence.get("meta")
    if meta:
        meta["authors"] = [a for a in meta.get("authors", []) if a]
        meta["subjects"] = list(dict.fromkeys(s for s in meta.get("subjects", []) if s))
        if not any(meta.get(k) for k in ("title", "authors", "isbn", "series", "subjects")):
            evidence["meta"], evidence["meta_group"] = None, None
    return evidence


def _read_by_format(path: str, ext: str) -> dict:
    if ext in (".epub", ".kepub", ".zip", ".cbz", ".docx", ".htmlz", ".fb2.zip"):
        with zipfile.ZipFile(path) as zf:
            return _read_zip_like(zf, ext)
    if ext in (".mobi", ".azw", ".azw3", ".prc", ".pdb"):
        return _read_mobi(path)
    if ext == ".pdf":
        return _read_pdf(path)
    if ext == ".fb2":
        with open(path, "rb") as f:
            return _read_fb2_bytes(f.read(MAX_INNER_BYTES))
    if ext in (".txt", ".rtf"):
        return _read_text_file(path, rtf=(ext == ".rtf"))
    return {}


def extract(path: str) -> dict:
    """Read one ebook file (or "archive.zip::member") into an evidence dict. Never raises."""
    archive, member = split_member(path)
    name = (member or path).rsplit("/", 1)[-1]
    _, ext = file_stem(name)
    evidence = {"format": ext.lstrip(".") or "unknown", "meta": None, "meta_group": None,
                "filename": parse_filename(name)}
    try:
        if member is None:
            result = _read_by_format(path, ext)
        else:
            with tempfile.TemporaryDirectory() as tmp:
                local = os.path.join(tmp, name)
                with zipfile.ZipFile(archive) as zf, zf.open(member) as src, open(local, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                result = _read_by_format(local, ext)
        evidence.update(result)
        if result.get("inner"):
            evidence["filename_inner"] = parse_filename(result["inner"].rsplit("/", 1)[-1])
    except Exception as e:  # corrupt, truncated, DRM, or not what the extension claims
        evidence["error"] = f"{type(e).__name__}: {e}"[:300]
    return sanitize_json(_finish(evidence))


def read_sidecar_opf(path: str) -> dict | None:
    """Calibre's per-book metadata.opf, as calibre-group evidence."""
    try:
        with open(path, "rb") as f:
            meta, _, _ = parse_opf(f.read(512 * 1024))
        return meta
    except Exception:
        return None
