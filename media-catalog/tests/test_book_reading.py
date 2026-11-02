import struct
import zipfile

import pytest

from books import reading


def _mobi(path, encryption: int):
    """Minimal PalmDB with one record whose PalmDOC header carries the encryption type."""
    head = bytearray(78)
    head[60:68] = b"BOOKMOBI"
    head[76:78] = struct.pack(">H", 1)
    record0 = struct.pack(">HHIHHH", 2, 0, 1000, 1, 4096, encryption)
    path.write_bytes(bytes(head) + struct.pack(">II", 86, 0) + record0)
    return str(path)


def _zip(path, members: dict):
    with zipfile.ZipFile(path, "w") as z:
        for name, data in members.items():
            z.writestr(name, data)
    return str(path)


def test_formats_are_ranked_reflowable_first_then_comics_pdf_and_conversions():
    order = sorted(["RTF", "PDF", "CBR", "MOBI", "EPUB", "DJVU"], key=reading.read_rank)
    assert order == ["EPUB", "MOBI", "CBR", "PDF", "RTF", "DJVU"]
    assert [reading.how(f) for f in ("epub", "pdf", "lit", "djvu")] == ["native", "pdf", "convert", None]
    assert not reading.readable("DJVU") and reading.readable("cbz")


def test_comic_archives_are_mostly_images():
    assert reading._is_comic(["p01.jpg", "p02.JPG", "p03.png", "p04.webp", "ComicInfo.xml"])
    assert not reading._is_comic(["book.txt", "cover.jpg"])
    assert not reading._is_comic([])


def test_encrypted_mobi_is_drm(tmp_path):
    assert reading._drm(_mobi(tmp_path / "a.mobi", 2), "MOBI")
    assert not reading._drm(_mobi(tmp_path / "b.azw", 0), "AZW")


def test_epub_font_obfuscation_is_not_drm_but_adept_is(tmp_path):
    obfuscated = '<encryption><EncryptedData><EncryptionMethod Algorithm="http://www.idpf.org/2008/embedding"/>' \
                 '</EncryptedData></encryption>'
    aes = obfuscated.replace("http://www.idpf.org/2008/embedding", "http://www.w3.org/2001/04/xmlenc#aes128-cbc")
    assert not reading._drm(_zip(tmp_path / "a.epub", {"META-INF/encryption.xml": obfuscated}), "EPUB")
    assert reading._drm(_zip(tmp_path / "b.epub", {"META-INF/encryption.xml": aes}), "EPUB")
    assert reading._drm(_zip(tmp_path / "c.epub", {"META-INF/rights.xml": "<rights/>"}), "EPUB")


def test_prepare_serves_native_files_and_zipped_comics_in_place(tmp_path):
    pdf = tmp_path / "a.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    assert reading.prepare(1, str(pdf), "PDF") == (str(pdf), "pdf", "pdf")
    epub = _zip(tmp_path / "b.epub", {"mimetype": "application/epub+zip"})
    assert reading.prepare(2, epub, "EPUB") == (epub, "epub", "foliate")
    comic = _zip(tmp_path / "c.cbr", {"01.jpg": b"x", "02.jpg": b"y"})   # a "CBR" that is really a zip
    assert reading.prepare(3, comic, "CBR") == (comic, "cbz", "foliate")


def test_unreadable_files_raise_and_the_failure_is_remembered(tmp_path, monkeypatch):
    djvu = tmp_path / "a.djvu"
    djvu.write_bytes(b"AT&TFORM")
    with pytest.raises(reading.NotReadable):
        reading.prepare(4, str(djvu), "DJVU")
    calls = []
    monkeypatch.setattr(reading, "_prepare", lambda *a: calls.append(a))
    with pytest.raises(reading.NotReadable):
        reading.prepare(4, str(djvu), "DJVU")
    assert calls == []
    with pytest.raises(reading.NotReadable, match="not on disk"):
        reading.prepare(5, str(tmp_path / "gone.epub"), "EPUB")


def test_a_timed_out_conversion_is_retried(tmp_path, monkeypatch):
    rtf = tmp_path / "a.rtf"
    rtf.write_bytes(b"{\\rtf1 hi}")
    calls = []

    def slow(*a):
        calls.append(a)
        raise reading.TimedOut("conversion took longer than 5 minutes")
    monkeypatch.setattr(reading, "_prepare", slow)
    for _ in range(2):
        with pytest.raises(reading.TimedOut):
            reading.prepare(6, str(rtf), "RTF")
    assert len(calls) == 2
