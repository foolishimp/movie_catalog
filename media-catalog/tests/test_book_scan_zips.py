"""Calibre libraries archived as one zip per author are read as the books inside them."""
import os
import zipfile

from books import reading, scan
from books.formats import extract, split_member

FB2 = ('<?xml version="1.0" encoding="utf-8"?><FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">'
       '<description><title-info><author><first-name>Joe</first-name><last-name>Abercrombie</last-name></author>'
       '<book-title>The Blade Itself</book-title></title-info></description><body><section><p>Logen.</p></section>'
       '</body></FictionBook>')
BLADE = "Joe Abercrombie/The Blade Itself (3)"


def _library(tmp_path):
    lib = tmp_path / "Library"
    lib.mkdir()
    with zipfile.ZipFile(lib / "Joe Abercrombie.zip", "w") as zf:
        zf.writestr(f"{BLADE}/The Blade Itself - Joe Abercrombie.fb2", FB2)
        zf.writestr(f"{BLADE}/cover.jpg", b"\xff\xd8jpeg")
        zf.writestr(f"{BLADE}/metadata.opf", "<package/>")
        zf.writestr("Joe Abercrombie/Not In Calibre (9)/Other.mobi", b"x")
    with zipfile.ZipFile(lib / "Wallpapers.zip", "w") as zf:
        zf.writestr("a/b/c.jpg", b"x")
    calibre_by_dir = {os.path.join(str(lib), BLADE): {"library": "Kindle", "title": "The Blade Itself"},
                      os.path.join(str(lib), "Joe Abercrombie/Gone (4)"): {"library": "Kindle", "title": "Gone"}}
    return str(lib), calibre_by_dir


def test_zip_members_are_matched_to_the_librarys_book_folders(tmp_path):
    lib, calibre_by_dir = _library(tmp_path)
    zip_path = os.path.join(lib, "Joe Abercrombie.zip")
    folders = scan.folder_index(calibre_by_dir)
    books, covers = scan._library_zip_books(zip_path, lib, folders)
    book_dir = os.path.join(lib, BLADE)
    assert books == [(f"{zip_path}::{BLADE}/The Blade Itself - Joe Abercrombie.fb2", len(FB2), book_dir)]
    assert covers == {book_dir: f"{zip_path}::{BLADE}/cover.jpg"}
    assert scan._library_zip_books(os.path.join(lib, "Wallpapers.zip"), lib, folders) == ([], {})


def test_zip_folders_match_the_catalogue_whatever_their_case(tmp_path):
    lib, calibre_by_dir = _library(tmp_path)
    zip_path = os.path.join(lib, "Joe Abercrombie.zip")
    with zipfile.ZipFile(zip_path, "a") as zf:
        zf.writestr("Joe Abercrombie/gone (4)/Gone - Joe Abercrombie.mobi", b"BOOKMOBI")
    books, _ = scan._library_zip_books(zip_path, lib, scan.folder_index(calibre_by_dir))
    assert (f"{zip_path}::Joe Abercrombie/gone (4)/Gone - Joe Abercrombie.mobi", 8,
            os.path.join(lib, "Joe Abercrombie/Gone (4)")) in books


def test_library_zips_become_their_books_and_the_library_folder_is_never_a_book(tmp_path):
    lib, calibre_by_dir = _library(tmp_path)
    author_zip, other_zip = os.path.join(lib, "Joe Abercrombie.zip"), os.path.join(lib, "Wallpapers.zip")
    loose = os.path.join(lib, "Loose Novel - Some Author.epub")
    files = [(author_zip, 100, 1.0), (other_zip, 50, 1.0), (loose, 400_000, 1.0)]
    file_root = {p: str(tmp_path) for p, _, _ in files}
    # A stray metadata.opf in the library's own folder must not make one book of everything in it.
    dir_info = {lib: {"opf": True, "cover": False}}
    units, file_unit, kept, storage = scan.group_units(files, dir_info, file_root, {lib}, calibre_by_dir)

    assert storage == {author_zip}
    member = f"{author_zip}::{BLADE}/The Blade Itself - Joe Abercrombie.fb2"
    blade = units["dir:" + os.path.join(lib, BLADE)]
    assert blade["calibre_dir"] and blade["library"] == "Kindle"
    assert blade["cover_path"] == f"{author_zip}::{BLADE}/cover.jpg"
    assert file_unit[member] == "dir:" + os.path.join(lib, BLADE)
    assert (member, len(FB2), 1.0) in kept and file_root[member] == str(tmp_path)
    assert "dir:" + lib not in units
    assert author_zip not in file_unit                    # read as its books, not as a file
    assert file_unit[other_zip].startswith("file:")       # an unrelated zip stays a loose file
    assert file_unit[loose].startswith("file:")


def test_extract_reads_a_book_inside_a_zip(tmp_path):
    lib, _ = _library(tmp_path)
    member = f"{lib}/Joe Abercrombie.zip::{BLADE}/The Blade Itself - Joe Abercrombie.fb2"
    assert split_member(member) == (f"{lib}/Joe Abercrombie.zip", f"{BLADE}/The Blade Itself - Joe Abercrombie.fb2")
    assert split_member("/a/b.epub") == ("/a/b.epub", None)
    evidence = extract(member)
    assert evidence["format"] == "fb2" and not evidence.get("error")
    assert evidence["meta"]["title"] == "The Blade Itself"
    gone = extract(f"{lib}/Joe Abercrombie.zip::{BLADE}/Missing.fb2")
    assert gone["format"] == "fb2" and "KeyError" in gone["error"]


def test_the_reader_unpacks_a_book_inside_a_zip_once(tmp_path, monkeypatch):
    lib, _ = _library(tmp_path)
    monkeypatch.setattr(reading, "CACHE_DIR", tmp_path / "cache")
    member = f"{lib}/Joe Abercrombie.zip::{BLADE}/The Blade Itself - Joe Abercrombie.fb2"
    path, ext, mode = reading.prepare(7, member, "FB2")
    assert (ext, mode) == ("fb2", "foliate")
    assert path.startswith(str(tmp_path / "cache")) and open(path).read() == FB2
    assert reading.prepare(7, member, "FB2")[0] == path
    missing = f"{lib}/Joe Abercrombie.zip::{BLADE}/Missing.fb2"
    try:
        reading.prepare(8, missing, "FB2")
    except reading.NotReadable as e:
        assert "no longer inside" in str(e)
    else:
        raise AssertionError("a missing member must not be readable")
