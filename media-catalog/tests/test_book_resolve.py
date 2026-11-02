from collections import Counter

import pytest

from books.resolve import Context, _similar_title, resolve_unit


def ctx_with(authors: dict) -> Context:
    ctx = Context()
    ctx.author_freq = Counter(authors)
    return ctx


def calibre_unit(title, authors, tags=(), isfdb=False, files=()):
    return {
        "calibre": {"library": "SFF 1990s", "title": title, "authors": list(authors), "tags": list(tags),
                    "isfdb": isfdb, "identifiers": {}, "formats": []},
        "calibre_dir": True, "directory": "/lib/X/Y (1)", "cover_path": None, "files": list(files),
    }


def epub(title, authors, group="publisher", text=None):
    return {"file_path": "/lib/X/Y (1)/b.epub", "format": "epub", "present": True, "size_bytes": 1,
            "evidence": {"format": "epub", "meta": {"title": title, "authors": authors}, "meta_group": group,
                         "text": text, "filename": {}}}


def test_swapped_calibre_record_is_corrected_by_corpus_frequency():
    ctx = ctx_with({"isaac asimov": 80})
    r = resolve_unit(calibre_unit("Isaac Asimov", ["Foundation"]), ctx)
    assert r["title"] == "Foundation"
    assert r["authors"] == ["Isaac Asimov"]
    assert r["swapped"]


def test_swapped_title_is_rebuilt_from_the_raw_author_field():
    # Calibre split the title "People, Places & Things" on '&' when it stored it as authors.
    ctx = ctx_with({"stephen king": 80})
    r = resolve_unit(calibre_unit("Stephen King", ["People, Places", "Things"]), ctx)
    assert (r["title"], r["authors"], r["swapped"]) == ("People, Places & Things", ["Stephen King"], True)


def test_correct_calibre_record_is_kept():
    ctx = ctx_with({"isaac asimov": 80})
    r = resolve_unit(calibre_unit("Foundation", ["Isaac Asimov"]), ctx)
    assert (r["title"], r["authors"], r["swapped"]) == ("Foundation", ["Isaac Asimov"], False)


def test_isfdb_record_is_never_swapped():
    ctx = ctx_with({"dune messiah": 50})
    r = resolve_unit(calibre_unit("Dune Messiah", ["Frank Herbert"], isfdb=True), ctx)
    assert r["title"] == "Dune Messiah" and not r["swapped"]


def test_publisher_metadata_overrules_swapped_calibre():
    ctx = ctx_with({})
    unit = calibre_unit("Keith Laumer", ["The Time Bender"], files=[epub("The Time Bender", ["Keith Laumer"])])
    r = resolve_unit(unit, ctx)
    assert r["title"] == "The Time Bender"
    assert r["authors"] == ["Keith Laumer"]
    assert r["swapped"]


def test_calibre_embedded_copy_is_not_a_second_vote():
    ctx = ctx_with({})
    unit = calibre_unit("Keith Laumer", ["The Time Bender"],
                        files=[epub("Keith Laumer", ["The Time Bender"], group="calibre")])
    r = resolve_unit(unit, ctx)
    sources = [c["source"] for c in r["provenance"]["candidates"]]
    assert sources == ["calibre:SFF 1990s"]


def test_front_matter_byline_supports_the_author():
    ctx = ctx_with({})
    text = {"bylines": ["Christopher Hitchens"], "copyright": [], "title_page": "god is not great", "isbns": []}
    unit = {"calibre": None, "calibre_dir": False, "directory": "/lib/Books", "cover_path": None,
            "files": [{"file_path": "/lib/Books/God is not Great - Christopher Hitchens.pdf", "format": "pdf",
                       "present": True, "size_bytes": 1,
                       "evidence": {"format": "pdf", "meta": {"title": None, "authors": ["Asim"]},
                                    "meta_group": "document", "text": text}}]}
    r = resolve_unit(unit, ctx)
    assert r["title"] == "God is not Great"
    assert r["authors"] == ["Christopher Hitchens"]


def test_awards_split_out_of_subjects():
    ctx = ctx_with({})
    r = resolve_unit(calibre_unit("The Forever War", ["Joe Haldeman"],
                                  tags=["Novel", "_isfdb", "Win of 1976 Hugo Award for Best Novel"]), ctx)
    assert r["subjects"] == ["Novel"]
    assert [(a["award"], a["result"], a["year"]) for a in r["awards"]] == [("Hugo Award", "winner", 1976)]


def test_title_with_author_suffix_is_not_swapped():
    ctx = ctx_with({"aristotle": 30})
    unit = {"calibre": None, "calibre_dir": False, "directory": "/lib/Books/Aristotle/Metaphysics - Aristotle (297)",
            "cover_path": None,
            "files": [{"file_path": "/lib/Books/Aristotle/Metaphysics - Aristotle (297)/Metaphysics - Aristotle - Aristotle.mobi",
                       "format": "mobi", "present": True, "size_bytes": 1,
                       "evidence": {"format": "mobi", "meta": {"title": "Metaphysics - Aristotle", "authors": ["Aristotle"]},
                                    "meta_group": "calibre", "text": None}}]}
    r = resolve_unit(unit, ctx)
    assert (r["title"], r["authors"], r["swapped"]) == ("Metaphysics", ["Aristotle"], False)


def test_publisher_in_pdf_author_field_is_dropped_and_filename_isbn_kept():
    ctx = ctx_with({})
    unit = {"calibre": None, "calibre_dir": False, "directory": "/lib/Books", "cover_path": None,
            "files": [{"file_path": "/lib/Books/android-ndk-beginners-guide 9781849691529 57448.pdf", "format": "pdf",
                       "present": True, "size_bytes": 1,
                       "evidence": {"format": "pdf", "meta": {"title": None, "authors": ["Packt Publishing"]},
                                    "meta_group": "document", "text": None}}]}
    r = resolve_unit(unit, ctx)
    assert r["title"] == "Android Ndk Beginners Guide"
    assert r["authors"] == [] and r["isbn"] == "9781849691529"
    assert not r["needs_review"]


def mobi(title, authors, group="publisher"):
    return {"file_path": "/lib/X/Y (1)/b.mobi", "format": "mobi", "present": True, "size_bytes": 1,
            "evidence": {"format": "mobi", "meta": {"title": title, "authors": authors}, "meta_group": group,
                         "text": None}}


def test_uploader_handle_does_not_outvote_calibre_and_mojibake_is_repaired():
    ctx = ctx_with({})
    unit = calibre_unit("The Book of Philip JosÃ© Farmer", ["Philip JosÃ© Farmer"],
                        files=[mobi("Philip Jose Farmer - The Book of Philip Jose Farmer", ["casey ostrander"])])
    r = resolve_unit(unit, ctx)
    assert r["title"] == "The Book of Philip José Farmer"
    assert r["authors"] == ["Philip José Farmer"]
    assert not r["needs_review"]


def test_near_matching_file_metadata_is_not_a_disagreement():
    ctx = ctx_with({})
    r = resolve_unit(calibre_unit("Orbit 5", ["Damon Knight"],
                                  files=[epub("Orbit 5 - [Anthology]", ["Edited by Damon Night"])]), ctx)
    assert r["authors"] == ["Damon Knight"] and not r["needs_review"]


def test_crossed_file_metadata_is_not_a_disagreement():
    ctx = ctx_with({"john berryman": 3})
    r = resolve_unit(calibre_unit("The Right Time", ["John Berryman"],
                                  files=[mobi("John Berryman", ["The Right Time"])]), ctx)
    assert (r["title"], r["authors"]) == ("The Right Time", ["John Berryman"])
    assert not r["needs_review"]


def test_real_disagreement_is_flagged():
    ctx = ctx_with({})
    r = resolve_unit(calibre_unit("Rendezvous with Rama", ["Arthur C. Clarke"],
                                  files=[epub("The Left Hand of Darkness", ["Ursula K. Le Guin"])]), ctx)
    assert r["needs_review"] and "disagree" in r["review_reason"]


def test_calibre_escaped_comma_in_author_name():
    r = resolve_unit(calibre_unit("Slapstick", ["Kurt Vonnegut| Jr."]), ctx_with({}))
    assert r["authors"] == ["Kurt Vonnegut, Jr"]


@pytest.mark.parametrize("a,b,same", [
    ("Orbit 4", "Orbit 5", False),
    ("Orbit 4", "Orbit 41", False),
    ("Dune", "Dune Messiah", False),
    ("It", "Spirit", False),
    ("Subspace Survivors", "Subspace Survivors: The Classic SF Novelette", True),
    ("Orbit 5 - [Anthology]", "Orbit 5", True),
    ("God Bless You, Mr. Rosewater", "God Bless You, Mr. Rosewater or Pearls Before Swine", True),
    ("Anything You Can Do! (Part 1 of 2)", "Anything You Can Do!", True),
    ("The Left Hand of Darkness", "The Left Hand of Darkenss", True),
])
def test_similar_title(a, b, same):
    assert _similar_title(a, b) is same


def test_curated_calibre_author_list_beats_file_contributor_list():
    ctx = ctx_with({})
    r = resolve_unit(calibre_unit("Robots", ["Isaac Asimov", "Martin H. Greenberg", "Charles G. Waugh"],
                                  files=[epub("Isaac Asimov's Worlds of Science Fiction. Book 9: Robots",
                                              ["Frederik Pohl", "Henry Slesar", "Isaac Asimov"])]), ctx)
    assert r["title"] == "Robots"
    assert r["authors"] == ["Isaac Asimov", "Martin H. Greenberg", "Charles G. Waugh"]
    assert not r["needs_review"]


def test_coauthor_list_is_not_a_disagreement():
    ctx = ctx_with({})
    r = resolve_unit(calibre_unit("Republic and Empire", ["Jerry Pournelle", "John F. Carr"],
                                  files=[mobi("Imperial Stars 2-Republic and Empire", ["Jerry Pournelle"])]), ctx)
    assert (r["title"], r["needs_review"]) == ("Republic and Empire", False)


def test_isbn_filename_is_only_a_last_resort_title():
    pdf = {"file_path": "/lib/Books/185984054X.pdf", "format": "pdf", "present": True, "size_bytes": 1,
           "evidence": {"format": "pdf", "meta": {"title": "The Missionary Position", "authors": ["Christopher Hitchens"]},
                        "meta_group": "document", "text": None, "filename": {}}}
    unit = {"calibre": None, "calibre_dir": False, "directory": "/lib/Books", "cover_path": None, "files": [pdf]}
    r = resolve_unit(unit, ctx_with({}))
    assert (r["title"], r["authors"], r["isbn"]) == ("The Missionary Position", ["Christopher Hitchens"], "9781859840542")
    bare = dict(pdf, file_path="/lib/Books/0-06-059896-4.pdf", evidence=dict(pdf["evidence"], meta=None, meta_group=None))
    assert resolve_unit(dict(unit, files=[bare]), ctx_with({}))["title"] == "0-06-059896-4"


def test_layout_and_temp_file_names_are_not_titles():
    from books.formats import is_junk_title
    for junk in ("Acr34B.tmp", "AcrC1E1.TMP", "UNAPPROACHABLE EAST.qxd", "Feast of the Gobbler.indd", "Untitled-1.p65",
                 "DjVu PostScript document", "default", "Administrator", "Roleplay Adventure Record Sheet.ai"):
        assert is_junk_title(junk), junk
    for title in ("Manual of the Planes", "The Sandman Vol. 1", "Dr. Jekyll and Mr. Hyde"):
        assert not is_junk_title(title), title


def test_calibre_unknown_author_records_split_title_and_author():
    ctx = ctx_with({"danielle steel": 30, "jim kelly": 3})
    r = resolve_unit(calibre_unit("Changes - Danielle Steel", ["Unknown"]), ctx)
    assert (r["title"], r["authors"]) == ("Changes", ["Danielle Steel"])
    r = resolve_unit(calibre_unit("Fire Baby, The - Jim Kelly", ["Unknown"]), ctx)
    assert (r["title"], r["authors"]) == ("The Fire Baby", ["Jim Kelly"])
    r = resolve_unit(calibre_unit("Cornell Woolrich - Dancing Detective", ["Unknown"]), ctx_with({"cornell woolrich": 6}))
    assert (r["title"], r["authors"]) == ("Dancing Detective", ["Cornell Woolrich"])
    r = resolve_unit(calibre_unit("Margaret Carter- Foxfire", ["Unknown"]), ctx_with({}))
    assert (r["title"], r["authors"]) == ("Foxfire", ["Margaret Carter"])
    assert resolve_unit(calibre_unit("Hitler's Niece - Hansen", ["Unknown"]), ctx_with({}))["title"] != "Hansen"
    r = resolve_unit(calibre_unit("__The Joy Luck Club - Amy Tan", ["Unknown"]), ctx_with({"amy tan": 3}))
    assert r["title"] == "The Joy Luck Club"
    # A title that merely contains a dash keeps it.
    r = resolve_unit(calibre_unit("Star Wars - The Clone Wars", ["Karen Traviss"]), ctx_with({"karen traviss": 5}))
    assert r["title"] == "Star Wars - The Clone Wars"


def test_a_lowercase_author_is_shown_capitalised():
    r = resolve_unit(calibre_unit("The Rum Diary", ["hunter s thompson"]), ctx_with({"hunter s thompson": 4}))
    assert r["authors"] == ["Hunter S Thompson"]


def test_stored_copyright_names_cut_at_an_initial_or_running_into_an_isbn_are_cleaned():
    text = {"bylines": [], "title_page": None, "copyright": [{"name": "Hunter S", "year": 1998},
                                                            {"name": "Jane Doe ISBN", "year": 1998}]}
    unit = {"calibre": None, "calibre_dir": False, "directory": "/lib/x", "cover_path": None,
            "files": [epub("Untitled Thing", [], group="document", text=text)]}
    r = resolve_unit(unit, ctx_with({}))
    assert r["authors"] == ["Jane Doe"]


def test_scanners_and_formatters_are_not_authors():
    from books.resolve import _clean_authors
    assert _clean_authors(["Scanned and PDF by ExampleUser", "Formatting By: uploader@example.Com", "Bracers CERT v1.01",
                           "WoT.opd", "Unknown", "EPC Crew", "ebook", "IGI Global", "Character Sheet",
                           "Paramount Pictures", "Pocket Books", "Monte Cook"]) == ["Monte Cook"]


def loose_unit(name, files):
    return {"calibre": None, "calibre_dir": False, "directory": "/lib/x", "cover_path": None,
            "files": [dict(f, file_path=f"/lib/x/{name}") for f in files]}


def test_title_page_lines_that_are_not_bylines():
    def unit(name, bylines=(), copyright=()):
        text = {"bylines": list(bylines), "title_page": None, "copyright": [{"name": n, "year": None} for n in copyright]}
        return loose_unit(name, [dict(epub(None, [], group="document", text=text), evidence={
            "format": "pdf", "meta": None, "meta_group": None, "text": text})])
    ctx = ctx_with({})
    assert resolve_unit(unit("Fallen Heroes.pdf", ["Chapter One"]), ctx)["authors"] == []
    assert resolve_unit(unit("Expert SSIS.pdf", copyright=["Clearance Center", "ed material"]), ctx)["authors"] == []
    assert resolve_unit(unit("Sea Lords of Gondor.pdf", copyright=["TOLKIEN ENTERPRISES"]), ctx)["authors"] == []
    # A line that repeats the title, and the staff list of a dated magazine issue.
    assert resolve_unit(unit("ICE5522 - Rolemaster - Spell Law.pdf", ["SPELL LAW"]), ctx)["authors"] == []
    r = resolve_unit(unit("Roger Zelazny- Three Descents.pdf", ["Roger Zelazny"]), ctx)
    assert r["authors"] == ["Roger Zelazny"]
    assert resolve_unit(unit("New Scientist 2009-01-17.pdf", ["Craig Mackie"]), ctx)["authors"] == []
    assert resolve_unit(unit("Hermes Portal.pdf", ["Craig Mackie"]), ctx)["authors"] == ["Craig Mackie"]


def test_a_lowercase_record_is_still_turned_round():
    kindle = mobi("hunter s thompson - hell's angels", ["Hell's Angels"], group="calibre")
    r = resolve_unit(loose_unit("Thompson, Hunter S. - Hell's Angels.mobi", [kindle]), ctx_with({}))
    assert (r["title"], r["authors"]) == ("Hell's Angels", ["Hunter S. Thompson"])


def test_a_one_word_pdf_author_is_a_login():
    pdf = dict(epub("Spell Law", ["Morgan"], group="document"), file_path="/lib/x/Spell Law.pdf")
    assert resolve_unit(loose_unit("Spell Law.pdf", [pdf]), ctx_with({}))["authors"] == []
    pdf = dict(epub("Guide", ["a.b.example"], group="document"), file_path="/lib/x/Guide.pdf")
    assert resolve_unit(loose_unit("Guide.pdf", [pdf]), ctx_with({}))["authors"] == []
    pdf = dict(epub("Spell Law", ["Coleman Charlton"], group="document"), file_path="/lib/x/Spell Law.pdf")
    assert resolve_unit(loose_unit("Spell Law.pdf", [pdf]), ctx_with({}))["authors"] == ["Coleman Charlton"]


def test_a_shouted_spelling_does_not_replace_a_good_one():
    pdf = dict(epub("SHADOW LEADER", [], group="document"), file_path="/lib/x/a.pdf")
    r = resolve_unit(loose_unit("Shadow Leader - Tara K. Harper.pdf", [pdf]), ctx_with({"tara k. harper": 9}))
    assert (r["title"], r["authors"]) == ("Shadow Leader", ["Tara K. Harper"])
    pdf = dict(epub("F O U R", [], group="document"), file_path="/lib/x/a.pdf")
    assert resolve_unit(loose_unit("Dissolution.pdf", [pdf]), ctx_with({}))["title"] == "Dissolution"



def test_a_franchise_gives_way_to_the_writer():
    pdf = dict(epub("Doctor Who - Fury From The Deep", ["Victor Pemberton"], group="document"), file_path="/lib/x/a.pdf")
    r = resolve_unit(loose_unit("Doctor Who - 041 - Fury From the Deep.pdf", [pdf]), ctx_with({}))
    assert r["authors"] == ["Victor Pemberton"]
    bare = {"format": "pdf", "present": True, "size_bytes": 1, "evidence": {}}
    r = resolve_unit(loose_unit("STAR TREK - 041 - Fury From the Deep.pdf", [bare]), ctx_with({}))
    assert r["authors"] == ["Star Trek"]     # with nothing better it still files the book


def test_title_page_names_are_tidied():
    text = {"bylines": [": GORDON EKLUND and GREGORY BENFORD,"], "title_page": None, "copyright": []}
    pdf = {"format": "pdf", "present": True, "size_bytes": 1,
           "evidence": {"format": "pdf", "meta": None, "meta_group": None, "text": text}}
    r = resolve_unit(loose_unit("If the Stars Are Gods.pdf", [pdf]), ctx_with({}))
    assert r["authors"] == ["Gordon Eklund", "Gregory Benford"]


def lookup_ctx(title, names, subtitle=None):
    ctx = ctx_with({})
    ctx.lookups = {"9780672329814": {"title": title, "subtitle": subtitle, "authors": [{"name": n} for n in names]}}
    return ctx


def test_a_catalogue_record_names_the_author_when_its_title_agrees():
    bare = {"format": "pdf", "present": True, "size_bytes": 1, "evidence": {}}
    unit = loose_unit("c-3-0-unleashed-with-the-net-framework-3-5.9780672329814.1234.pdf", [bare])
    assert resolve_unit(unit, lookup_ctx("C♯ 3.0 unleashed", ["Joseph Mayo"], "with the .NET Framework 3.5")
                        )["authors"] == ["Joseph Mayo"]
    # Another book's ISBN: a different title names nobody.
    assert resolve_unit(unit, lookup_ctx("Russian for dummies", ["Andrew Kaufman"]))["authors"] == []
    # Organisations and repeated, inverted entries are not authors.
    assert resolve_unit(unit, lookup_ctx("C# 3.0 Unleashed", ["Mayo, Joseph, J.", "Joseph Mayo", "Sams Team"])
                        )["authors"] == ["Joseph Mayo"]
    assert resolve_unit(unit, lookup_ctx("C# 3.0 Unleashed", ["LearningExpress (Organization)"]))["authors"] == []


def test_a_catalogue_record_does_not_cut_the_books_own_author_list():
    pdf = dict(epub("Text Mining", ["Michael W. Berry", "Jacob Kogan"], group="document"), file_path="/lib/x/a.pdf")
    unit = loose_unit("text-mining.9780672329814.1.pdf", [pdf])
    r = resolve_unit(unit, lookup_ctx("Text mining", ["Michael J. A. Berry"], "applications and theory"))
    assert r["authors"] == ["Michael W. Berry", "Jacob Kogan"]
    # A franchise in the author's place gives way to the record's writer.
    text = {"bylines": [], "copyright": [], "title_page": None, "isbns": ["9780672329814"]}
    unit = loose_unit("Star Trek - Requiem.pdf", [{"format": "pdf", "present": True, "size_bytes": 1, "evidence": {
        "format": "pdf", "meta": None, "meta_group": None, "text": text}}])
    r = resolve_unit(unit, lookup_ctx("Requiem: Excalibur Book One", ["Peter David"], "Star Trek: New Frontier #9"))
    assert r["authors"] == ["Peter David"]
