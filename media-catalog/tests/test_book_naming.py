import pytest

from books.naming import (
    is_corporate, recase_name,
    clean_stem, file_stem, find_isbns, flip_last_first, front_matter_clues, name_score,
    normalize_isbn, parse_filename, split_authors, strip_author_from_title,
)


@pytest.mark.parametrize("name", [
    "Isaac Asimov", "J. R. R. Tolkien", "Ursula K. Le Guin", "Asimov, Isaac", "Larry Niven & Jerry Pournelle",
    "Philip K. Dick", "A. Bertram Chandler", "Kristine Kathryn Rusch", "Robert A. Heinlein", "Frederik Pohl",
])
def test_names_score_as_names(name):
    assert name_score(name) > 0.5, name


@pytest.mark.parametrize("title", [
    "The Waterborn", "Towers of Ash", "Foundation and Empire", "A Wizard of Earthsea",
    "Tschai 3 - The DirDir", "The Book of Dreams", "Star Trek: The Next Generation",
    "Harry Potter and the Goblet of Fire",
])
def test_titles_score_below_names(title):
    assert name_score(title) < 0.5, title


def test_two_capitalised_words_are_only_weak_evidence():
    # Surface form cannot separate these; corpus frequency decides in the resolver.
    assert abs(name_score("Dune Messiah")) < 1.0
    assert name_score("Isaac Asimov") > name_score("Dune Messiah") + 1.0


def test_flip_last_first():
    assert flip_last_first("Asimov, Isaac") == "Isaac Asimov"
    assert flip_last_first("Vonnegut, Kurt, Jr.") == "Vonnegut, Kurt, Jr."
    assert flip_last_first("Isaac Asimov") == "Isaac Asimov"


def test_split_authors():
    assert split_authors("Larry Niven & Jerry Pournelle") == ["Larry Niven", "Jerry Pournelle"]
    assert split_authors("Robert Jordan and Brandon Sanderson") == ["Robert Jordan", "Brandon Sanderson"]
    assert split_authors("Asimov, Isaac") == ["Isaac Asimov"]


def test_file_stem_handles_double_extensions():
    assert file_stem("Book.fb2.zip") == ("Book", ".fb2.zip")
    assert file_stem("A - B.EPUB") == ("A - B", ".epub")


def test_clean_stem_strips_release_junk():
    assert clean_stem("Salt and Smoke (2018) [ePub, Mobi]")[0] == "Salt and Smoke"
    assert clean_stem("Salt and Smoke (2018) [ePub, Mobi]")[1] == 2018
    assert clean_stem("The Saxon Tales (v1.0) [dovah]")[0] == "The Saxon Tales"
    assert clean_stem("The.Name.Of.The.Wind")[0] == "The Name Of The Wind"


def test_parse_author_dash_title_is_ambiguous():
    p = parse_filename("Isaac Asimov - Foundation.epub")
    assert (p["first"], p["second"], p["ambiguous"]) == ("Isaac Asimov", "Foundation", True)


def test_parse_series_in_middle():
    p = parse_filename("Alice Arden & Bram Cole - [Silver Tide 13] - Towers of Ash (v1.0).epub")
    assert p["series"] == "Silver Tide"
    assert p["series_index"] == 13
    assert p["first"] == "Alice Arden & Bram Cole"
    assert p["second"] == "Towers of Ash"


def test_parse_series_leading_the_name():
    p = parse_filename("Kushiel 04 - Kushiel's Scion - Jacqueline Carey.pdf")
    assert (p["series"], p["series_index"]) == ("Kushiel", 4)
    assert (p["first"], p["second"], p["ambiguous"]) == ("Kushiel's Scion", "Jacqueline Carey", True)
    p = parse_filename("The Arthurian Saga 02 - The Hollow Hills - Mary Stewart.mobi")
    assert (p["series"], p["first"], p["second"]) == ("The Arthurian Saga", "The Hollow Hills", "Mary Stewart")
    # A year or a long number is not a series index.
    assert parse_filename("Fahrenheit 451 - Ray Bradbury - Unabridged.pdf")["series"] is None


def test_ebook_site_slugs_with_prefixes_and_copy_numbers():
    for name, title in [("ipad_data-mining-with-microsoft-sql-server-2008.9780470277744.49668.pdf",
                         "Data Mining with Microsoft Sql Server 2008"),
                        ("The-Productive-Programmer-Theory-in-Practice.9780596519780.34002.pdf",
                         "The Productive Programmer Theory in Practice"),
                        ("data-mining-a-heuristic-approach.9781930708259.55265 (1).pdf", "Data Mining a Heuristic Approach")]:
        p = parse_filename(name)
        assert (p["pattern"], p["first"]) == ("slug_isbn", title)
    assert parse_filename("ipad_data-mining.9780470277744.49668.pdf")["isbn"] == "9780470277744"
    assert parse_filename("41674.programming-microsoft-sql-server-2008-pro-developer.chm")["first"] == \
        "Programming Microsoft Sql Server 2008 Pro Developer"
    assert parse_filename("51994.silverlight-4-problem-design-solution-51994.pdf")["first"] == \
        "Silverlight 4 Problem Design Solution"


def test_game_lines_are_the_series_not_the_author():
    p = parse_filename("D&D 3.0 Level 3 Adventure - A Tale Manifesting.pdf")
    assert (p["first"], p["second"], p["series"], p["ambiguous"]) == \
        ("A Tale Manifesting", None, "D&D 3.0 Level 3 Adventure", False)
    assert parse_filename("Night's Dark Masters - Warhammer Fantasy Roleplay.pdf")["first"] == "Night's Dark Masters"
    p = parse_filename("Forgotten Realms - Homeland - R. A. Salvatore.epub")
    assert (p["series"], p["first"], p["second"]) == ("Forgotten Realms", "Homeland", "R. A. Salvatore")
    # A game-world novel named "Setting - Author" keeps its author.
    assert parse_filename("Dragonlance - Margaret Weis.epub")["pattern"] == "a_b"
    assert is_corporate("Ars Magica") and is_corporate("GURPS") and not is_corporate("Margaret Weis")
    assert split_authors("D&D") == ["D&D"] and split_authors("AD&D 2nd Edition") == ["AD&D 2nd Edition"]
    assert split_authors("Larry Niven & Jerry Pournelle") == ["Larry Niven", "Jerry Pournelle"]


def test_product_codes_and_book_kinds_label_a_game_file():
    p = parse_filename("Ars Magica 3rd ed - Parma Fabula - wwp 0204.pdf")
    assert (p["first"], p["series"], p["pattern"]) == ("Parma Fabula", "Ars Magica 3rd ed", "game_title")
    p = parse_filename("ICE 5805 - Rolemaster - Spell Law Of Mentalism.pdf")
    assert (p["first"], p["series"]) == ("Spell Law Of Mentalism", "Rolemaster")
    assert parse_filename("Campaigns - Hillmen of The Trollshaw.pdf")["first"] == "Hillmen of The Trollshaw"
    p = parse_filename("Rolemaster - Shadow World - Nomads of the Nine Nations.pdf")
    assert (p["first"], p["series"], p["ambiguous"]) == ("Nomads of the Nine Nations", "Shadow World", False)
    p = parse_filename("Gurps - Transhuman Space - Deep Beyond.pdf")
    assert (p["first"], p["series"]) == ("Deep Beyond", "Transhuman Space")
    for label in ("WOC 1150", "Ice 1020", "Supplement", "Warhammer 6ed", "WARHAMMER 5th", "In Nomine"):
        assert is_corporate(label), label
    assert not is_corporate("Year 2000") and parse_filename("Year 2000 - Some Author.epub")["pattern"] == "a_b"
    assert split_authors("J&F") != ["J&F"]
    assert is_corporate("Necromunda") and parse_filename("Necromunda - Desterrados.pdf")["first"] == "Desterrados"
    # Calibre's "Unknown" in a file name is no part of it.
    p = parse_filename("Cornell Woolrich - Dancing Detective - Unknown.mobi")
    assert (p["first"], p["second"]) == ("Cornell Woolrich", "Dancing Detective")


def test_front_matter_credits_name_the_writers():
    text = "FORGE OF FURY\nDesign: Richard Baker\nEditing: Miranda Horner\nCover Design: Sean Glenn"
    assert front_matter_clues(text)["bylines"] == ["Richard Baker"]
    assert front_matter_clues("Written by Jeff Grubb and Kate Novak")["bylines"] == ["Jeff Grubb", "Kate Novak"]
    assert front_matter_clues("Authors: Wizards of the Coast")["bylines"] == []
    issue = "\n".join(f"by {n}" for n in ["Mark Shirley", "Tyler Larson", "Sean Winslow", "Andrew Gronosky", "Tim Ferguson"])
    assert front_matter_clues(issue)["bylines"] == [] and front_matter_clues(issue)["contributors"]
    # Scanned rulebooks set bylines in capitals.
    assert front_matter_clues("ANGELIC PLAYER'S GUIDE\nBy DEREK PEARCY\nBy S. JOHN Ross")["bylines"] == \
        ["Derek Pearcy", "S. John Ross"]


def test_parse_title_by_author():
    p = parse_filename("Salt and Smoke by Helen Marsh [ePub, Mobi].zip")
    assert (p["first"], p["second"], p["ambiguous"]) == ("Salt and Smoke", "Helen Marsh", False)


def test_parse_book_prefix():
    p = parse_filename("(Book 4) J. K. Rowling - Harry Potter and the Goblet of Fire.pdf")
    assert p["series_index"] == 4
    assert p["first"] == "J. K. Rowling"


def test_parse_calibre_filename_strips_id():
    p = parse_filename("The Waterborn - J. Gregory Keyes.rar")
    assert (p["first"], p["second"]) == ("The Waterborn", "J. Gregory Keyes")


@pytest.mark.parametrize("raw,expected", [
    ("0-441-17271-7", "9780441172719"),
    ("978-0-441-17271-9", "9780441172719"),
    ("ISBN 0441172717", "9780441172719"),
    ("0-441-17271-8", None),
    ("1234567890123", None),
])
def test_normalize_isbn(raw, expected):
    assert normalize_isbn(raw) == expected


def test_find_isbns_prefers_labelled():
    text = "Printed in 1990 0-441-17271-7 something\nISBN: 978-0-553-29335-7 First edition"
    found = find_isbns(text)
    assert found[0]["isbn"] == "9780553293357" and found[0]["labelled"]
    assert {f["isbn"] for f in found} == {"9780553293357", "9780441172719"}


def test_front_matter_clues():
    text = "FOUNDATION\n\nby\n\nIsaac Asimov\n\nCopyright © 1951 by Isaac Asimov. All rights reserved.\nISBN 0-553-29335-4"
    clues = front_matter_clues(text)
    assert clues["bylines"] == ["Isaac Asimov"]
    assert clues["title_page"] == "FOUNDATION"
    assert clues["copyright"][0] == {"name": "Isaac Asimov", "year": 1951}


def test_front_matter_title_by_author_on_one_line():
    clues = front_matter_clues("THE EMPIRE OF GLASS by ANDY LANE\nPROLOGUE\nJuly 1587")
    assert clues["bylines"] == ["Andy Lane"] and clues["title_page"] == "THE EMPIRE OF GLASS"
    # Prose is not a title page.
    assert front_matter_clues("He was the man who was killed by John Smith")["bylines"] == []


def test_book_design_and_map_credits_are_not_the_author():
    page = ("THE RUM DIARY\nDesigned by Anita Karl and Jim Kemp\n"
            "Maps copyright © 1998 by Anita Karl and Jim Kemp ISBN 0-684-85521-6\n"
            "Copyright © 1998 by Hunter S. Thompson\nAll rights reserved.")
    clues = front_matter_clues(page)
    assert clues["bylines"] == []
    assert [c["name"] for c in clues["copyright"]] == ["Hunter S. Thompson"]
    assert front_matter_clues("Copyright 2001 by Jane Doe ISBN 0-441-00001-1")["copyright"][0]["name"] == "Jane Doe"


def test_names_in_one_case_are_recased():
    assert [recase_name(n) for n in ("hunter s thompson", "J.R.R. TOLKIEN", "O'BRIEN", "S. JOHN Ross",
                                     "Ursula K. Le Guin", "JEAN-LUC GODARD")] == \
        ["Hunter S Thompson", "J.R.R. Tolkien", "O'Brien", "S. John Ross", "Ursula K. Le Guin", "Jean-Luc Godard"]


def test_comma_separated_full_names_are_a_list():
    assert split_authors("Matt Drance, Paul Warren") == ["Matt Drance", "Paul Warren"]
    assert split_authors("Le Guin, Ursula K.") == ["Ursula K. Le Guin"]
    assert split_authors("Garcia Marquez, Gabriel") == ["Gabriel Garcia Marquez"]


def test_editor_roles_are_stripped():
    assert split_authors("Edited By Damon Knight") == ["Damon Knight"]
    assert split_authors("Damon Knight (ed.)") == ["Damon Knight"]


def test_a_writer_may_share_a_publishers_name():
    for writer in ("Nancy Springer", "Ridley Pearson", "Kathleen O'Reilly", "M. N. Pearson", "SARRA MANNING",
                   "O'Reilly, Bill", "Bill O'Reilly & Martin Dugard", "Mark Pearson & Mark"):
        assert not is_corporate(writer), writer
    for publisher in ("Springer", "Springer-Verlag", "John Wiley", "John Wiley & Sons", "Wiley-Interscience",
                      "O'Reilly", "O'Reilly & Associates", "Manning", "Pearson Prentice Hall"):
        assert is_corporate(publisher), publisher


def test_publishers_and_joined_strings_do_not_score_as_names():
    assert name_score("Packt Publishing") < 0
    assert name_score("Metaphysics - Aristotle") < 0


def test_slug_isbn_download_names():
    p = parse_filename("pro-hadoop 9781430219422 46458.pdf")
    assert (p["first"], p["isbn"], p["pattern"]) == ("Pro Hadoop", "9781430219422", "slug_isbn")
    p = parse_filename("introduction-to-genetic-algorithms.9783540731894.55286.pdf")
    assert p["first"] == "Introduction to Genetic Algorithms"


def test_bare_isbn_filename_yields_isbn():
    assert parse_filename("0-06-059896-4.pdf")["isbn"] == "9780060598969"


def test_repeated_author_part_and_list_number():
    p = parse_filename("Metaphysics - Aristotle - Aristotle.mobi")
    assert (p["first"], p["second"], p["pattern"]) == ("Metaphysics", "Aristotle", "a_b")
    p = parse_filename("3. Life, the Universe, and Everything - Douglas Adams.mobi")
    assert (p["first"], p["series_index"]) == ("Life, the Universe, and Everything", 3.0)
    assert parse_filename("1984 - George Orwell.epub")["first"] == "1984"


def test_author_joined_onto_title_is_removed():
    assert strip_author_from_title("Metaphysics - Aristotle", ["Aristotle"]) == "Metaphysics"
    assert strip_author_from_title("Orbit 4 - Anthology", ["Damon Knight"]) == "Orbit 4 - Anthology"


def test_a_product_code_leading_a_part_is_dropped():
    r = parse_filename("ICE6503P Rolemaster Classic - Spell Law.pdf")
    assert (r["first"], r["second"], r["series"]) == ("Spell Law", None, "Rolemaster Classic")


def test_chm_author_fields_with_the_spaces_lost():
    assert split_authors("byBrian KnightandErik Veerman") == ["Brian Knight", "Erik Veerman"]
    assert split_authors("Ferdinand Mount") == ["Ferdinand Mount"]


def test_copyrighted_material_is_not_a_holder():
    assert front_matter_clues("Copyrighted material\nCopyright 2001 by Jane Doe.")["copyright"] == [
        {"name": "Jane Doe", "year": 2001}]


def test_a_bare_number_part_is_the_series_index():
    r = parse_filename("3 - The Mammoth Hunters - Jean M. Auel.pdf")
    assert (r["first"], r["second"], r["series_index"]) == ("The Mammoth Hunters", "Jean M. Auel", 3.0)
    r = parse_filename("War of the Spider Queen - 1 - Dissolution - R. A. Salvatore.pdf")
    assert (r["series"], r["series_index"], r["first"], r["second"]) == (
        "War of the Spider Queen", 1.0, "Dissolution", "R. A. Salvatore")
    r = parse_filename("Gentry Lee - 01 - Bright Messengers.pdf")
    assert (r["first"], r["second"], r["series_index"], r["ambiguous"]) == ("Gentry Lee", "Bright Messengers", 1.0, True)
    r = parse_filename("The History of Middle-Earth - 11 - J.R.R. Tolkien.pdf")
    assert (r["first"], r["second"], r["series_index"]) == ("The History of Middle-Earth", "J.R.R. Tolkien", 11.0)


def test_a_bare_hyphen_after_a_full_name():
    r = parse_filename("Charles L. Harness-The Alchemist.epub")
    assert (r["first"], r["second"]) == ("Charles L. Harness", "The Alchemist")
    assert parse_filename("Spider-Man Blue.cbr")["first"] == "Spider-Man Blue"


def test_a_degree_after_a_name_is_dropped():
    assert split_authors("Edward E. Smith, Ph.D") == ["Edward E. Smith"]
    assert split_authors("Lita Epstein, MBA") == ["Lita Epstein"]


def test_credits_set_in_two_columns():
    page = ("THE TEMPLE\nOF REDCLIFF\nCREDITS\nDesign:\nEditing:\nTypesetting:\nCartography:\n"
            "Eric Cagle\nPenny Williams\nNancy Walker\nRob Lazzaretti\nBased on the original game")
    clues = front_matter_clues(page)
    assert clues["bylines"] == ["Eric Cagle"] and clues["credited"]
    page = "CREDITS\nDesign:\nRobert Wiese\nEditing:\nPenny Williams\nTypesetting:\nNancy Walker"
    assert front_matter_clues(page)["bylines"] == ["Robert Wiese"]
    page = "CREDITS\n0&0 Lead Designers:\nMike Mearls,\nWriting:\nJames\nGraphic\nDesigners:\nBree Heiss,"
    assert front_matter_clues(page)["bylines"] == ["Mike Mearls"]


def test_a_credits_list_cut_off_at_the_end_of_a_line():
    assert split_authors("Steve Miller, Anthony Pryor, Penny") == ["Steve Miller", "Anthony Pryor"]
    assert split_authors("Le Guin, Ursula K.") == ["Ursula K. Le Guin"]
