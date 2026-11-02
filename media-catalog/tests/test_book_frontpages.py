"""Title-page clues read from the first pages of books that lack an author."""
from books import frontpages

KNOWN = {"anne mccaffrey", "robert frost", "stephenie meyer"}


def test_a_known_author_on_the_title_page_is_a_byline():
    clues = frontpages.clues_from("DRAGONFLIGHT\nAnne McCaffrey\nBallantine Books", [], KNOWN)
    assert clues["bylines"] == ["Anne McCaffrey"]


def test_a_full_name_is_taken_only_when_nothing_else_names_the_author():
    clues = frontpages.clues_from("All the Mathematics You Missed\nThomas A. Garrity\nCambridge University Press",
                                  [], KNOWN)
    assert clues["bylines"] == ["Thomas A. Garrity"]
    clues = frontpages.clues_from("Dune\nby Frank Herbert\nThomas A. Garrity", [], KNOWN)
    assert clues["bylines"] == ["Frank Herbert"]
    # Publisher, edition and catalogue lines are not people.
    assert frontpages.clues_from("I. Title.\nU.S. Corporate and Government Sales", [], KNOWN)["bylines"] == []


def test_weak_bylines_only_confirm_a_book_that_has_an_author():
    epigraph = "ECLIPSE\nFire and Ice\nSome say the world will end in fire\nRobert Frost"
    current = ["Twilight 03", "Eclipse - Stephenie Meyer"]
    assert frontpages.clues_from(epigraph, [], KNOWN, current)["bylines"] == []
    assert frontpages.clues_from("Stephenie Meyer\nEclipse", [], KNOWN, current)["bylines"] == ["Stephenie Meyer"]
    # Blurb attributions are skipped.
    assert frontpages.clues_from("Praise\n—Robert Frost\nEclipse", [], KNOWN)["bylines"] == []


def test_contributors_to_a_magazine_are_not_its_authors():
    issue = "\n".join(f"Article {i}\nby {n}" for i, n in enumerate(
        ["Mark Shirley", "Tyler Larson", "Sean Winslow", "Andrew Gronosky", "Timothy Ferguson"]))
    assert frontpages.clues_from("Hermes Portal 10\nShirley, Sean Michael Winslow\n" + issue, [], KNOWN)["bylines"] == []
    credits = "Manual of the Planes\nDesign: Jeff Grubb, Bruce R. Cordell, David Noonan"
    assert frontpages.clues_from(credits, [], KNOWN)["bylines"] == ["Jeff Grubb", "Bruce R. Cordell", "David Noonan"]


def test_merge_adds_clues_without_replacing_the_scans():
    evidence = {"format": "pdf", "meta": {"title": "X", "authors": ["harry"]}, "meta_group": "document",
                "text": {"bylines": [], "copyright": [{"name": "Thomas A. Garrity", "year": 2001}],
                         "title_page": None, "isbns": ["9780521797078"], "chars": 900}}
    found = {"text": "Thomas A. Garrity\nAll the Mathematics You Missed", "headlines": [], "source": "pdf-text",
             "meta": {"title": "Other", "authors": ["Someone"]}}
    ev = frontpages.merge(evidence, found, KNOWN)
    assert ev["text"]["bylines"] == ["Thomas A. Garrity"]
    assert ev["text"]["copyright"] == evidence["text"]["copyright"] and ev["text"]["isbns"] == ["9780521797078"]
    assert ev["meta"] == evidence["meta"]                     # the scan's metadata wins
    assert ev["frontpages"] == {"source": "pdf-text", "chars": len(found["text"])}
    bare = frontpages.merge({"format": "lrf", "meta": None}, dict(found, source="calibre"), KNOWN)
    assert bare["meta"]["authors"] == ["Someone"] and bare["meta_group"] == "document"


def test_a_credits_block_outranks_other_names_on_the_page():
    page = "CREDITS\nDesign:\nEditing:\nTypesetting:\nEric Cagle\nPenny Williams\nNancy Walker"
    assert frontpages.clues_from(page, [], {"nancy walker"})["bylines"] == ["Eric Cagle"]
    old = {"format": "pdf", "text": {"bylines": ["Nancy Walker"], "copyright": [], "title_page": None, "isbns": []}}
    found = {"text": page, "headlines": [], "source": "pdf-text", "meta": None}
    assert frontpages.merge(old, found, set(), credits_win=True)["text"]["bylines"] == ["Eric Cagle"]
    assert frontpages.merge(old, found, set())["text"]["bylines"] == ["Nancy Walker", "Eric Cagle"]
