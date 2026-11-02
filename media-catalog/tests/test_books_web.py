from books.formats import sanitize_json
from web.books import books_url, build_where, filter_chips, goodreads_url
from web.common import map_path, parse_network_mounts, pick_sort


def test_default_where_lists_books_on_disk_and_references():
    where, args = build_where({})
    assert where == "NOT b.hidden" and args == []


def test_status_selects_on_disk_or_works_with_no_copy_on_disk():
    assert build_where({"status": "available"})[0] == "NOT b.hidden AND b.available"
    where = build_where({"status": "missing"})[0]
    assert "NOT b.available AND NOT EXISTS" in where and "b2.work_key = b.work_key AND b2.available" in where


def test_array_filters_use_containment_and_award_result_combines():
    where, args = build_where({"author": "Ursula K. Le Guin", "award": "Hugo Award", "award_result": "winner",
                               "format": "epub"})
    assert "b.authors @> ARRAY[%s]::text[]" in where
    assert "b.formats @> ARRAY[%s]::text[]" in where
    assert "a.award = %s AND a.result = %s" in where
    assert args == ["Ursula K. Le Guin", "EPUB", "Hugo Award", "winner"]


def test_any_award_winner_and_decade():
    where, args = build_where({"award": "any", "award_result": "winner", "decade": "1960"})
    assert "a.award" not in where and "a.result = %s" in where
    assert args == ["winner", 1960, 1969]


def test_junk_numbers_are_ignored():
    where, args = build_where({"min_rating": "lots", "year_from": "x", "decade": ""})
    assert where == "NOT b.hidden" and args == []


def test_skip_drops_one_category_filter():
    where, args = build_where({"author": "A", "series": "S"}, skip=("author",))
    assert "authors" not in where and args == ["S"]


def test_books_url_applies_and_removes_changes():
    params = {"author": "Frank Herbert", "sort": "year", "junk": "x"}
    assert books_url(params, series="Dune") == "/books?author=Frank+Herbert&sort=year&series=Dune"
    assert books_url(params, author=None, sort=None) == "/books"


def test_filter_chips_remove_their_own_filter():
    chips = filter_chips({"author": "Frank Herbert", "starred": "1"})
    assert [(c["label"], c["value"]) for c in chips] == [("Author", "Frank Herbert"), ("Starred", "yes")]
    assert chips[0]["remove"] == "/books?starred=1"


def test_goodreads_link_prefers_id_then_isbn_then_search():
    assert goodreads_url({"goodreads_id": "234225"}) == "https://www.goodreads.com/book/show/234225"
    assert goodreads_url({"isbn": "9780441013593"}) == "https://www.goodreads.com/book/isbn/9780441013593"
    assert goodreads_url({"title": "Dune", "authors": ["Frank Herbert"]}) == \
        "https://www.goodreads.com/search?q=Dune+Frank+Herbert"


def test_pick_sort_falls_back_to_default():
    options = {"title": "t", "year": "y"}
    assert pick_sort("year", "title", options) == ("year", "y")
    assert pick_sort("drop table", "title", options) == ("title", "t")


def test_network_mounts_split_on_volume_paths(monkeypatch):
    monkeypatch.setenv("NETWORK_MOUNTS", "/Volumes/Library=smb://h._smb._tcp.local/Library"
                                         ":/Volumes/Archive=smb://h/Archive")
    assert parse_network_mounts() == {"/Volumes/Library": "smb://h._smb._tcp.local/Library",
                                      "/Volumes/Archive": "smb://h/Archive"}


def test_recovered_share_maps_to_lan_paths():
    v = map_path("/Volumes/Archive/Library/lib/Books/A B.epub")
    assert v["smb"] == "smb://nas/Archive/Library/lib/Books/A%20B.epub"
    assert v["unc"] == "\\\\nas\\Archive\\Library\\lib\\Books\\A B.epub"


def test_nul_characters_are_stripped_from_evidence():
    assert sanitize_json({"text": {"bylines": ["JOH\x00N"]}, "k\x00": 1}) == {"text": {"bylines": ["JOHN"]}, "k": 1}


def test_lone_surrogates_and_non_finite_floats_are_removed():
    assert sanitize_json({"t": "ab\ud800c", "r": float("nan"), "s": [float("inf"), 1.5]}) == \
        {"t": "abc", "r": None, "s": [None, 1.5]}
