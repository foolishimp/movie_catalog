from books.calibre import library_label


def test_a_named_library_drops_the_calibre_prefix():
    assert library_label("/Volumes/Share/calibre/calibre fiction 1990s") == "fiction 1990s"


def test_a_generic_library_is_named_by_its_path_without_the_mount():
    assert library_label("/Volumes/Share/docs/lib/calibre") == "docs lib calibre"
    assert library_label("/Volumes/Share/docs/lib") == "docs lib"
    assert library_label("/Volumes/Share/calibre") == "calibre"
    assert library_label("/Volumes/calibre") == "calibre"
