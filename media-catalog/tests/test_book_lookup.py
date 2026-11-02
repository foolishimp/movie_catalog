"""ISBN lookups: only the ISBN is sent, in both spellings."""
from books.lookup import isbn13_to_10


def test_isbn13_to_10():
    assert isbn13_to_10("9780672329814") == "0672329816"
    assert isbn13_to_10("9780804429573") == "080442957X"
    assert isbn13_to_10("9791234567896") is None     # 979 numbers have no ISBN-10
