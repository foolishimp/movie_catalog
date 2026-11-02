import pytest

from books.awards import LONGLIST, NOMINEE, WINNER, classify_award_tag


def one(tag):
    awards = classify_award_tag(tag)
    assert len(awards) == 1, (tag, awards)
    a = awards[0]
    return a["award"], a["category"], a["year"], a["result"]


@pytest.mark.parametrize("tag,expected", [
    ("Win of 1976 Hugo Award for Best Novel", ("Hugo Award", "Novel", 1976, WINNER)),
    ("Nomination of 1966 Nebula Award for Short Story", ("Nebula Award", "Short Story", 1966, NOMINEE)),
    ("Preliminary Nominees of 2008 BSFA Award for Best Short Fiction", ("BSFA Award", "Short Fiction", 2008, LONGLIST)),
    ("Nominations Below Cutoff of 1995 Hugo Award for Best Novel", ("Hugo Award", "Novel", 1995, LONGLIST)),
    ("1 of 1975 Locus Award for Best Novel", ("Locus Award", "Novel", 1975, WINNER)),
    ("3 of 1985 Locus Award for Best SF Novel", ("Locus Award", "SF Novel", 1985, NOMINEE)),
    ("Preliminary Nominees of 1996 BFA Award for August Derleth Fantasy Award (Best Novel)",
     ("British Fantasy Award", "August Derleth Fantasy Award (Best Novel)", 1996, LONGLIST)),
    ("hugo award for best novel winner", ("Hugo Award", "Novel", None, WINNER)),
    ("philip k dick award finalist", ("Philip K. Dick Award", None, None, NOMINEE)),
    ("bram stoker award for superior achievement in a novel finalist", ("Bram Stoker Award", "Novel", None, NOMINEE)),
    ("Hugo Award Winner", ("Hugo Award", None, None, WINNER)),
    ("2009 Nebula Award Nominee", ("Nebula Award", None, 2009, NOMINEE)),
    ("Hugo 1992 Winner", ("Hugo Award", None, 1992, WINNER)),
    ("Clarke 1993 Nominated", ("Arthur C. Clarke Award", None, 1993, NOMINEE)),
    ("Locus Best Collection Winner 1985", ("Locus Award", "Collection", 1985, WINNER)),
    ("honorable mention dozois years best", ("Dozois Year's Best", None, None, LONGLIST)),
    ("Newbery Medal", ("Newbery Medal", None, None, WINNER)),
    ("Hugo Award", ("Hugo Award", None, None, None)),
    ("Kurd Laßwitz-Preis Award: Best Foreign Novel", ("Kurd Laßwitz Award", "Foreign Novel", None, None)),
    ("Acclaimed.World Fantasy Award.Nom", ("World Fantasy Award", None, None, NOMINEE)),
])
def test_known_formats(tag, expected):
    assert one(tag) == expected


def test_joint_award_tag_splits():
    awards = classify_award_tag("hugo and nebula award for best novel finalist")
    assert [a["award"] for a in awards] == ["Hugo Award", "Nebula Award"]
    assert all(a["result"] == NOMINEE for a in awards)


@pytest.mark.parametrize("tag", [
    "Lottery winners", "Lottery winners - Fiction", "Nobel Prize winners", "Pulitzer Prizes",
    "award winners", "1-award-winner", "Christian History & Denominations", "Science Fiction",
    "_isfdb", "Novel",
])
def test_not_awards(tag):
    assert classify_award_tag(tag) == []
