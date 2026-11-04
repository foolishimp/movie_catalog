"""Compare two snapshots: python diff_snap.py BEFORE.json AFTER.json [samples]"""
import json, os, re, sys
from collections import Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LOCAL = json.load(open(os.path.join(ROOT, "tools", "data", "local.json")))
GAME_DIR = f"/{LOCAL['game_folder']}/"
before, after = (json.load(open(p)) for p in sys.argv[1:3])
n = int(sys.argv[3]) if len(sys.argv) > 3 else 12
GAME = re.compile(r"^(?:A?D&D|Dungeons|Forgotten Realms|Dragonlance|Ravenloft|Greyhawk|Warhammer|GURPS|Ars Magica|"
                  r"Rolemaster|Shadowrun|Pathfinder|Star Wars|World of Darkness)", re.I)
groups = {k: [] for k in ("calibre: authors changed", "calibre: title changed", "loose: authors changed",
                          "loose: title changed", "series now a game line (outside the game folder)", "author lost",
                          "author gained", "review flag cleared", "review flag set")}
for i, a in after.items():
    b = before.get(i)
    if not b:
        continue
    lib = "calibre" if a["library"] else "loose"
    if a["authors"] != b["authors"]:
        groups[f"{lib}: authors changed"].append((b, a))
        if b["authors"] and not a["authors"]:
            groups["author lost"].append((b, a))
        if a["authors"] and not b["authors"]:
            groups["author gained"].append((b, a))
    if a["title"] != b["title"]:
        groups[f"{lib}: title changed"].append((b, a))
    if a["series"] != b["series"] and a["series"] and GAME.match(a["series"]) and GAME_DIR not in a["directory"]:
        groups["series now a game line (outside the game folder)"].append((b, a))
    if b["needs_review"] and not a["needs_review"]:
        groups["review flag cleared"].append((b, a))
    if a["needs_review"] and not b["needs_review"]:
        groups["review flag set"].append((b, a))

for name, pairs in groups.items():
    print(f"== {name}: {len(pairs)}")
    for b, a in pairs[:n]:
        print(f"   {a['id']}: {b['title']!r} / {b['authors']} / {b['series']}  ->  {a['title']!r} / {a['authors']} / {a['series']}")
rpg = [a for a in after.values() if GAME_DIR in a["directory"]]
bad = Counter()
for a in rpg:
    for au in a["authors"] or []:
        if au in ("D", "AD") or GAME.match(au) or re.search(r"\.\w{2,4}$|^\w+\.\w+\.\w+$", au):
            bad[au] += 1
print("== RPG units still with a game line, D/AD or handle as author:", sum(bad.values()), bad.most_common(10))
junk = [a for a in rpg if re.search(r"\.(tmp|qxd|indd|p65|doc|pmd)$|^Acr\w*\b", a["title"] or "", re.I)]
print("== RPG units with a layout/temp-file title:", len(junk), [a["title"] for a in junk[:10]])
