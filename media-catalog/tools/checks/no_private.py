"""Find private names in the files git would publish; exits 1 on any hit: python no_private.py

The names come from tools/data/local.json and the folder settings in .env, neither of which is in git.
A term written in lower case matches any case. Names that come from .env are never printed.
"""
import json
import os
import re
import subprocess
import sys

from dotenv import dotenv_values

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SKIP = ("web/static/foliate/",)


def strings(value):
    """Every string inside a JSON value."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from strings(v)


def env_terms():
    """Folders under a mount, and network hosts, named by .env."""
    env = dotenv_values(os.path.join(ROOT, ".env"))
    text = " ".join(env.get(k) or "" for k in ("MEDIA_DIRS", "BOOK_DIRS", "NETWORK_MOUNTS", "PATH_MAPPINGS"))
    terms = set(re.findall(r"/Volumes/[^:=\"\s]+", text))
    terms |= set(re.findall(r"(?:smb|afp)://(?:[^@/\s]+@)?([^/.\s\"]+)", text))
    return terms


def pattern(term):
    flags = re.I if term == term.lower() else 0
    return re.compile(r"(?<![A-Za-z0-9_])" + re.escape(term) + r"(?![A-Za-z0-9_])", flags)


def main():
    with open(os.path.join(ROOT, "tools", "data", "local.json")) as f:
        local = json.load(f)
    named = set(local["private_terms"]) | {s for s in strings(local) if s.startswith("/") and s.count("/") > 1}
    from_env = env_terms() - named
    checks = [(pattern(t), t) for t in sorted(named)] + [(pattern(t), "(a folder or host from .env)") for t in sorted(from_env)]

    files = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"],
                           capture_output=True, text=True, cwd=ROOT, check=True).stdout.splitlines()
    hits = 0
    for rel in files:
        path = os.path.join(ROOT, rel)
        if rel.startswith(SKIP) or not os.path.isfile(path):
            continue
        with open(path, errors="replace") as f:
            for number, line in enumerate(f, 1):
                for regex, shown in checks:
                    if regex.search(line):
                        print(f"{rel}:{number}: {shown}")
                        hits += 1
    print(f"{hits} private names in {len(files)} files" if hits else f"clean: {len(files)} files, {len(checks)} names checked")
    return 1 if hits else 0


if __name__ == "__main__":
    sys.exit(main())
