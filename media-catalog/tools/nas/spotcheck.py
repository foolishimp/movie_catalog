#!/usr/bin/env python3
"""Compare each file's size, start, middle and end against its copy (read-only): spotcheck.py SOURCE DEST"""
import fcntl
import os
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA = os.path.join(ROOT, "tools", "data", "nas")
LOG = os.path.join(DATA, "spotcheck.log")

SAMPLE = 64 * 1024 * 1024
CHUNK = 4 * 1024 * 1024
F_NOCACHE = 48  # macOS: read from the NAS, not from what a recent copy left in the Mac's cache
SKIP = (".DS_Store",)


def open_uncached(path):
    fd = os.open(path, os.O_RDONLY)
    fcntl.fcntl(fd, F_NOCACHE, 1)
    return fd


def read_exact(fd, offset, length):
    data = bytearray()
    while len(data) < length:
        got = os.pread(fd, length - len(data), offset + len(data))
        if not got:
            break
        data += got
    return bytes(data)


def spans(size):
    """Whole file when it is too small to sample, otherwise start, middle and end."""
    if size <= 3 * SAMPLE:
        return [(0, size)]
    return [(0, SAMPLE), ((size - SAMPLE) // 2, SAMPLE), (size - SAMPLE, SAMPLE)]


def check(src, dst):
    size = os.path.getsize(src)
    if os.path.getsize(dst) != size:
        return "DIFFERENT size"
    a = open_uncached(src)
    b = open_uncached(dst)
    try:
        for start, length in spans(size):
            for off in range(start, start + length, CHUNK):
                n = min(CHUNK, start + length - off)
                x, y = read_exact(a, off, n), read_exact(b, off, n)
                if len(x) != n or len(y) != n:
                    return f"SHORT READ at {off}"
                if x != y:
                    return f"DIFFERENT at {off}"
    finally:
        os.close(a)
        os.close(b)
    return None


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    src_root, dst_root = sys.argv[1:]
    rels = []
    for folder, _, names in os.walk(src_root):
        for name in names:
            if name not in SKIP and not name.startswith("._"):
                rels.append(os.path.relpath(os.path.join(folder, name), src_root))
    os.makedirs(DATA, exist_ok=True)
    same = bad = 0
    with open(LOG, "a", buffering=1) as log:
        for i, rel in enumerate(sorted(rels), 1):
            try:
                problem = check(os.path.join(src_root, rel), os.path.join(dst_root, rel))
            except OSError as e:
                problem = f"ERROR {e}"
            stamp = time.strftime("%H:%M")
            if problem:
                bad += 1
                log.write(f"{stamp} {i}/{len(rels)} {problem}: {rel}\n")
            else:
                same += 1
                log.write(f"{stamp} {i}/{len(rels)} same: {rel}\n")
    print(f"{time.strftime('%H:%M')} DONE: {same} same, {bad} with problems, of {len(rels)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
