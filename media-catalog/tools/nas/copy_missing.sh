#!/bin/bash
# Copy files missing from DEST: copy_missing.sh SOURCE DEST
# Never overwrites and never deletes. Each file goes to a hidden .part name and is
# renamed only after its size matches, so an interrupted copy can't pass for a
# finished one. A failed file is logged and the rest still run.
SRC=${1%/}
DST=${2%/}
HERE=$(cd "$(dirname "$0")" && pwd)
LOG=$HERE/../data/nas/copy_missing.log
mkdir -p "$(dirname "$LOG")"

ok=0; bad=0; same=0
while IFS= read -r -d '' path; do
  rel=${path#"$SRC"/}
  size=$(stat -f %z "$path")
  out="$DST/$rel"
  tmp="$(dirname "$out")/.$(basename "$out").part"
  if [ -e "$out" ]; then
    if [ "$(stat -f %z "$out")" = "$size" ]; then
      same=$((same + 1))
    else
      echo "$(date '+%H:%M') FAILED different size already there: $rel" >> "$LOG"
      bad=$((bad + 1))
    fi
    continue
  fi
  if [ -e "$tmp" ]; then
    echo "$(date '+%H:%M') FAILED partial file already there: $rel" >> "$LOG"
    bad=$((bad + 1))
    continue
  fi
  mkdir -p "$(dirname "$out")"
  if cp -p "$path" "$tmp" 2>> "$LOG" \
    && [ "$(stat -f %z "$tmp")" = "$size" ] \
    && mv -n "$tmp" "$out" 2>> "$LOG"; then
    echo "$(date '+%H:%M') ok $size $rel" >> "$LOG"
    ok=$((ok + 1))
  else
    echo "$(date '+%H:%M') FAILED $size $rel" >> "$LOG"
    bad=$((bad + 1))
  fi
done < <(find "$SRC" -type f -not -name '.DS_Store' -not -name '._*' -print0)

echo "$(date '+%H:%M') DONE: $ok copied, $same already there, $bad failed"
[ "$bad" -eq 0 ]
