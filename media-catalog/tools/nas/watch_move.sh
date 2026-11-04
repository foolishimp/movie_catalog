#!/bin/bash
# Watch a Finder move between network shares: watch_move.sh SOURCE DEST
# Exits when SOURCE is gone (finished), a share disconnects, or DEST's volume
# stops growing for 15 minutes (stalled, error dialog, or cancelled).
SRC=$1
DST=$2
INTERVAL=${INTERVAL:-120}
HERE=$(cd "$(dirname "$0")" && pwd)
LOG=$HERE/../data/nas/watch_move.log
mkdir -p "$(dirname "$LOG")"

share() { echo "$1" | cut -d/ -f1-3; }
used() { df -k "$DST" 2>/dev/null | awk 'NR==2{print $3}'; }
free_gb() { df -k "$1" 2>/dev/null | awk 'NR==2{printf "%.1f", $4/1048576}'; }

SRC_SHARE=$(share "$SRC")
DST_SHARE=$(share "$DST")
last=$(used); still=0
while true; do
  sleep "$INTERVAL"
  for v in "$SRC_SHARE" "$DST_SHARE"; do
    if [ ! -d "$v" ]; then
      echo "$(date '+%H:%M') DISCONNECTED: $v is no longer mounted"; exit 3
    fi
  done
  if [ ! -e "$SRC" ]; then
    echo "$(date '+%H:%M') FINISHED: $SRC is gone. Source free=$(free_gb "$SRC_SHARE") GB, destination free=$(free_gb "$DST_SHARE") GB"
    exit 0
  fi
  now=$(used)
  if [ -n "$now" ] && [ -n "$last" ] && [ $((now - last)) -lt 102400 ]; then
    still=$((still + INTERVAL))
  else
    still=0
  fi
  echo "$(date '+%H:%M') destination used=$((now / 1048576)) GB, idle ${still}s, helper=$(pgrep -x DesktopServicesHelper || echo none)" >> "$LOG"
  last=$now
  if [ "$still" -ge 900 ]; then
    echo "$(date '+%H:%M') STALLED: $DST has not grown for 15 minutes and $SRC still exists. helper=$(pgrep -x DesktopServicesHelper || echo none)"
    exit 2
  fi
done
