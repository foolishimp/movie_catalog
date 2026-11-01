#!/usr/bin/env python3
"""
Host-side opener service for the Media Catalog web app.

Runs on the Mac so the (possibly dockerised) web UI can trigger host-level
actions: launch VLC, reveal in Finder, and now kick off catalog refreshes
(scan + enrich) and per-entry metadata resyncs.

Usage:
    python host_opener.py [--port 9111]

    # With network mount auto-mounting:
    NETWORK_MOUNTS="/Volumes/Media=smb://server/share" python host_opener.py

The web app reaches this service via HOST_OPENER_URL (see docker-compose.yml;
defaults to http://host.docker.internal:9111).

Endpoints (all JSON):
    POST /open-vlc          {file_path}            → open a file in VLC
    POST /reveal            {file_path}            → reveal in Finder
    POST /refresh                                   → start scan → enrich pipeline
    GET  /refresh/status                            → progress / phase / counts
    POST /resync            {entry_id}              → re-enrich one entry synchronously
"""
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Network mount helpers (shared with the open-vlc / reveal endpoints)
# ---------------------------------------------------------------------------

def _parse_network_mounts():
    raw = os.environ.get("NETWORK_MOUNTS", "")
    mounts = {}
    for entry in raw.split(":/Volumes/"):
        if "=" not in entry:
            continue
        if not entry.startswith("/"):
            entry = "/Volumes/" + entry
        vol, url = entry.split("=", 1)
        mounts[vol] = url
    return mounts


NETWORK_MOUNTS = _parse_network_mounts()


def ensure_mounted(file_path: str) -> bool:
    if os.path.exists(file_path):
        return True
    if not NETWORK_MOUNTS:
        return False
    for vol_path, mount_url in NETWORK_MOUNTS.items():
        if file_path.startswith(vol_path) and not os.path.ismount(vol_path):
            try:
                print(f"[opener] mounting {mount_url} for {vol_path}")
                subprocess.run(["open", mount_url], check=True, timeout=15)
                for _ in range(10):
                    if os.path.exists(file_path):
                        return True
                    time.sleep(1)
            except Exception as e:
                print(f"[opener] mount failed: {e}")
    return os.path.exists(file_path)


def normalize_vlc_window() -> None:
    """Best-effort recovery for macOS remembering an off-screen VLC window."""
    script = """
tell application "VLC"
    activate
    reopen
end tell
delay 0.5
tell application "System Events"
    if not (exists process "VLC") then return
    tell process "VLC"
        set frontmost to true
        repeat 10 times
            if (count of windows) > 0 then exit repeat
            delay 0.2
        end repeat
        set windowCount to count of windows
        repeat with i from 1 to windowCount
            try
                set w to window i
                try
                    set value of attribute "AXMinimized" of w to false
                end try
                set position of w to {120, 120}
                set size of w to {1120, 630}
            end try
        end repeat
    end tell
end tell
"""
    try:
        subprocess.run(
            ["osascript"],
            input=script,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception as e:
        print(f"[opener] VLC window normalization skipped: {e}")


# ---------------------------------------------------------------------------
# Refresh (scan → enrich) supervision
# ---------------------------------------------------------------------------

REFRESH_LOG = ROOT / ".refresh.log"
# If the pipeline's log file hasn't been touched in this many seconds AND the
# process is still "running" per poll(), we treat it as stuck and allow a new
# /refresh to terminate and replace it. This protects against an enricher that
# wedges on a socket read, an OS sleep, or a zombie bash wrapper.
STALE_THRESHOLD_SEC = 180
_refresh_lock = threading.Lock()
_refresh_proc: subprocess.Popen | None = None
_refresh_started_at: float | None = None
_refresh_finished_at: float | None = None


def _accessible_media_dirs() -> tuple[list[str], list[str]]:
    raw = os.environ.get("MEDIA_DIRS", "")
    dirs = [d.strip() for d in raw.split(":") if d.strip()]
    existing = [d for d in dirs if os.path.exists(d)]
    missing = [d for d in dirs if not os.path.exists(d)]
    return existing, missing


def _mount_network_volumes(timeout_total: float = 20.0) -> None:
    """For any NETWORK_MOUNTS entry whose volume isn't mounted, kick off `open smb://…`
    and wait briefly for it to appear. Used before a scan to recover from dropped mounts."""
    if not NETWORK_MOUNTS:
        return
    deadline = time.time() + timeout_total
    for vol_path, mount_url in NETWORK_MOUNTS.items():
        if os.path.ismount(vol_path) and os.path.exists(vol_path):
            continue
        try:
            print(f"[opener] reconnecting {mount_url} for {vol_path}")
            subprocess.run(["open", mount_url], check=True, timeout=15)
        except Exception as e:
            print(f"[opener] mount command failed for {vol_path}: {e}")
            continue
        while time.time() < deadline:
            if os.path.ismount(vol_path) and os.path.exists(vol_path):
                break
            time.sleep(1)


def _build_refresh_command(do_scan: bool = True, do_enrich: bool = True) -> tuple[list[str], list[str]]:
    """Return (argv-for-bash, missing_dirs) or raise ValueError."""
    parts = []
    if do_scan:
        existing, missing = _accessible_media_dirs()
        if not existing:
            raise ValueError("No accessible MEDIA_DIRS — nothing to scan")
        parts.append(
            shlex.quote(sys.executable) + " -u -m scanner.scan "
            + " ".join(shlex.quote(d) for d in existing)
        )
    else:
        missing = []
    if do_enrich:
        key = os.environ.get("OMDB_API_KEY", "")
        if not key or key == "your_omdb_api_key_here":
            if not parts:
                raise ValueError("OMDB_API_KEY not set and scan skipped — nothing to do")
        else:
            parts.append(shlex.quote(sys.executable) + " -u -m enricher.omdb")
    if not parts:
        raise ValueError("Nothing to run")
    pipeline = " && echo '--- next stage ---' && ".join(parts)
    return ["bash", "-c", pipeline], missing


def _heartbeat_age_sec() -> float | None:
    """Seconds since the refresh log was last written, or None if no log."""
    if not REFRESH_LOG.exists():
        return None
    try:
        return time.time() - REFRESH_LOG.stat().st_mtime
    except Exception:
        return None


def _kill_refresh_group(reason: str) -> None:
    """Terminate the refresh pipeline and everything it spawned (scanner,
    enricher, the bash wrapper) by killing its process group.

    The caller must hold `_refresh_lock`.
    """
    global _refresh_proc
    import signal
    proc = _refresh_proc
    if proc is None:
        return
    if proc.poll() is not None:
        _refresh_proc = None
        return
    print(f"[opener] killing refresh pid {proc.pid} — {reason}")
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
            try: proc.wait(timeout=3)
            except subprocess.TimeoutExpired: pass
    except ProcessLookupError:
        pass
    except Exception as e:
        print(f"[opener] group-kill failed ({e}); falling back to proc.kill()")
        try: proc.kill()
        except Exception: pass
    _refresh_proc = None


def _start_refresh(do_scan: bool, do_enrich: bool, force: bool = False) -> dict:
    """Spawn the scan+enrich pipeline. Auto-recovers from stale runs.

    Behaviour when a run is already tracked:
      * alive + fresh heartbeat → return 409 `already_running`
      * alive + stale heartbeat (> STALE_THRESHOLD_SEC) → kill and start fresh
      * alive + force=True → kill and start fresh unconditionally
      * dead (poll() returned) → clear state and start fresh
    """
    global _refresh_proc, _refresh_started_at, _refresh_finished_at
    with _refresh_lock:
        proc = _refresh_proc
        is_alive = proc is not None and proc.poll() is None
        if is_alive:
            age = _heartbeat_age_sec()
            stale = age is not None and age > STALE_THRESHOLD_SEC
            if force:
                _kill_refresh_group("force=True requested")
            elif stale:
                _kill_refresh_group(f"stale heartbeat ({age:.0f}s since log write)")
            else:
                return {
                    "error": "already_running",
                    "started_at": _refresh_started_at,
                    "pid": proc.pid,
                    "heartbeat_age_sec": age,
                    "stale_threshold_sec": STALE_THRESHOLD_SEC,
                }
        elif proc is not None:
            # Dead — clear stale handle so status reporting doesn't confuse.
            _refresh_proc = None

        # Try to bring back network volumes that may have dropped since last run.
        _mount_network_volumes()

        cmd, missing = _build_refresh_command(do_scan=do_scan, do_enrich=do_enrich)
        log_file = open(REFRESH_LOG, "w")
        # start_new_session=True puts the child in its own process group so the
        # whole chain (bash → python → tqdm) can be killed together later.
        _refresh_proc = subprocess.Popen(
            cmd, stdout=log_file, stderr=subprocess.STDOUT, cwd=ROOT,
            start_new_session=True,
        )
        _refresh_started_at = time.time()
        _refresh_finished_at = None
        print(f"[opener] refresh started (pid {_refresh_proc.pid}, scan={do_scan}, enrich={do_enrich}, force={force})")
        return {
            "ok": True,
            "pid": _refresh_proc.pid,
            "started_at": _refresh_started_at,
            "missing_dirs": missing,
        }


def _cancel_refresh() -> dict:
    """Cancel any running refresh pipeline. Safe to call when nothing is running."""
    with _refresh_lock:
        proc = _refresh_proc
        if proc is None or proc.poll() is not None:
            return {"ok": True, "was_running": False}
        pid = proc.pid
        _kill_refresh_group("user cancel")
        return {"ok": True, "was_running": True, "pid": pid}


def _read_refresh_log() -> str:
    if not REFRESH_LOG.exists():
        return ""
    try:
        data = REFRESH_LOG.read_bytes()
        # Keep the tail so parsing stays cheap on long logs.
        if len(data) > 64 * 1024:
            data = data[-64 * 1024:]
        return data.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _parse_refresh_status(log_text: str) -> dict:
    """Extract phase + counts from the pipeline log."""
    if not log_text:
        return {"phase": "starting"}
    status: dict = {"phase": "starting"}

    if "Media Catalog Scanner" in log_text or "Scanning " in log_text:
        status["phase"] = "scanning"

    m = re.search(r"Found ([\d,]+) video files", log_text)
    if m:
        status["scan_found"] = int(m.group(1).replace(",", ""))
    m = re.search(r"([\d,]+) already cataloged, ([\d,]+) new", log_text)
    if m:
        status["scan_already"] = int(m.group(1).replace(",", ""))
        status["scan_new"] = int(m.group(2).replace(",", ""))

    if "OMDb Metadata Enricher" in log_text or "entries to enrich" in log_text:
        status["phase"] = "enriching"
    m = re.search(r"([\d,]+) entries to enrich \(([\d,]+) unique", log_text)
    if m:
        status["enrich_entries"] = int(m.group(1).replace(",", ""))
        status["enrich_unique"] = int(m.group(2).replace(",", ""))
    ticks = re.findall(r"Enriching:\s*\d+%\|[^|]*\|\s*([\d,]+)/([\d,]+)", log_text)
    if ticks:
        status["enrich_done"] = int(ticks[-1][0].replace(",", ""))
        status["enrich_total"] = int(ticks[-1][1].replace(",", ""))

    if "✅ Enriched" in log_text or "Catalog now contains" in log_text or "Nothing to enrich" in log_text:
        status["phase"] = "done"
    return status


def _refresh_status() -> dict:
    with _refresh_lock:
        proc = _refresh_proc
        started = _refresh_started_at
        finished = _refresh_finished_at
    running = proc is not None and proc.poll() is None
    exit_code = None
    if proc is not None and not running:
        exit_code = proc.returncode
        # Latch the finished_at the first time we notice it completed.
        if finished is None:
            with _refresh_lock:
                if _refresh_finished_at is None:
                    globals()["_refresh_finished_at"] = time.time()
                finished = _refresh_finished_at

    log_text = _read_refresh_log()
    parsed = _parse_refresh_status(log_text)
    if not running and proc is not None:
        parsed["phase"] = "done" if exit_code == 0 else "error"

    tail_lines = [l for l in log_text.splitlines() if l.strip()][-8:]
    age = _heartbeat_age_sec()
    return {
        "running": running,
        "pid": proc.pid if proc else None,
        "started_at": started,
        "finished_at": finished,
        "exit_code": exit_code,
        "heartbeat_age_sec": age,
        "stale_threshold_sec": STALE_THRESHOLD_SEC,
        "stale": bool(running and age is not None and age > STALE_THRESHOLD_SEC),
        "tail": tail_lines,
        **parsed,
    }


def _resync_one(entry_id: int) -> dict:
    """Synchronously re-enrich a single entry via `enricher.omdb --id N`."""
    if not isinstance(entry_id, int) or entry_id <= 0:
        return {"error": "invalid entry_id"}
    key = os.environ.get("OMDB_API_KEY", "")
    if not key or key == "your_omdb_api_key_here":
        return {"error": "OMDB_API_KEY not set on host"}
    try:
        r = subprocess.run(
            [sys.executable, "-u", "-m", "enricher.omdb", "--id", str(entry_id)],
            capture_output=True, text=True, timeout=30, cwd=ROOT,
        )
        if r.returncode != 0:
            return {"error": r.stderr.strip() or "enricher failed", "stdout": r.stdout[-1000:]}
        return {"ok": True, "stdout": r.stdout[-2000:]}
    except subprocess.TimeoutExpired:
        return {"error": "resync timed out after 30s"}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class OpenerHandler(BaseHTTPRequestHandler):
    # --- Helpers ---
    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except Exception:
            return {}

    def _respond(self, status: int, data: dict):
        payload = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        print(f"[opener] {args[0]}")

    # --- GET ---
    def do_GET(self):
        if self.path == "/refresh/status":
            self._respond(200, _refresh_status())
            return
        if self.path == "/health":
            self._respond(200, {"ok": True, "mounts": list(NETWORK_MOUNTS)})
            return
        self._respond(404, {"error": "unknown endpoint"})

    # --- POST ---
    def do_POST(self):
        body = self._read_body()

        if self.path == "/refresh":
            do_scan = bool(body.get("scan", True))
            do_enrich = bool(body.get("enrich", True))
            force = bool(body.get("force", False))
            try:
                result = _start_refresh(do_scan=do_scan, do_enrich=do_enrich, force=force)
            except ValueError as e:
                self._respond(400, {"error": str(e)})
                return
            status = 409 if result.get("error") == "already_running" else 202
            self._respond(status, result)
            return

        if self.path == "/refresh/cancel":
            self._respond(200, _cancel_refresh())
            return

        if self.path == "/resync":
            entry_id = body.get("entry_id")
            try:
                entry_id = int(entry_id)
            except (TypeError, ValueError):
                self._respond(400, {"error": "entry_id must be an integer"})
                return
            result = _resync_one(entry_id)
            self._respond(200 if result.get("ok") else 500, result)
            return

        # File-path-based endpoints below all need a valid path.
        file_path = body.get("file_path", "")
        if not file_path or ".." in file_path or not file_path.startswith("/"):
            self._respond(400, {"error": "invalid file_path"})
            return

        if self.path == "/open-vlc":
            cmd = ["open", "-n", "-a", "VLC", file_path]
        elif self.path == "/reveal":
            cmd = ["open", "-R", file_path]
        else:
            self._respond(404, {"error": "unknown endpoint"})
            return

        if not os.path.exists(file_path):
            if not ensure_mounted(file_path):
                self._respond(404, {"error": f"file not found: {file_path}"})
                return

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self._respond(500, {"error": result.stderr.strip() or "command failed"})
            else:
                if self.path == "/open-vlc":
                    normalize_vlc_window()
                self._respond(200, {"ok": True})
        except Exception as e:
            self._respond(500, {"error": str(e)})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Host-side opener for media-catalog")
    parser.add_argument("--port", type=int, default=int(os.environ.get("OPENER_PORT", 9111)))
    args = parser.parse_args()

    print(f"Host opener listening on :{args.port}")
    print("Endpoints: POST /open-vlc, POST /reveal, POST /refresh, GET /refresh/status, POST /resync")
    if NETWORK_MOUNTS:
        print(f"Network mounts configured: {list(NETWORK_MOUNTS.keys())}")
    else:
        print("No NETWORK_MOUNTS configured – files must already be accessible")
    existing, missing = _accessible_media_dirs()
    if existing:
        print(f"MEDIA_DIRS accessible: {existing}")
    if missing:
        print(f"MEDIA_DIRS missing:    {missing}")
    ThreadingHTTPServer(("0.0.0.0", args.port), OpenerHandler).serve_forever()
