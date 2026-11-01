#!/usr/bin/env python3
"""
start.py — One-command startup for Media Catalog.

What it does:
  1. Ensures Docker Desktop is running (macOS: opens it if not)
  2. Starts the Postgres container (docker compose up -d db)
  3. Waits for the database to be ready
  4. Installs any missing Python dependencies
  5. Connects any configured network volumes (NETWORK_MOUNTS)
  6. Starts host_opener.py in the background (for VLC/Finder from the web UI)
  7. Starts a chained scan → enrich pipeline in the background
     (the OMDb enricher only begins after the scanner finishes, so freshly
     scanned entries actually get internet metadata)
  8. Opens the web UI in your browser
  9. Runs the web server in the foreground (Ctrl+C to stop everything)

Robustness:
  * If :8080 is already serving the catalog, reuse it instead of failing.
    Background workers still start and the script waits for them to finish.
  * If :8080 is occupied by something else, abort with a clear message
    before spawning any workers (no more orphan scanner/enricher).
  * host_opener is supervised: port-probed after launch, skipped if already up.
  * Background processes get a post-spawn alive-check with a log tail if they
    die immediately.

Usage:
    python start.py               # normal start
    python start.py --no-scan     # skip the scanner
    python start.py --no-enrich   # skip the OMDb enricher
    python start.py --no-opener   # skip host_opener
    python start.py --no-browser  # don't auto-open browser
"""
import argparse
import os
import shlex
import signal
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WEB_PORT = 8080
OPENER_PORT = 9111
SCAN_TS = ROOT / ".last_scan"   # touched when a scan completes; drives staleness
DB_URL = None
_bg_procs = []  # list of (label, proc, log_file)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def step(msg): print(f"\n\033[1;34m→\033[0m {msg}")
def ok(msg):   print(f"  \033[32m✓\033[0m {msg}")
def warn(msg): print(f"  \033[33m⚠\033[0m  {msg}")
def fail(msg):
    print(f"  \033[31m✗\033[0m {msg}")
    sys.exit(1)


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


# ---------------------------------------------------------------------------
# Port + health helpers
# ---------------------------------------------------------------------------

def port_pid(port: int):
    """Return PID listening on `port`, or None."""
    try:
        r = subprocess.run(
            ["lsof", "-ti", f":{port}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=5,
        )
        pids = [int(p) for p in r.stdout.split() if p.isdigit()]
        return pids[0] if pids else None
    except Exception:
        return None


def wait_for_port(port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def http_ok(url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return 200 <= r.status < 500
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Background-process supervision
# ---------------------------------------------------------------------------

def popen_bg(cmd, label, health_port: int | None = None, settle: float = 1.0):
    """Spawn a background subprocess, register cleanup, verify it stayed alive.

    Returns the Popen handle on success, or None if the process died immediately.
    """
    log_path = ROOT / f".{label}.log"
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        cmd, stdout=log_file, stderr=subprocess.STDOUT, cwd=ROOT,
    )
    _bg_procs.append((label, proc, log_file))

    time.sleep(settle)
    if proc.poll() is not None:
        log_file.close()
        try:
            tail = log_path.read_text().strip().splitlines()[-10:]
        except Exception:
            tail = []
        warn(f"{label} exited immediately (code {proc.returncode}). Log tail:")
        for line in tail:
            print(f"      {line}")
        return None

    if health_port is not None:
        if wait_for_port(health_port, timeout=5):
            ok(f"{label} started (pid {proc.pid}, :{health_port}, log .{label}.log)")
        else:
            warn(f"{label} started (pid {proc.pid}) but did not bind :{health_port} in 5s — see .{label}.log")
    else:
        ok(f"{label} started (pid {proc.pid}, log .{label}.log)")
    return proc


def cleanup(sig=None, frame=None):
    if _bg_procs:
        print("\n\033[1mStopping background processes…\033[0m")
        for label, proc, log_file in _bg_procs:
            if proc.poll() is not None:
                print(f"  {label} already exited")
            else:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                    print(f"  stopped {label}")
                except Exception:
                    try: proc.kill()
                    except Exception: pass
                    print(f"  killed {label}")
            try: log_file.close()
            except Exception: pass
    sys.exit(0)


signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def load_env():
    global DB_URL
    env_file = ROOT / ".env"
    if not env_file.exists():
        warn(".env not found — copying from .env.example")
        example = ROOT / ".env.example"
        if example.exists():
            import shutil
            shutil.copy(example, env_file)
            warn("Edit .env and set MEDIA_DIRS (and OMDB_API_KEY if you have one), then re-run.")
            sys.exit(0)
        fail("No .env or .env.example found.")

    from dotenv import load_dotenv
    load_dotenv(env_file)
    DB_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://catalog:catalog@localhost:5432/media_catalog",
    )
    ok(".env loaded")


def ensure_docker():
    step("Checking Docker")
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        if r.returncode == 0:
            ok("Docker is running")
            return
    except FileNotFoundError:
        fail("Docker not found. Install Docker Desktop from https://www.docker.com/products/docker-desktop/")
    except subprocess.TimeoutExpired:
        pass

    warn("Docker Desktop is not running. Attempting to start it…")
    try:
        subprocess.run(["open", "-a", "Docker"], check=True)
    except Exception:
        fail("Could not start Docker Desktop. Please open it manually and re-run.")

    print("  Waiting for Docker to start", end="", flush=True)
    for _ in range(60):
        time.sleep(2)
        print(".", end="", flush=True)
        if subprocess.run(["docker", "info"], capture_output=True).returncode == 0:
            print()
            ok("Docker is now running")
            return
    print()
    fail("Docker Desktop did not start in time. Please open it manually and re-run.")


def start_db():
    step("Starting Postgres (docker compose up -d db)")
    run(["docker", "compose", "up", "-d", "db"], cwd=ROOT)
    ok("Container started")

    print("  Waiting for database to accept connections", end="", flush=True)
    import psycopg2
    for _ in range(30):
        time.sleep(2)
        print(".", end="", flush=True)
        try:
            psycopg2.connect(DB_URL, connect_timeout=3).close()
            print()
            ok("Database is ready")
            return
        except Exception:
            pass
    print()
    fail("Database did not become ready in time. Check: docker compose logs db")


def ensure_deps():
    step("Checking Python dependencies")
    req = ROOT / "requirements.txt"
    if not req.exists():
        warn("No requirements.txt found — skipping")
        return
    try:
        run(
            [sys.executable, "-m", "pip", "install", "-q", "-r", str(req)],
            capture_output=True,
        )
        ok("Dependencies installed")
    except subprocess.CalledProcessError as e:
        warn(f"pip install had errors: {e}")


def ensure_network_mounts():
    raw = os.getenv("NETWORK_MOUNTS", "")
    if not raw:
        return
    step("Connecting network volumes")
    mounts = {}
    for entry in raw.split(":/Volumes/"):
        if "=" not in entry:
            continue
        if not entry.startswith("/"):
            entry = "/Volumes/" + entry
        vol, url = entry.split("=", 1)
        mounts[vol] = url

    for vol_path, mount_url in mounts.items():
        if os.path.ismount(vol_path):
            ok(f"{vol_path} already mounted")
            continue
        print(f"  Mounting {vol_path} …", end="", flush=True)
        try:
            subprocess.run(["open", mount_url], check=True, timeout=15)
            for _ in range(15):
                time.sleep(1)
                if os.path.ismount(vol_path):
                    print()
                    ok(f"{vol_path} mounted")
                    break
                print(".", end="", flush=True)
            else:
                print()
                warn(f"{vol_path} did not appear after 15s — continuing anyway")
        except Exception as e:
            print()
            warn(f"Could not mount {vol_path}: {e}")


def scan_age_hours() -> float | None:
    """Hours since the last completed scan, or None if never scanned."""
    if not SCAN_TS.exists():
        return None
    return (time.time() - SCAN_TS.stat().st_mtime) / 3600.0


def scan_is_recent() -> bool:
    """True if a scan completed within SCAN_MAX_AGE_HOURS (default 6)."""
    try:
        max_age = float(os.getenv("SCAN_MAX_AGE_HOURS", "6"))
    except ValueError:
        max_age = 6.0
    age = scan_age_hours()
    return age is not None and age < max_age


def preflight_web() -> str:
    """Decide whether to start uvicorn or reuse an existing server.
    Must run BEFORE any background workers are spawned, so we don't orphan
    them if the port is unusable."""
    step(f"Preflight: checking :{WEB_PORT}")
    pid = port_pid(WEB_PORT)
    if not pid:
        ok(f"Port {WEB_PORT} is free")
        return "start"
    if http_ok(f"http://localhost:{WEB_PORT}/"):
        warn(f"Web server already running on :{WEB_PORT} (pid {pid}) — reusing it")
        return "reuse"
    fail(
        f"Port {WEB_PORT} is held by pid {pid} but it is not responding as the catalog. "
        f"Stop it first:  kill {pid}"
    )
    return "fail"  # unreachable


def start_host_opener():
    step("Starting host opener (background, for VLC / Reveal in Finder)")
    pid = port_pid(OPENER_PORT)
    if pid:
        ok(f"host_opener already running on :{OPENER_PORT} (pid {pid})")
        return
    script = ROOT / "host_opener.py"
    if not script.exists():
        warn(f"{script.name} not found — skipping")
        return
    popen_bg([sys.executable, str(script)], "host_opener", health_port=OPENER_PORT)


def start_scan_and_enrich(do_scan: bool, do_enrich: bool):
    """Chain scanner → enricher in one bg pipeline so the enricher only runs
    after the scan has written its new rows to the database."""
    if not do_scan and not do_enrich:
        return

    parts = []
    labels = []

    if do_scan:
        media_dirs = os.getenv("MEDIA_DIRS", "")
        if not media_dirs:
            warn("MEDIA_DIRS not set in .env — skipping scanner")
        else:
            dirs = [d.strip() for d in media_dirs.split(":") if d.strip()]
            existing = [d for d in dirs if Path(d).exists()]
            missing = [d for d in dirs if not Path(d).exists()]
            if missing:
                warn(f"Skipping {len(missing)} unmounted dir(s): {', '.join(missing)}")
            if existing:
                scan_cmd = (
                    shlex.quote(sys.executable)
                    + " -m scanner.scan "
                    + " ".join(shlex.quote(d) for d in existing)
                    + " && touch " + shlex.quote(str(SCAN_TS))  # mark scan freshness
                )
                parts.append(scan_cmd)
                labels.append("scan")
            else:
                warn("None of the MEDIA_DIRS are accessible — skipping scanner")

    if do_enrich:
        key = os.getenv("OMDB_API_KEY", "")
        if not key or key == "your_omdb_api_key_here":
            warn("OMDB_API_KEY not set in .env — skipping enricher")
        else:
            parts.append(
                shlex.quote(sys.executable) + " -m enricher.omdb"
            )
            labels.append("enrich")

    if not parts:
        return

    pipeline = " && echo '--- next stage ---' && ".join(parts)
    label = "+".join(labels)  # e.g. "scan+enrich" or just "scan"
    step(f"Starting {label} pipeline (background, chained)")
    popen_bg(["bash", "-c", pipeline], label)


def open_browser():
    url = f"http://localhost:{WEB_PORT}"
    print(f"\n  Opening {url} in browser…")
    time.sleep(0.5)
    try:
        subprocess.Popen(["open", url])
    except Exception:
        pass


def run_web_foreground():
    step(f"Starting web server on http://localhost:{WEB_PORT}")
    print("  Press Ctrl+C to stop everything.\n")
    try:
        subprocess.run(
            [
                sys.executable, "-m", "uvicorn",
                "web.app:app",
                "--host", "0.0.0.0",
                "--port", str(WEB_PORT),
                "--reload",
            ],
            cwd=ROOT,
            check=False,
        )
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()


def wait_for_bg_workers():
    """Reuse mode: wait for the scan/enrich pipeline to finish.
    Leave host_opener running after we exit (it is a long-lived service)."""
    watched = [(l, p) for (l, p, _) in _bg_procs if l != "host_opener"]
    if not watched:
        step("No background workers to wait on — done.")
        # Detach host_opener from cleanup so Ctrl+C doesn't kill it.
        _bg_procs.clear()
        return

    step("Waiting for background workers to finish (Ctrl+C to stop all)")
    try:
        while True:
            alive = [(l, p) for (l, p) in watched if p.poll() is None]
            if not alive:
                ok("All background workers finished.")
                # Keep host_opener alive after we exit.
                for entry in list(_bg_procs):
                    if entry[0] == "host_opener":
                        _bg_procs.remove(entry)
                return
            time.sleep(3)
    except KeyboardInterrupt:
        cleanup()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Start Media Catalog")
    parser.add_argument("--no-scan",    action="store_true", help="Skip directory scanner")
    parser.add_argument("--no-enrich",  action="store_true", help="Skip OMDb enricher")
    parser.add_argument("--no-opener",  action="store_true", help="Skip host_opener service")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser")
    parser.add_argument("--scan", action="store_true",
                        help="Force a scan+enrich refresh even if scanned recently")
    args = parser.parse_args()

    print("\n\033[1m🎬 Media Catalog — Starting up\033[0m")

    load_env()
    ensure_docker()
    start_db()
    ensure_deps()
    ensure_network_mounts()

    web_mode = preflight_web()  # 'start' or 'reuse'; 'fail' already exited

    if not args.no_opener:
        start_host_opener()

    # Smart default: refresh only if the catalog is stale (or --scan forces it).
    do_refresh = args.scan or not scan_is_recent()
    if not do_refresh:
        step("Refresh")
        ok(f"catalog scanned {scan_age_hours():.1f}h ago — skipping scan+enrich "
           f"(use --scan to force)")
    start_scan_and_enrich(
        do_scan=do_refresh and not args.no_scan,
        do_enrich=do_refresh and not args.no_enrich,
    )

    if not args.no_browser:
        open_browser()

    if web_mode == "start":
        run_web_foreground()   # blocking; cleanup() runs on exit
    else:
        wait_for_bg_workers()  # reuse mode


if __name__ == "__main__":
    main()
