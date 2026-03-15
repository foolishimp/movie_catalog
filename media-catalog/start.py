#!/usr/bin/env python3
"""
start.py — One-command startup for Media Catalog.

What it does:
  1. Ensures Docker Desktop is running (macOS: opens it if not)
  2. Starts the Postgres container (docker compose up -d db)
  3. Waits for the database to be ready
  4. Installs any missing Python dependencies
  5. Starts the directory scanner in the background
  6. Starts the OMDb enricher in the background (if OMDB_API_KEY is set)
  7. Opens the web UI in your browser
  8. Runs the web server in the foreground (Ctrl+C to stop)

Usage:
    python start.py               # normal start
    python start.py --no-scan     # skip scanner (catalog already up to date)
    python start.py --no-enrich   # skip enricher
    python start.py --no-browser  # don't auto-open browser
"""
import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WEB_PORT = 8080
DB_URL = None          # set after .env is loaded
_bg_procs = []         # background processes to clean up on exit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def step(msg):
    print(f"\n\033[1;34m→\033[0m {msg}")

def ok(msg):
    print(f"  \033[32m✓\033[0m {msg}")

def warn(msg):
    print(f"  \033[33m⚠\033[0m  {msg}")

def fail(msg):
    print(f"  \033[31m✗\033[0m {msg}")
    sys.exit(1)


def run(cmd, **kwargs):
    """Run a command, returning CompletedProcess. Raises on non-zero exit."""
    return subprocess.run(cmd, check=True, **kwargs)


def popen_bg(cmd, label):
    """Start a background subprocess, register it for cleanup."""
    log_path = ROOT / f".{label}.log"
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=ROOT,
    )
    _bg_procs.append((label, proc, log_file))
    ok(f"{label} started (log: .{label}.log, pid {proc.pid})")
    return proc


def cleanup(sig=None, frame=None):
    if _bg_procs:
        print("\n\033[1mStopping background processes…\033[0m")
        for label, proc, log_file in _bg_procs:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
            log_file.close()
            print(f"  stopped {label}")
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
        else:
            fail("No .env or .env.example found.")

    from dotenv import load_dotenv
    load_dotenv(env_file)
    DB_URL = os.getenv("DATABASE_URL", "postgresql://catalog:catalog@localhost:5432/media_catalog")
    ok(".env loaded")


def ensure_docker():
    step("Checking Docker")
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True, timeout=10
        )
        if result.returncode == 0:
            ok("Docker is running")
            return
    except FileNotFoundError:
        fail("Docker not found. Install Docker Desktop from https://www.docker.com/products/docker-desktop/")
    except subprocess.TimeoutExpired:
        pass

    # Docker not running — try to start Docker Desktop on macOS
    warn("Docker Desktop is not running. Attempting to start it…")
    try:
        subprocess.run(["open", "-a", "Docker"], check=True)
    except Exception:
        fail("Could not start Docker Desktop. Please open it manually and re-run.")

    print("  Waiting for Docker to start", end="", flush=True)
    for _ in range(60):
        time.sleep(2)
        print(".", end="", flush=True)
        result = subprocess.run(["docker", "info"], capture_output=True)
        if result.returncode == 0:
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
    for attempt in range(30):
        time.sleep(2)
        print(".", end="", flush=True)
        try:
            conn = psycopg2.connect(DB_URL, connect_timeout=3)
            conn.close()
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


def start_scanner():
    step("Starting directory scanner (background)")
    media_dirs = os.getenv("MEDIA_DIRS", "")
    if not media_dirs:
        warn("MEDIA_DIRS not set in .env — skipping scanner")
        return
    dirs = [d.strip() for d in media_dirs.split(":") if d.strip()]
    existing = [d for d in dirs if Path(d).exists()]
    missing  = [d for d in dirs if not Path(d).exists()]
    if missing:
        warn(f"Skipping {len(missing)} directory/ies not currently mounted: {', '.join(missing)}")
    if not existing:
        warn("None of the MEDIA_DIRS are accessible — skipping scanner")
        return
    popen_bg([sys.executable, "-m", "scanner.scan"] + existing, "scanner")


def start_enricher():
    step("Starting OMDb enricher (background)")
    key = os.getenv("OMDB_API_KEY", "")
    if not key or key in ("your_omdb_api_key_here", ""):
        warn("OMDB_API_KEY not set in .env — skipping enricher")
        return
    popen_bg([sys.executable, "-m", "enricher.omdb"], "enricher")


def open_browser():
    url = f"http://localhost:{WEB_PORT}"
    print(f"\n  Opening {url} in browser…")
    time.sleep(1.5)
    try:
        subprocess.Popen(["open", url])
    except Exception:
        pass


def start_web():
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Start Media Catalog")
    parser.add_argument("--no-scan",    action="store_true", help="Skip directory scanner")
    parser.add_argument("--no-enrich",  action="store_true", help="Skip OMDb enricher")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser")
    args = parser.parse_args()

    print("\n\033[1m🎬 Media Catalog — Starting up\033[0m")

    load_env()
    ensure_docker()
    start_db()
    ensure_deps()

    if not args.no_scan:
        start_scanner()
    if not args.no_enrich:
        start_enricher()
    if not args.no_browser:
        open_browser()

    start_web()   # blocking — runs until Ctrl+C


if __name__ == "__main__":
    main()
