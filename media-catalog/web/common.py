"""
Helpers shared by the movie and book sections of the web UI: templates,
paging and sorting, path mapping for LAN clients, network-mount recovery,
the host opener client, and a background job runner for scan pipelines.
"""
import json
import logging
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

from dotenv import load_dotenv
from starlette.templating import Jinja2Templates

load_dotenv()

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


def static_url(name: str) -> str:
    """/static URL with the file's mtime appended, so browsers fetch an edited file."""
    try:
        return f"/static/{name}?v={int(os.path.getmtime(os.path.join(STATIC_DIR, name)))}"
    except OSError:
        return f"/static/{name}"


templates.env.globals["static_url"] = static_url

PER_PAGE = 48
HOST_OPENER_URL = os.getenv("HOST_OPENER_URL", "").rstrip("/")


def positive_page(value) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1


def pick_sort(sort_key, default: str, options: dict) -> tuple[str, str]:
    """Validate a ?sort= value against a whitelist of ORDER BY fragments."""
    key = str(sort_key or default)
    if key not in options:
        key = default
    return key, options[key]


def json_safe(rows):
    """Round-trip through JSON so datetimes, dates and Decimals serialize."""
    return json.loads(json.dumps(rows, default=str))


# ---------------------------------------------------------------------------
# Universal path mapping: translate server-local mount paths (/Volumes/...)
# into network forms (smb:// URL, Windows UNC) usable from any LAN client.
# Configure via PATH_MAPPINGS in .env — a JSON object of
#   {"<local prefix>": {"smb": "smb://host/share", "unc": "\\\\host\\share"}}
# No mapping applies when it is unset or invalid.
# ---------------------------------------------------------------------------

DEFAULT_PATH_MAPPINGS = {}


def _load_path_mappings() -> dict:
    raw = os.getenv("PATH_MAPPINGS", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                out = {}
                for prefix, targets in data.items():
                    if isinstance(targets, dict):
                        out[str(prefix)] = {
                            "smb": str(targets.get("smb", "")),
                            "unc": str(targets.get("unc", "")),
                        }
                if out:
                    return out
        except (ValueError, TypeError) as e:
            logging.warning("PATH_MAPPINGS is not valid JSON (%s); ignoring it", e)
    return DEFAULT_PATH_MAPPINGS


PATH_MAPPINGS = _load_path_mappings()


def map_path(local_path: str) -> dict:
    """Translate a server-local file/dir path into per-client variants.

    Returns {"mac": <local path>[, "smb": <smb:// URL>, "unc": <\\host\share path>]}.
    smb/unc are present only when a configured mapping prefix matches.
    The smb form is percent-encoded so it pastes cleanly into Finder / VLC."""
    from urllib.parse import quote
    variants = {"mac": local_path}
    for prefix in sorted(PATH_MAPPINGS, key=len, reverse=True):
        p = prefix.rstrip("/")
        if not (local_path == p or local_path.startswith(p + "/")):
            continue
        rest = local_path[len(p):]  # "" or "/…"
        targets = PATH_MAPPINGS[prefix]
        if targets.get("smb"):
            variants["smb"] = targets["smb"].rstrip("/") + quote(rest, safe="/")
        if targets.get("unc"):
            variants["unc"] = targets["unc"].rstrip("\\") + rest.replace("/", "\\")
        break
    return variants


# ---------------------------------------------------------------------------
# Network mounts and the host opener
# ---------------------------------------------------------------------------

def parse_network_mounts() -> dict:
    """Parse NETWORK_MOUNTS ("/Volumes/A=smb://…:/Volumes/B=afp://…") into {volume: url}.

    Entries are split on ":/Volumes/" because the URLs themselves contain colons."""
    raw = os.getenv("NETWORK_MOUNTS", "")
    mounts = {}
    for entry in raw.split(":/Volumes/"):
        if "=" not in entry:
            continue
        if not entry.startswith("/"):
            entry = "/Volumes/" + entry
        vol, url = entry.split("=", 1)
        mounts[vol] = url
    return mounts


def ensure_mounted(file_path: str) -> bool:
    """If file_path doesn't exist, try to mount its network volume. Returns True if accessible."""
    if os.path.exists(file_path):
        return True
    for vol_path, mount_url in parse_network_mounts().items():
        if file_path.startswith(vol_path) and not os.path.ismount(vol_path):
            try:
                subprocess.run(["open", mount_url], check=True, timeout=15)
                # Wait briefly for the mount to appear
                for _ in range(10):
                    if os.path.exists(file_path):
                        return True
                    time.sleep(1)
            except Exception:
                pass
    return os.path.exists(file_path)


def host_open(endpoint: str, file_path: str) -> dict | None:
    """Delegate an open command to the host opener service. Returns response dict or None."""
    if not HOST_OPENER_URL:
        return None
    import urllib.request
    import urllib.error
    try:
        req = urllib.request.Request(
            f"{HOST_OPENER_URL}/{endpoint}",
            data=json.dumps({"file_path": file_path}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except Exception:
            return {"error": f"host opener HTTP {e.code}"}
    except urllib.error.URLError as e:
        logging.warning("host opener not reachable at %s: %s", HOST_OPENER_URL, e)
        return {"error": "host_opener.py not running"}
    except Exception as e:
        logging.warning("host opener error: %s", e)
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

class BackgroundJob:
    """Run a chain of subprocess stages in the background, one at a time.

    status() has the same shape as /api/refresh/status (running, stage,
    last_result), so the nav ⟳ button can drive any job that uses this.
    Each stage runs in its own process group so cancel() stops children too.
    """

    def __init__(self, log_path: Path, cwd: Path):
        self.log_path = Path(log_path)
        self.cwd = Path(cwd)
        self._lock = threading.Lock()
        self._state = {"running": False, "stage": None, "started_at": None, "last_completed_at": None,
                       "last_result": None, "pid": None, "exit_code": None}

    def _set(self, **kv):
        with self._lock:
            self._state.update(kv)

    def status(self) -> dict:
        with self._lock:
            s = dict(self._state)
        tail = []
        if self.log_path.exists():
            try:
                data = self.log_path.read_bytes()[-32 * 1024:].decode("utf-8", errors="replace")
                tail = [l for l in data.replace("\r", "\n").splitlines() if l.strip()][-8:]
            except OSError:
                pass
        s["tail"] = tail
        return s

    def start(self, stages: list[tuple[str, list[str]]]) -> bool:
        """Start the stages unless a run is already in progress. Returns False if busy."""
        with self._lock:
            if self._state["running"]:
                return False
            self._state.update(running=True, stage=stages[0][0] if stages else None,
                               started_at=time.time(), pid=None, exit_code=None)
        threading.Thread(target=self._run, args=(stages,), daemon=True).start()
        return True

    def _run(self, stages):
        summary, ok = [], True
        try:
            with open(self.log_path, "w") as log:
                for name, cmd in stages:
                    self._set(stage=name)
                    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                            cwd=self.cwd, start_new_session=True)
                    self._set(pid=proc.pid)
                    rc = proc.wait()
                    self._set(exit_code=rc)
                    if rc != 0:
                        ok = False
                        summary.append(f"{name} failed (exit {rc})")
                        break
                    summary.append(f"{name} ok")
        except Exception as e:
            ok = False
            summary.append(f"error: {e}")
        finally:
            self._set(running=False, stage=None, pid=None, last_completed_at=time.time(),
                      last_result={"ok": ok, "summary": "; ".join(summary) or "nothing ran"})

    def cancel(self) -> int | None:
        with self._lock:
            pid = self._state["pid"] if self._state["running"] else None
        if pid is not None:
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        return pid
