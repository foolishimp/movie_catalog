#!/usr/bin/env python3
"""
Host-side opener service for Docker deployments.
Run this on your Mac so the Docker web app can open VLC and Finder.

Usage:
    python host_opener.py [--port 9111]

Then set HOST_OPENER_URL=http://host.docker.internal:9111 in docker-compose.yml
(already configured by default).
"""
import json
import subprocess
import os
from http.server import HTTPServer, BaseHTTPRequestHandler


class OpenerHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
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

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                self._respond(500, {"error": result.stderr.strip() or "command failed"})
            else:
                self._respond(200, {"ok": True})
        except Exception as e:
            self._respond(500, {"error": str(e)})

    def _respond(self, status, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, fmt, *args):
        print(f"[opener] {args[0]}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Host-side opener for media-catalog")
    parser.add_argument("--port", type=int, default=int(os.environ.get("OPENER_PORT", 9111)))
    args = parser.parse_args()

    print(f"Host opener listening on :{args.port}")
    print("Endpoints: POST /open-vlc, POST /reveal  (body: {\"file_path\": \"...\"})")
    HTTPServer(("0.0.0.0", args.port), OpenerHandler).serve_forever()
