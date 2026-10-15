"""Optional completion-callback receiver (alternative to polling).

The Devin v1 Sessions API does not push status changes, so in webhook mode the
Streamlit app asks Devin (in the prompt) to POST its final structured output to
this endpoint. Each callback is written to ``<events-dir>/<session_id>.json``,
which the Streamlit app reads when it shares the same filesystem.

    WEBHOOK_SECRET=change-me python streamlit_app/webhook_receiver.py --port 8765

The endpoint must be reachable from Devin's machine (e.g. behind a reverse proxy
or tunnel with HTTPS). Requests without a matching ``X-Webhook-Secret`` are rejected.
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import re
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DEFAULT_EVENTS_DIR = Path(__file__).resolve().parent / ".webhook_events"
MAX_BODY_BYTES = 1_000_000
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def event_path(events_dir: Path, session_id: str) -> Path | None:
    if not _SESSION_ID_RE.match(session_id or ""):
        return None
    return events_dir / f"{session_id}.json"


def read_event(events_dir: Path, session_id: str) -> dict | None:
    candidates = {session_id, session_id.removeprefix("devin-")}
    for sid in candidates:
        path = event_path(events_dir, sid)
        if path and path.exists():
            try:
                return json.loads(path.read_text())
            except (OSError, ValueError):
                return None
    return None


def make_handler(secret: str, events_dir: Path) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _reply(self, code: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            self._reply(200, {"ok": True})

        def do_POST(self) -> None:  # noqa: N802
            provided = self.headers.get("X-Webhook-Secret", "")
            if not secret or not hmac.compare_digest(provided, secret):
                self._reply(401, {"error": "invalid secret"})
                return
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > MAX_BODY_BYTES:
                self._reply(
                    413 if length > MAX_BODY_BYTES else 400, {"error": "bad body size"}
                )
                return
            try:
                payload = json.loads(self.rfile.read(length))
            except ValueError:
                self._reply(400, {"error": "body must be JSON"})
                return
            if not isinstance(payload, dict):
                self._reply(400, {"error": "body must be a JSON object"})
                return
            path = event_path(events_dir, str(payload.get("session_id", "")))
            if path is None:
                self._reply(400, {"error": "missing or invalid session_id"})
                return
            payload["received_at"] = datetime.now(timezone.utc).isoformat()
            events_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2))
            self._reply(200, {"ok": True})

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--events-dir", type=Path, default=DEFAULT_EVENTS_DIR)
    args = parser.parse_args()
    secret = os.environ.get("WEBHOOK_SECRET", "")
    if not secret:
        raise SystemExit("Set WEBHOOK_SECRET before starting the receiver.")
    server = ThreadingHTTPServer(
        (args.host, args.port), make_handler(secret, args.events_dir)
    )
    print(f"Listening on http://{args.host}:{args.port}, writing to {args.events_dir}")
    server.serve_forever()


if __name__ == "__main__":
    main()
