"""Optional completion-callback receiver (alternative to polling).

The Devin v1 Sessions API does not push status changes, so in webhook mode the
Streamlit app asks Devin (in the prompt) to POST its final structured output to
this endpoint. Each callback is written to ``<events-dir>/<callback_id>.json``,
which the Streamlit app reads when it shares the same filesystem.

    WEBHOOK_SECRET=change-me python streamlit_app/webhook_receiver.py --port 8765

``WEBHOOK_SECRET`` never leaves the app/receiver. Each session gets its own
``X-Webhook-Token`` (``<callback_id>.<HMAC(secret, callback_id)>``), and the
callback is stored under that ``callback_id``, so a session can only write its own
event. The endpoint must be reachable from Devin's machine (e.g. behind a reverse
proxy or tunnel with HTTPS).
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import tempfile
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DEFAULT_EVENTS_DIR = Path(__file__).resolve().parent / ".webhook_events"
MAX_BODY_BYTES = 1_000_000
_CALLBACK_ID_RE = re.compile(r"^[a-f0-9]{32}$")


def _sign(secret: str, callback_id: str) -> str:
    return hmac.new(secret.encode(), callback_id.encode(), hashlib.sha256).hexdigest()


def new_callback_token(secret: str) -> tuple[str, str]:
    """Return ``(callback_id, token)`` for one session; give only the token to Devin."""
    callback_id = secrets.token_hex(16)
    return callback_id, f"{callback_id}.{_sign(secret, callback_id)}"


def verify_token(secret: str, token: str) -> str | None:
    """Return the callback_id a token was issued for, or None if it is not valid."""
    callback_id, _, sig = (token or "").partition(".")
    if not secret or not _CALLBACK_ID_RE.match(callback_id) or not sig:
        return None
    if not hmac.compare_digest(sig, _sign(secret, callback_id)):
        return None
    return callback_id


def event_path(events_dir: Path, callback_id: str) -> Path | None:
    if not _CALLBACK_ID_RE.match(callback_id or ""):
        return None
    return events_dir / f"{callback_id}.json"


def read_event(events_dir: Path, callback_id: str) -> dict | None:
    path = event_path(events_dir, callback_id)
    if not path or not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def write_event(events_dir: Path, callback_id: str, payload: dict) -> None:
    """Atomically replace the event so readers never see a partial file."""
    path = event_path(events_dir, callback_id)
    if path is None:
        raise ValueError("invalid callback_id")
    events_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=events_dir, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh, indent=2)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


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
            callback_id = verify_token(secret, self.headers.get("X-Webhook-Token", ""))
            if callback_id is None:
                self._reply(401, {"error": "invalid token"})
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
            payload["callback_id"] = callback_id
            payload["received_at"] = datetime.now(timezone.utc).isoformat()
            write_event(events_dir, callback_id, payload)
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
