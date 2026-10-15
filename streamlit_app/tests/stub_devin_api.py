"""In-process stub of the Devin v1 Sessions API for tests and offline UI demos.

python streamlit_app/tests/stub_devin_api.py --port 8799
# then set the app's API base URL to http://127.0.0.1:8799/v1 and any API key
"""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VALID_KEY = "cog_test"


class StubState:
    def __init__(self, polls_until_finished: int = 2):
        self.polls_until_finished = polls_until_finished
        self.sessions: dict[str, dict] = {}
        self.created_payloads: list[dict] = []
        self.fail_next: list[int] = []
        self.lock = threading.Lock()


def make_handler(state: StubState, accept_any_key: bool = False):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # keep test output quiet
            pass

        def _send(self, code: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _auth_ok(self) -> bool:
            auth = self.headers.get("Authorization", "")
            if accept_any_key and auth.startswith("Bearer ") and len(auth) > 7:
                return True
            return auth == f"Bearer {VALID_KEY}"

        def _guard(self) -> bool:
            if state.fail_next:
                self._send(state.fail_next.pop(0), {"detail": "injected failure"})
                return False
            if not self._auth_ok():
                self._send(401, {"detail": "Invalid API key"})
                return False
            return True

        def do_POST(self):  # noqa: N802
            if not self._guard():
                return
            body = json.loads(
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
            )
            if self.path == "/v1/sessions":
                if not body.get("prompt"):
                    self._send(
                        422,
                        {
                            "detail": [
                                {"loc": ["body", "prompt"], "msg": "field required"}
                            ]
                        },
                    )
                    return
                with state.lock:
                    state.created_payloads.append(body)
                    sid = f"devin-stub{len(state.sessions) + 1:04d}"
                    state.sessions[sid] = {"polls": 0, "title": body.get("title")}
                self._send(
                    200,
                    {
                        "session_id": sid,
                        "url": f"https://app.devin.ai/sessions/{sid[6:]}",
                        "is_new_session": True,
                    },
                )
            elif self.path.endswith("/message"):
                self._send(200, {"detail": "ok"})
            else:
                self._send(404, {"detail": "Not Found"})

        def do_GET(self):  # noqa: N802
            if not self._guard():
                return
            sid = self.path.rsplit("/", 1)[-1]
            sess = state.sessions.get(sid)
            if not self.path.startswith("/v1/sessions/") or sess is None:
                self._send(404, {"detail": "Session not found"})
                return
            with state.lock:
                sess["polls"] += 1
                finished = sess["polls"] >= state.polls_until_finished
            pr = "https://github.com/example/repo/pull/1"
            messages = [
                {
                    "event_id": "e1",
                    "type": "initial_user_message",
                    "message": "Convert...",
                    "timestamp": "2026-01-01T00:00:00Z",
                },
                {
                    "event_id": "e2",
                    "type": "devin_message",
                    "message": "Reading the playbook and the uploaded graph.",
                    "timestamp": "2026-01-01T00:01:00Z",
                },
            ]
            if finished:
                messages.append(
                    {
                        "event_id": "e3",
                        "type": "devin_message",
                        "message": f"Done. All reconciliation controls PASS. PR: {pr}",
                        "timestamp": "2026-01-01T00:30:00Z",
                    }
                )
            self._send(
                200,
                {
                    "session_id": sid,
                    "status": "stopped" if finished else "running",
                    "status_enum": "finished" if finished else "working",
                    "title": sess["title"],
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:30:00Z"
                    if finished
                    else "2026-01-01T00:01:00Z",
                    "messages": messages,
                    "pull_request": {"url": pr} if finished else None,
                    "structured_output": (
                        {"pr_url": pr, "status": "reconciliation_passed"}
                        if finished
                        else {"pr_url": None, "status": "in_progress"}
                    ),
                },
            )

    return Handler


def serve(port: int = 0, polls_until_finished: int = 2, accept_any_key: bool = False):
    state = StubState(polls_until_finished)
    server = ThreadingHTTPServer(
        ("127.0.0.1", port), make_handler(state, accept_any_key)
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8799)
    parser.add_argument("--polls-until-finished", type=int, default=4)
    args = parser.parse_args()
    srv, _ = serve(args.port, args.polls_until_finished, accept_any_key=True)
    print(
        f"Stub Devin API on http://127.0.0.1:{srv.server_address[1]}/v1 (any Bearer key)"
    )
    threading.Event().wait()
