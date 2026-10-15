"""Thin client for the Devin v1 Sessions API plus the conversion-prompt builder.

Kept free of Streamlit imports so it can be unit-tested and reused from scripts.
API reference: https://docs.devin.ai/api-reference/v1/overview
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import requests

DEFAULT_BASE_URL = "https://api.devin.ai/v1"
DEFAULT_REPO = "Cognition-Partner-Workshops/uc-data-migration-abinitio-to-pyspark"
PLAYBOOK_PATH = ".workshop/playbooks/abinitio-to-pyspark-conversion.devin.md"
SKILLS_PATH = ".agents/skills/"
ALLOWED_EXTENSIONS = ("mp", "ksh", "dml", "pset", "xfr")

TERMINAL_STATUSES = {"finished", "expired"}
WAITING_STATUSES = {"blocked"}

STRUCTURED_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "pr_url": {
            "type": ["string", "null"],
            "description": "URL of the pull request containing the conversion.",
        },
        "status": {
            "type": "string",
            "enum": [
                "in_progress",
                "reconciliation_passed",
                "reconciliation_failed",
                "blocked",
            ],
        },
        "converted_jobs": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Paths of the PySpark jobs created or changed under src/jobs/.",
        },
        "reconciliation_summary": {
            "type": "string",
            "description": "One line per control: name and PASS/FAIL/SKIP.",
        },
        "source_quirks": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Legacy behaviours reproduced as source-faithful and flagged.",
        },
    },
    "required": ["pr_url", "status"],
    "additionalProperties": True,
}


class DevinAPIError(Exception):
    """A Devin API call failed; ``message`` is safe to show in the UI."""

    def __init__(
        self, message: str, status_code: int | None = None, retryable: bool = False
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.retryable = retryable


@dataclass
class UploadedSource:
    name: str
    content: str
    truncated: bool = False


@dataclass
class ConversionRequest:
    repo: str
    sources: list[UploadedSource]
    graph_name: str = ""
    target_jobs: str = ""
    namespace: str = "dev"
    extra_instructions: str = ""
    webhook_url: str = ""
    tags: list[str] = field(default_factory=list)


def decode_upload(name: str, data: bytes, max_chars: int) -> UploadedSource:
    """Decode an uploaded Ab Initio file as text, truncating to ``max_chars``."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("latin-1")
    if len(text) > max_chars:
        return UploadedSource(name=name, content=text[:max_chars], truncated=True)
    return UploadedSource(name=name, content=text)


def _fence_for(content: str) -> str:
    fence = "```"
    while fence in content:
        fence += "`"
    return fence


def build_prompt(req: ConversionRequest) -> str:
    lines = [
        f"Convert the uploaded Ab Initio graph to a verified PySpark job in the repo `{req.repo}`.",
        "",
        "Follow the conversion procedure in the playbook "
        f"`{PLAYBOOK_PATH}` in that repo (read it first; it defines the source-parity "
        "principle, procedure, postconditions and forbidden actions), and the repo "
        f"mechanics in the Skill(s) under `{SKILLS_PATH}` (commands, namespaces, where "
        "DML schemas and reconciliation controls live).",
        "",
        "## Inputs",
        f"- Graph: {req.graph_name or '(infer from the uploaded files)'}",
        f"- Target job(s): {req.target_jobs or '(choose per the playbook, under src/jobs/)'}",
        f"- Namespace: {req.namespace or 'dev'}",
    ]
    if req.extra_instructions.strip():
        lines += ["", "## Additional instructions", req.extra_instructions.strip()]

    lines += [
        "",
        "## Deliverable",
        "Open a PR against the default branch with the converted job(s), the "
        "reconciliation controls and the reconciliation report. Keep the structured "
        "output updated as you go: set `pr_url` as soon as the PR exists and `status` "
        "to `reconciliation_passed` / `reconciliation_failed` at the end.",
    ]
    if req.webhook_url:
        lines += [
            "",
            "## Completion callback",
            f"When you are done (or blocked), POST the final structured output as JSON to "
            f"`{req.webhook_url}` with header `X-Webhook-Secret: $WEBHOOK_SECRET` "
            "(the secret is provided to this session as the `WEBHOOK_SECRET` environment "
            "variable). Include your `session_id` in the body.",
        ]

    lines += ["", "## Uploaded Ab Initio source files", ""]
    for src in req.sources:
        fence = _fence_for(src.content)
        note = (
            " (TRUNCATED by uploader - ask for the full file if needed)"
            if src.truncated
            else ""
        )
        lines += [f"### `{src.name}`{note}", fence, src.content.rstrip("\n"), fence, ""]
    return "\n".join(lines).rstrip() + "\n"


def _error_detail(resp: requests.Response) -> str:
    try:
        body = resp.json()
    except ValueError:
        return (resp.text or "").strip()[:500]
    detail = body.get("detail", body) if isinstance(body, dict) else body
    if isinstance(detail, list):
        parts = []
        for d in detail:
            if isinstance(d, dict):
                loc = ".".join(str(x) for x in d.get("loc", []))
                parts.append(
                    f"{loc}: {d.get('msg', d)}" if loc else str(d.get("msg", d))
                )
            else:
                parts.append(str(d))
        return "; ".join(parts)
    return str(detail)[:500]


class DevinClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        max_retries: int = 2,
        session: requests.Session | None = None,
    ):
        if not api_key:
            raise DevinAPIError("No Devin API key configured.")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.http = session or requests.Session()
        self.http.headers.update(
            {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            try:
                resp = self.http.request(method, url, timeout=self.timeout, **kwargs)
            except requests.Timeout as exc:
                err = DevinAPIError(f"Request to {url} timed out.", retryable=True)
                if attempt >= self.max_retries:
                    raise err from exc
            except requests.ConnectionError as exc:
                err = DevinAPIError(
                    f"Could not connect to {self.base_url}.", retryable=True
                )
                if attempt >= self.max_retries:
                    raise err from exc
            except requests.RequestException as exc:
                raise DevinAPIError(f"Request failed: {exc}") from exc
            else:
                if resp.ok:
                    try:
                        return resp.json()
                    except ValueError as exc:
                        raise DevinAPIError(
                            "Devin API returned a non-JSON response."
                        ) from exc
                err = self._http_error(resp)
                if not err.retryable or attempt >= self.max_retries:
                    raise err
                retry_after = resp.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    time.sleep(min(int(retry_after), 30))
                    attempt += 1
                    continue
            attempt += 1
            time.sleep(min(2**attempt, 10))

    @staticmethod
    def _http_error(resp: requests.Response) -> DevinAPIError:
        code = resp.status_code
        detail = _error_detail(resp)
        if code == 401:
            msg = "Unauthorized (401): the API key is missing, invalid or revoked."
        elif code == 403:
            msg = "Forbidden (403): the API key lacks permission for this organization/action."
        elif code == 404:
            msg = "Not found (404): check the session ID and API base URL."
        elif code == 422:
            msg = f"Request rejected by validation (422): {detail}"
        elif code == 429:
            msg = "Rate limited (429): slow down the poll interval and retry."
        elif code >= 500:
            msg = f"Devin API server error ({code}). Try again shortly."
        else:
            msg = f"Devin API error ({code}): {detail}"
        if detail and code != 422 and (code in (401, 403, 404, 429) or code >= 500):
            msg = f"{msg} Details: {detail}"
        return DevinAPIError(
            msg, status_code=code, retryable=code == 429 or code >= 500
        )

    def create_session(
        self,
        prompt: str,
        *,
        title: str | None = None,
        playbook_id: str | None = None,
        tags: list[str] | None = None,
        max_acu_limit: int | None = None,
        idempotent: bool = False,
        unlisted: bool = False,
        session_secrets: list[dict[str, Any]] | None = None,
        structured_output_schema: dict[str, Any] | None = STRUCTURED_OUTPUT_SCHEMA,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "prompt": prompt,
            "idempotent": idempotent,
            "unlisted": unlisted,
        }
        optional = {
            "title": title,
            "playbook_id": playbook_id,
            "tags": tags,
            "max_acu_limit": max_acu_limit,
            "session_secrets": session_secrets,
            "structured_output_schema": structured_output_schema,
        }
        payload.update({k: v for k, v in optional.items() if v})
        return self._request("POST", "/sessions", json=payload)

    def get_session(self, session_id: str) -> dict[str, Any]:
        return self._request("GET", f"/sessions/{session_id}")

    def send_message(self, session_id: str, message: str) -> dict[str, Any]:
        return self._request(
            "POST", f"/sessions/{session_id}/message", json={"message": message}
        )


def session_state(details: dict[str, Any]) -> str:
    return (details.get("status_enum") or details.get("status") or "unknown").lower()


def is_terminal(details: dict[str, Any]) -> bool:
    return session_state(details) in TERMINAL_STATUSES


def last_devin_message(details: dict[str, Any]) -> str | None:
    for msg in reversed(details.get("messages") or []):
        if msg.get("type") == "devin_message" and msg.get("message"):
            return msg["message"]
    return None


def pr_url(details: dict[str, Any]) -> str | None:
    structured = details.get("structured_output")
    if isinstance(structured, dict) and structured.get("pr_url"):
        return structured["pr_url"]
    pr = details.get("pull_request")
    if isinstance(pr, dict) and pr.get("url"):
        return pr["url"]
    return None
