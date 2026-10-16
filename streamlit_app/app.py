"""Streamlit launcher: upload Ab Initio sources and start a Devin conversion session.

Run with:  streamlit run streamlit_app/app.py
"""

from __future__ import annotations

import os
import secrets
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

from devin_client import (
    ALLOWED_EXTENSIONS,
    DEFAULT_APP_URL,
    DEFAULT_BASE_URL,
    DEFAULT_REPO,
    ConversionRequest,
    DevinAPIError,
    DevinClient,
    build_prompt,
    decode_upload,
    is_terminal,
    last_devin_message,
    pr_url,
    session_state,
)
from webhook_receiver import DEFAULT_EVENTS_DIR, new_callback_token, read_event

MAX_CONSECUTIVE_POLL_ERRORS = 5
STATUS_ICONS = {
    "working": ":material/autorenew:",
    "blocked": ":material/pause_circle:",
    "finished": ":material/check_circle:",
    "expired": ":material/timer_off:",
}

st.set_page_config(
    page_title="Ab Initio → PySpark via Devin", page_icon=":material/sync_alt:"
)


def _secret(name: str) -> str:
    if os.environ.get(name):
        return os.environ[name]
    try:
        return str(st.secrets.get(name, ""))
    except Exception:  # no secrets.toml configured
        return ""


def _state():
    ss = st.session_state
    ss.setdefault("sessions", {})  # session_id -> {url, title, mode, callback_id}
    ss.setdefault("details", {})  # session_id -> last GET /sessions/{id} payload
    ss.setdefault("poll_errors", {})  # session_id -> consecutive error count
    ss.setdefault("last_error", {})  # session_id -> last error message
    ss.setdefault("active", None)
    ss.setdefault("webhook_secret", secrets.token_urlsafe(24))
    return ss


ss = _state()

# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.header("Devin API")
    api_key = st.text_input(
        "API key",
        value=_secret("DEVIN_API_KEY"),
        type="password",
        help="Personal access token or service-user key (cog_...). "
        "Read from DEVIN_API_KEY env var or .streamlit/secrets.toml if set.",
    )
    base_url = st.text_input(
        "API base URL", value=_secret("DEVIN_API_BASE_URL") or DEFAULT_BASE_URL
    ).strip()
    app_url = (
        st.text_input(
            "Devin web app URL",
            value=_secret("DEVIN_APP_URL") or DEFAULT_APP_URL,
            help="Used for session links when the API response has no URL "
            "(set this for enterprise deployments).",
        )
        .strip()
        .rstrip("/")
    )

    st.header("Target")
    repo = st.text_input("Target repo (owner/name)", value=DEFAULT_REPO).strip()
    playbook_id = st.text_input(
        "Playbook ID (optional)",
        help="If the conversion playbook is registered in your org, attach it by ID "
        "(e.g. playbook-...). The prompt references the in-repo playbook either way.",
    ).strip()

    st.header("Updates")
    mode = st.radio("Update mode", ["Poll", "Webhook"], horizontal=True, key="mode")
    poll_interval = st.slider(
        "Poll interval (seconds)", min_value=5, max_value=300, value=20, step=5
    )
    webhook_url = ""
    if mode == "Webhook":
        webhook_url = st.text_input(
            "Webhook URL",
            placeholder="https://your-host.example.com/devin-callback",
            help="Public HTTPS URL of streamlit_app/webhook_receiver.py. "
            "Devin POSTs its final structured output here.",
        ).strip()
        st.text_input(
            "Webhook secret",
            key="webhook_secret",
            type="password",
            help="Start the receiver with WEBHOOK_SECRET set to this value. It is never "
            "sent to Devin: each session gets its own derived WEBHOOK_TOKEN.",
        )
        events_dir = Path(
            st.text_input(
                "Receiver events dir", value=str(DEFAULT_EVENTS_DIR), key="events_dir"
            )
        )
    else:
        events_dir = DEFAULT_EVENTS_DIR

    with st.expander("Advanced"):
        max_acu = st.number_input(
            "Max ACU limit (0 = none)", min_value=0, value=0, step=1
        )
        tags_raw = st.text_input("Tags (comma-separated)", value="abinitio-to-pyspark")
        unlisted = st.checkbox("Unlisted session", value=False)
        idempotent = st.checkbox(
            "Idempotent create",
            value=True,
            help="Sends idempotent=true so a retried or repeated launch of the same "
            "request reuses the existing session instead of starting a duplicate.",
        )
        max_chars = st.number_input(
            "Max characters embedded per file",
            min_value=1_000,
            max_value=500_000,
            value=60_000,
            step=5_000,
            help="Larger files are truncated in the prompt and flagged to Devin.",
        )


def _client() -> DevinClient | None:
    if not api_key:
        st.error(
            "Enter a Devin API key in the sidebar (see README for how to create one)."
        )
        return None
    try:
        return DevinClient(api_key=api_key, base_url=base_url)
    except DevinAPIError as exc:
        st.error(exc.message)
        return None


# ---------------------------------------------------------------- launch form
st.title("Ab Initio → PySpark conversion")
st.caption(
    "Upload an Ab Initio graph and its companions; Devin converts it in the target repo "
    "following the conversion playbook and opens a PR."
)

uploads = st.file_uploader(
    "Ab Initio files (" + ", ".join(f".{e}" for e in ALLOWED_EXTENSIONS) + ")",
    type=list(ALLOWED_EXTENSIONS),
    accept_multiple_files=True,
)
col1, col2 = st.columns(2)
graph_name = col1.text_input("Graph name", placeholder="e.g. run_daily_orders")
namespace = col2.text_input("Namespace", value="dev")
target_jobs = st.text_input(
    "Target job(s) / output table(s)",
    placeholder="e.g. src/jobs/curated_transactions.py",
)
extra = st.text_area("Additional instructions (optional)", height=80)

sources = [decode_upload(f.name, f.getvalue(), int(max_chars)) for f in uploads or []]
for s in sources:
    if s.truncated:
        st.warning(
            f"`{s.name}` exceeds {int(max_chars):,} characters and will be truncated."
        )

tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
request = ConversionRequest(
    repo=repo,
    sources=sources,
    graph_name=graph_name.strip(),
    target_jobs=target_jobs.strip(),
    namespace=namespace.strip(),
    extra_instructions=extra,
    webhook_url=webhook_url,
    tags=tags,
)
prompt = build_prompt(request) if sources else ""
if sources:
    with st.expander(f"Prompt preview ({len(prompt):,} characters)"):
        st.code(prompt, language="markdown")

problems = []
if not sources:
    problems.append("upload at least one file")
if "/" not in repo:
    problems.append("set the target repo as owner/name")
if mode == "Webhook" and not webhook_url.startswith("https://"):
    problems.append("set an https:// webhook URL (or switch to Poll)")

if st.button("Start Devin session", type="primary", disabled=bool(problems)):
    client = _client()
    if client:
        title = f"Ab Initio → PySpark: {graph_name.strip() or sources[0].name}"
        session_secrets = None
        callback_id = None
        if mode == "Webhook":
            callback_id, token = new_callback_token(ss.webhook_secret)
            session_secrets = [
                {"key": "WEBHOOK_TOKEN", "value": token, "sensitive": True}
            ]
        try:
            with st.spinner("Creating session..."):
                created = client.create_session(
                    prompt,
                    title=title,
                    playbook_id=playbook_id or None,
                    tags=tags or None,
                    max_acu_limit=int(max_acu) or None,
                    unlisted=unlisted,
                    idempotent=idempotent,
                    session_secrets=session_secrets,
                )
        except DevinAPIError as exc:
            st.error(f"Could not start the session. {exc.message}")
        else:
            sid = created["session_id"]
            reused = created.get("is_new_session") is False and sid in ss.sessions
            if not reused:
                ss.sessions[sid] = {
                    "url": created.get("url"),
                    "title": title,
                    "created_at": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M UTC"
                    ),
                    "mode": mode,
                    "callback_id": callback_id,
                }
            ss.active = sid
            if created.get("is_new_session") is False:
                st.info(
                    f"Devin returned an existing session for this identical request: "
                    f"{created.get('url')}"
                )
            else:
                st.success(f"Session started: {created.get('url')}")
elif problems:
    st.caption("To start: " + "; ".join(problems) + ".")

with st.expander("Track an existing session"):
    existing = st.text_input("Session ID", placeholder="devin-...").strip()
    if st.button("Track", disabled=not existing):
        ss.sessions.setdefault(existing, {"url": None, "title": existing, "mode": mode})
        ss.active = existing


# ---------------------------------------------------------------- status panel
def _event(sid: str) -> dict | None:
    callback_id = ss.sessions.get(sid, {}).get("callback_id")
    return read_event(events_dir, callback_id) if callback_id else None


def _refresh(sid: str) -> None:
    client = _client()
    if not client:
        return
    try:
        ss.details[sid] = client.get_session(sid)
        ss.poll_errors[sid] = 0
        ss.last_error.pop(sid, None)
    except DevinAPIError as exc:
        ss.poll_errors[sid] = ss.poll_errors.get(sid, 0) + 1
        ss.last_error[sid] = exc.message


def _render(sid: str) -> None:
    meta = ss.sessions[sid]
    details = ss.details.get(sid, {})
    state = session_state(details) if details else "pending"
    url = meta.get("url") or details.get("url")
    if not url:
        url = f"{app_url}/sessions/{sid.removeprefix('devin-')}"
    event = _event(sid)
    label = state
    if event and not is_terminal(details):
        # the API snapshot may be stale; surface the callback's final status
        label = f"{state} · callback: {event.get('status') or 'received'}"

    st.subheader(meta.get("title") or sid)
    c1, c2 = st.columns([1, 2])
    c1.markdown(f"{STATUS_ICONS.get(state, ':material/help:')} **{label}**")
    c2.link_button("Open session in Devin", url)
    if details.get("updated_at"):
        st.caption(f"Last updated {details['updated_at']}")

    if sid in ss.last_error:
        st.warning(
            f"Last status check failed ({ss.poll_errors.get(sid, 0)} in a row): "
            f"{ss.last_error[sid]}"
        )

    if event:
        st.info(f"Webhook callback received {event.get('received_at', '')}")

    link = pr_url(details) or (event or {}).get("pr_url")
    if link:
        st.markdown(f"**Pull request:** [{link}]({link})")

    structured = event or details.get("structured_output")
    if structured:
        st.markdown("**Structured output**")
        st.json(structured)

    final = last_devin_message(details)
    if final:
        st.markdown("**Latest message from Devin**")
        with st.container(border=True):
            st.markdown(final)

    msgs = details.get("messages") or []
    if msgs:
        with st.expander(f"Message history ({len(msgs)})"):
            for m in msgs:
                who = m.get("username") or m.get("type", "")
                st.markdown(f"`{m.get('timestamp', '')}` **{who}**")
                st.text(m.get("message", ""))

    if state in ("blocked", "working"):
        with st.form(f"msg-{sid}", clear_on_submit=True):
            reply = st.text_input("Send a message to Devin")
            if st.form_submit_button("Send") and reply.strip():
                client = _client()
                if client:
                    try:
                        client.send_message(sid, reply.strip())
                        st.toast("Message sent")
                    except DevinAPIError as exc:
                        st.error(exc.message)


if ss.sessions:
    st.divider()
    ids = list(ss.sessions)
    ss.active = st.selectbox(
        "Session",
        ids,
        index=ids.index(ss.active) if ss.active in ids else len(ids) - 1,
        format_func=lambda i: f"{ss.sessions[i].get('title')} ({i})",
    )
    sid = ss.active
    if st.button("Refresh now"):
        _refresh(sid)

    done = bool(ss.details.get(sid)) and is_terminal(ss.details[sid])
    too_many_errors = ss.poll_errors.get(sid, 0) >= MAX_CONSECUTIVE_POLL_ERRORS
    webhook_mode = ss.sessions[sid].get("mode") == "Webhook"
    if too_many_errors:
        st.error(
            "Automatic polling paused after repeated failures. Use *Refresh now* to retry."
        )
    auto = not done and not too_many_errors

    @st.fragment(run_every=poll_interval if auto else None)
    def status_panel() -> None:
        if auto and (not webhook_mode or sid not in ss.details):
            _refresh(sid)
        elif auto and webhook_mode and _event(sid):
            _refresh(sid)
        _render(sid)
        now_done = bool(ss.details.get(sid)) and is_terminal(ss.details[sid])
        now_errors = ss.poll_errors.get(sid, 0) >= MAX_CONSECUTIVE_POLL_ERRORS
        if auto and (now_done or now_errors):
            st.rerun()  # full rerun re-creates the fragment without run_every

    status_panel()
