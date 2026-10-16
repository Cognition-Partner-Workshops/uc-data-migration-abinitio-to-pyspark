import json
import urllib.request
from pathlib import Path

import pytest
from stub_devin_api import VALID_KEY, serve

import devin_client as dc
from webhook_receiver import read_event


@pytest.fixture
def stub():
    server, state = serve(polls_until_finished=2)
    base = f"http://127.0.0.1:{server.server_address[1]}/v1"
    yield base, state
    server.shutdown()


def _request(**kw):
    defaults = dict(
        repo=dc.DEFAULT_REPO,
        sources=[dc.UploadedSource("orders.mp", "graph body ``` with fence")],
        graph_name="orders",
        namespace="dev",
    )
    defaults.update(kw)
    return dc.ConversionRequest(**defaults)


def test_prompt_references_playbook_skills_and_files():
    prompt = dc.build_prompt(_request())
    assert dc.DEFAULT_REPO in prompt
    assert dc.PLAYBOOK_PATH in prompt
    assert dc.SKILLS_PATH in prompt
    assert "### `orders.mp`" in prompt
    # content containing ``` gets a longer fence so the block stays intact
    assert "````\ngraph body ``` with fence\n````" in prompt
    assert "Completion callback" not in prompt


def test_prompt_webhook_section():
    prompt = dc.build_prompt(_request(webhook_url="https://hook.example.com/cb"))
    assert "https://hook.example.com/cb" in prompt
    assert "$WEBHOOK_TOKEN" in prompt


def test_decode_upload_truncates_and_handles_latin1():
    src = dc.decode_upload("a.dml", "é".encode("latin-1") + b"x" * 20, max_chars=5)
    assert src.truncated and len(src.content) == 5


def test_create_and_poll_until_finished(stub):
    base, state = stub
    client = dc.DevinClient(VALID_KEY, base_url=base)
    created = client.create_session("hello", title="t", tags=["x"], max_acu_limit=None)
    payload = state.created_payloads[0]
    assert payload["structured_output_schema"]["required"] == ["pr_url", "status"]
    assert "max_acu_limit" not in payload

    first = client.get_session(created["session_id"])
    assert not dc.is_terminal(first)
    assert dc.pr_url(first) is None
    second = client.get_session(created["session_id"])
    assert dc.is_terminal(second)
    assert dc.pr_url(second) == "https://github.com/example/repo/pull/1"
    assert dc.last_devin_message(second).startswith("Done.")


def test_bad_key_is_friendly_401(stub):
    base, _ = stub
    with pytest.raises(dc.DevinAPIError) as exc:
        dc.DevinClient("cog_wrong", base_url=base).create_session("hi")
    assert exc.value.status_code == 401
    assert "API key" in exc.value.message


def test_validation_error_detail(stub):
    base, _ = stub
    with pytest.raises(dc.DevinAPIError) as exc:
        dc.DevinClient(VALID_KEY, base_url=base).create_session("")
    assert exc.value.status_code == 422
    assert "body.prompt: field required" in exc.value.message


def test_retries_server_errors_then_succeeds(stub, monkeypatch):
    base, state = stub
    monkeypatch.setattr(dc.time, "sleep", lambda _: None)
    state.fail_next = [503, 502]
    created = dc.DevinClient(VALID_KEY, base_url=base, max_retries=2).create_session(
        "hi", idempotent=True
    )
    assert created["session_id"].startswith("devin-")


def test_non_idempotent_create_is_not_retried_on_5xx(stub, monkeypatch):
    base, state = stub
    monkeypatch.setattr(dc.time, "sleep", lambda _: None)
    state.fail_next = [503, 503]
    with pytest.raises(dc.DevinAPIError, match="may still have been applied"):
        dc.DevinClient(VALID_KEY, base_url=base, max_retries=2).create_session("hi")
    assert state.fail_next == [503]  # exactly one attempt


def test_lost_create_response_does_not_duplicate_sessions(stub):
    base, state = stub
    client = dc.DevinClient(VALID_KEY, base_url=base, timeout=0.3, max_retries=2)

    state.create_delays = [1.0]
    with pytest.raises(dc.DevinAPIError, match="may still have been applied"):
        client.create_session("plain")
    assert len(state.sessions) == 1  # committed once, never retried

    state.create_delays = [1.0]
    created = client.create_session("idem", idempotent=True)
    assert created["is_new_session"] is False  # retry returned the first session
    assert len(state.sessions) == 2


def test_rate_limited_create_is_retried(stub, monkeypatch):
    base, state = stub
    monkeypatch.setattr(dc.time, "sleep", lambda _: None)
    state.fail_next = [429]
    created = dc.DevinClient(VALID_KEY, base_url=base).create_session("hi")
    assert created["is_new_session"] is True


def test_gives_up_after_max_retries(stub, monkeypatch):
    base, state = stub
    monkeypatch.setattr(dc.time, "sleep", lambda _: None)
    state.fail_next = [500, 500, 500]
    with pytest.raises(dc.DevinAPIError) as exc:
        dc.DevinClient(VALID_KEY, base_url=base, max_retries=2).create_session(
            "hi", idempotent=True
        )
    assert exc.value.retryable and exc.value.status_code == 500


def test_unreachable_host(monkeypatch):
    monkeypatch.setattr(dc.time, "sleep", lambda _: None)
    client = dc.DevinClient(
        VALID_KEY, base_url="http://127.0.0.1:9/v1", timeout=1, max_retries=0
    )
    with pytest.raises(dc.DevinAPIError, match="Could not connect"):
        client.get_session("devin-x")


def test_missing_key():
    with pytest.raises(dc.DevinAPIError, match="No Devin API key"):
        dc.DevinClient("")


def _post(url, body, token):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"X-Webhook-Token": token}
    )
    return urllib.request.urlopen(req)


def test_webhook_receiver_roundtrip(tmp_path):
    from http.server import ThreadingHTTPServer
    from threading import Thread

    import webhook_receiver as wr

    server = ThreadingHTTPServer(("127.0.0.1", 0), wr.make_handler("s3cret", tmp_path))
    Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    cid_a, token_a = wr.new_callback_token("s3cret")
    cid_b, _ = wr.new_callback_token("s3cret")
    body = {"session_id": "devin-abc", "pr_url": "https://x/pull/2"}
    try:
        for bad in ("nope", "s3cret", f"{cid_b}.{token_a.split('.')[1]}"):
            with pytest.raises(urllib.error.HTTPError) as exc:
                _post(url, body, bad)
            assert exc.value.code == 401

        assert _post(url, body, token_a).status == 200
        assert read_event(tmp_path, cid_a)["pr_url"] == "https://x/pull/2"
        assert read_event(tmp_path, cid_b) is None

        # a session's body cannot redirect the write to another session's event
        _post(url, {"session_id": cid_b, "callback_id": cid_b}, token_a)
        assert read_event(tmp_path, cid_b) is None
        assert read_event(tmp_path, cid_a)["callback_id"] == cid_a
        assert not list(tmp_path.glob(".tmp-*"))
    finally:
        server.shutdown()


def test_verify_token_rejects_other_secret():
    import webhook_receiver as wr

    cid, token = wr.new_callback_token("one")
    assert wr.verify_token("one", token) == cid
    assert wr.verify_token("two", token) is None
    assert wr.verify_token("", token) is None
    assert wr.event_path(Path("/tmp"), "../../etc") is None
