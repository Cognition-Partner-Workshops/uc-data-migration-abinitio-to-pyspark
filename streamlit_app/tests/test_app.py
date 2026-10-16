from pathlib import Path

from stub_devin_api import VALID_KEY, serve
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "app.py")


def test_app_renders_without_key_and_disables_launch():
    at = AppTest.from_file(APP, default_timeout=30).run()
    assert not at.exception
    launch = next(b for b in at.button if b.label == "Start Devin session")
    assert launch.disabled


def test_app_tracks_existing_session_until_finished(monkeypatch):
    server, state = serve(polls_until_finished=1)
    state.sessions["devin-stub0001"] = {"polls": 0, "title": "t"}
    monkeypatch.setenv("DEVIN_API_KEY", VALID_KEY)
    monkeypatch.setenv(
        "DEVIN_API_BASE_URL", f"http://127.0.0.1:{server.server_address[1]}/v1"
    )
    try:
        at = AppTest.from_file(APP, default_timeout=30).run()
        sid_input = next(t for t in at.text_input if t.label == "Session ID")
        sid_input.input("devin-stub0001")
        next(b for b in at.button if b.label == "Track").click()
        at.run()
        assert not at.exception
        md = " ".join(m.value for m in at.markdown)
        assert "finished" in md
        assert "https://github.com/example/repo/pull/1" in md
    finally:
        server.shutdown()


def test_webhook_callback_overrides_stale_snapshot(monkeypatch, tmp_path):
    import webhook_receiver as wr

    server, state = serve(polls_until_finished=99)
    state.sessions["devin-stub0001"] = {"polls": 0, "title": "t"}
    monkeypatch.setenv("DEVIN_API_KEY", VALID_KEY)
    monkeypatch.setenv(
        "DEVIN_API_BASE_URL", f"http://127.0.0.1:{server.server_address[1]}/v1"
    )
    cid, _ = wr.new_callback_token("s")
    wr.write_event(
        tmp_path,
        cid,
        {"status": "reconciliation_passed", "pr_url": "https://x/pull/9"},
    )
    try:
        at = AppTest.from_file(APP, default_timeout=30)
        at.session_state["sessions"] = {
            "devin-stub0001": {
                "url": None,
                "title": "t",
                "mode": "Webhook",
                "callback_id": cid,
            }
        }
        at.session_state["active"] = "devin-stub0001"
        at.session_state["mode"] = "Webhook"
        at.session_state["events_dir"] = str(tmp_path)
        at.run()
        assert not at.exception
        md = " ".join(m.value for m in at.markdown)
        assert "callback: reconciliation_passed" in md
        assert "https://x/pull/9" in md
        assert any("reconciliation_passed" in j.value for j in at.json)
    finally:
        server.shutdown()
