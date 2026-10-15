# Ab Initio → PySpark conversion launcher (Streamlit)

A small Streamlit UI that takes uploaded Ab Initio sources (`.mp`, `.ksh`, `.dml`,
`.pset`, plus `.xfr`), starts a Devin session through the
[Devin v1 Sessions API](https://docs.devin.ai/api-reference/v1/overview) to convert
them in this repo, and tracks the session until Devin opens a PR.

The generated prompt:

- names the target repo (default `Cognition-Partner-Workshops/uc-data-migration-abinitio-to-pyspark`),
- tells Devin to follow `.workshop/playbooks/abinitio-to-pyspark-conversion.devin.md`
  and the repo Skill(s) under `.agents/skills/`,
- embeds every uploaded file verbatim in a fenced block (truncated past a configurable size, and flagged to Devin when truncated),
- asks for a PR plus a `structured_output` (`pr_url`, `status`, `converted_jobs`,
  `reconciliation_summary`, `source_quirks`). The JSON schema is sent as
  `structured_output_schema` on session creation.

## Files

| File | Purpose |
|---|---|
| `app.py` | Streamlit UI: upload, configure, launch, poll, display |
| `devin_client.py` | `requests` client for `POST /v1/sessions`, `GET /v1/sessions/{id}`, `POST /v1/sessions/{id}/message`, plus the prompt builder |
| `webhook_receiver.py` | Optional stdlib HTTP receiver for completion callbacks (instead of polling) |
| `tests/` | pytest suite + `stub_devin_api.py`, a local fake of the API for offline demos |

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r streamlit_app/requirements.txt
streamlit run streamlit_app/app.py
```

### 1. Create a Devin API key

Either kind works with the v1 API, sent as `Authorization: Bearer <key>`
([docs](https://docs.devin.ai/api-reference/authentication)):

- **Personal access token**: acts as you. Create it in Devin under
  **Settings → API keys / Personal access tokens**. Good for local use.
- **Service user API key**: a non-human identity with its own role, created by an
  org/enterprise admin under **Settings → Service users**. Use this for a shared
  deployment so sessions keep working after people leave or change roles.

Both start with `cog_`. The key needs permission to create sessions in the org that
has GitHub access to the target repo.

### 2. Provide the key to the app

You can do any of these (listed by precedence):

1. Environment variable: `export DEVIN_API_KEY=cog_...`
2. `.streamlit/secrets.toml` (keep it out of git):
   ```toml
   DEVIN_API_KEY = "cog_..."
   # DEVIN_API_BASE_URL = "https://api.devin.ai/v1"
   ```
3. Paste it into the **API key** field in the sidebar. It stays in the browser session only.

If your Devin deployment uses a different API host, set **API base URL** in the sidebar
(or `DEVIN_API_BASE_URL`).

## Using the app

1. In the sidebar, set the **Target repo** (`owner/name`), the **Poll interval**
   (5–300 s), and optionally a **Playbook ID** if the conversion playbook is
   registered in your org (`!convert-abinitio-to-pyspark`). Under *Advanced* you can set an ACU limit,
   tags, unlisted, and the per-file embed limit.
2. Upload the graph's files. You can also fill in the graph name, target job(s) and
   namespace, which match the playbook's "Required from user" inputs.
3. Check the prompt in **Prompt preview**, then click **Start Devin session**.
4. The status panel refreshes every *poll interval* seconds. It shows the session status,
   the **Open session in Devin** link, the PR URL (from `structured_output.pr_url` or
   `pull_request.url`), the raw `structured_output`, Devin's latest message and the full
   message history. When the session is `working` or `blocked`, you can reply to Devin
   from the panel. Polling stops once the session is `finished` or `expired`.
   To follow a session started somewhere else, use **Track an existing session**.

### Error handling

- 401/403/404/422 produce a clear message: bad key, missing permission, unknown
  session, or validation details from the API.
- Timeouts, connection errors, 429 and 5xx responses are retried with backoff
  (and `Retry-After` is respected). If polling fails 5 times in a row, it pauses
  and shows a **Refresh now** button.

## Optional: webhook instead of polling

The v1 Sessions API has no push notifications for session status, so the webhook
mode works the other way around. When you pick **Update mode → Webhook**:

- The prompt asks Devin to `POST` its final structured output (plus `session_id`)
  to your **Webhook URL** with the header `X-Webhook-Secret: $WEBHOOK_SECRET`.
- The secret goes to the session as a sensitive `session_secrets` entry
  (`WEBHOOK_SECRET`). It never appears in the prompt text.
- The app calls the API once for the session link. After that it only watches the
  receiver's events directory, and does a final API refresh when the callback arrives.

To run the receiver:

```bash
WEBHOOK_SECRET='<same value as the sidebar "Webhook secret">' \
  python streamlit_app/webhook_receiver.py --port 8765 --events-dir streamlit_app/.webhook_events
```

Expose it over HTTPS at a URL that Devin's machine can reach (reverse proxy, ingress or
tunnel), and enter that URL in the sidebar. The receiver checks the secret
(constant-time compare), caps the body at 1 MB, and writes `<session_id>.json` into
the events dir. The Streamlit app must be able to read that dir (same host or a shared volume).
If you'd rather drive this from Devin itself, a Devin
[Automation](https://docs.devin.ai/product-guides/automations) with a webhook trigger
is the native option for *incoming* events.

## Offline demo / tests

```bash
pip install pytest
python -m pytest streamlit_app/tests -q

# Fake API that accepts any key and finishes after 4 polls:
python streamlit_app/tests/stub_devin_api.py --port 8799
DEVIN_API_KEY=cog_demo DEVIN_API_BASE_URL=http://127.0.0.1:8799/v1 streamlit run streamlit_app/app.py
```
