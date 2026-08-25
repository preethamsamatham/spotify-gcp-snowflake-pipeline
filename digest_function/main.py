"""Spotify pipeline daily digest.
Runs locally (reads snowflake_key.p8, writes digest_preview.html) OR
as a Cloud Function (reads secrets from Secret Manager, sends via SendGrid).
Switch is the RUNTIME env var: unset = local, 'cloud' = deployed.
"""
import os
import re
from cryptography.hazmat.primitives import serialization
from google.cloud import storage
import snowflake.connector

# ---- config ----
ACCOUNT = "GIHJUIA-ZR03463"
USER = "PINTU"
BUCKET = "spotify-etl-preetham"
PROCESSED_PREFIX = "raw_data/processed/"
TRANSFORMED_PREFIX = "transformed_data/"
GCP_PROJECT = "python-gcp-snowflake"
EMAIL_TO = "mukundmk1990@gmail.com"       # your inbox
EMAIL_FROM = "mukundmk1990@gmail.com"     # must be a SendGrid-verified sender (D2)
DATE_RE = re.compile(r"_(\d{8})_")
IS_CLOUD = os.environ.get("RUNTIME") == "cloud"

# ---- secrets ----
def _secret(name):
    """Fetch a secret payload from Secret Manager (cloud only)."""
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    path = f"projects/{GCP_PROJECT}/secrets/{name}/versions/latest"
    return client.access_secret_version(name=path).payload.data

def get_private_key():
    if IS_CLOUD:
        pem = _secret("snowflake_key")            # secret holds the .p8 bytes
    else:
        with open("snowflake_key.p8", "rb") as f:
            pem = f.read()
    pkey = serialization.load_pem_private_key(pem, password=None)
    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

# ---- snowflake ----
def get_latest_run():
    conn = snowflake.connector.connect(
        account=ACCOUNT, user=USER, private_key=get_private_key(),
        warehouse="SPOTIFY_WH", database="SPOTIFY_DB", schema="RAW",
    )
    cur = conn.cursor()
    cur.execute("""SELECT run_id, completed_at, tables_loaded
                   FROM pipeline_state ORDER BY completed_at DESC LIMIT 1""")
    row = cur.fetchone()
    if row is None:
        cur.close(); conn.close()
        return None, []
    run_id, completed_at, tables_loaded = row
    cur.execute("""SELECT table_name, rows_inserted, rows_updated
                   FROM load_log WHERE run_id = %s ORDER BY table_name""", (run_id,))
    tables = cur.fetchall()
    cur.close(); conn.close()
    return (run_id, completed_at, tables_loaded), tables

# ---- gcs ----
def _latest_date(blobs):
    ds = [DATE_RE.search(b.name).group(1) for b in blobs if DATE_RE.search(b.name)]
    return max(ds) if ds else None

def list_processed():
    client = storage.Client(project=GCP_PROJECT)
    out = []
    for b in client.list_blobs(BUCKET, prefix=PROCESSED_PREFIX):
        if b.name.endswith("/"):
            continue
        lines = b.download_as_text().count("\n") + 1
        out.append((b.name.split("/")[-1], b.size, lines))
    return out

def list_transformed_latest():
    client = storage.Client(project=GCP_PROJECT)
    blobs = [b for b in client.list_blobs(BUCKET, prefix=TRANSFORMED_PREFIX)
             if not b.name.endswith("/")]
    latest = _latest_date(blobs)
    out = []
    for b in blobs:
        m = DATE_RE.search(b.name)
        if m and m.group(1) == latest:
            rows = max(b.download_as_text().count("\n") - 1, 0)
            out.append((b.name.split("/")[-1], b.size, rows))
    return sorted(out), latest

# ---- compose ----
def build(run, tables, processed, transformed, latest_date):
    run_id, completed_at, tables_loaded = run
    new_rows = sum(t[1] for t in tables)
    complete = tables_loaded == 4
    if not complete:
        subject = f"⚠ Spotify pipeline: partial load ({tables_loaded}/4 tables)"
        state, color = f"PARTIAL ({tables_loaded}/4)", "#e0a800"
    elif new_rows > 0:
        subject = f"Spotify pipeline: {new_rows} new rows"
        state, color = "COMPLETE", "#1DB954"
    else:
        subject = "Spotify pipeline: no changes"
        state, color = "COMPLETE", "#1DB954"
    tr = "".join(f"<tr><td>{t[0]}</td><td align=right>{t[1]}</td><td align=right>{t[2]}</td></tr>" for t in tables)
    pr = "".join(f"<tr><td>{n}</td><td align=right>{s:,} B</td><td align=right>{c}</td></tr>" for n, s, c in processed)
    xr = "".join(f"<tr><td>{n}</td><td align=right>{s:,} B</td><td align=right>{c}</td></tr>" for n, s, c in transformed)
    html = f"""<html><body style="font-family:Segoe UI,sans-serif;color:#222">
<h2 style="color:{color}">Spotify Pipeline — {state}</h2>
<p>Run <code>{run_id}</code><br>Completed {completed_at} · <b>{new_rows} new rows</b></p>
<h3>Snowflake load ({tables_loaded}/4 tables)</h3>
<table border=1 cellpadding=6 style="border-collapse:collapse">
<tr style="background:#f0f0f0"><th align=left>Table</th><th>Inserted</th><th>Updated</th></tr>{tr}</table>
<h3>Processed JSON — {len(processed)} file(s)</h3>
<table border=1 cellpadding=6 style="border-collapse:collapse">
<tr style="background:#f0f0f0"><th align=left>File</th><th>Size</th><th>Lines</th></tr>{pr}</table>
<h3>Transformed CSV — {len(transformed)} file(s), latest {latest_date}</h3>
<table border=1 cellpadding=6 style="border-collapse:collapse">
<tr style="background:#f0f0f0"><th align=left>File</th><th>Size</th><th>Rows</th></tr>{xr}</table>
</body></html>"""
    return subject, html

# ---- send ----
def send_email(subject, html):
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    msg = Mail(from_email=EMAIL_FROM, to_emails=EMAIL_TO,
               subject=subject, html_content=html)
    key = _secret("sendgrid_api_key").decode()
    SendGridAPIClient(key).send(msg)

# ---- orchestration ----
def run_digest():
    run, tables = get_latest_run()
    if run is None:
        return "Spotify pipeline: no load run detected", "<p>pipeline_state empty.</p>"
    processed = list_processed()
    transformed, latest = list_transformed_latest()
    return build(run, tables, processed, transformed, latest)

# Cloud Function entry point
def digest_pipeline(request):
    subject, html = run_digest()
    send_email(subject, html)
    return (subject, 200)

# Local entry point
if __name__ == "__main__":
    subject, html = run_digest()
    with open("digest_preview.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("SUBJECT:", subject)
    print("Wrote digest_preview.html")