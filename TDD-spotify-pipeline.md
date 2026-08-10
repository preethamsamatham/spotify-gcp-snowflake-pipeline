# Technical Design Document — Spotify → GCP → Snowflake Pipeline

**Author:** Preetham Samatham
**Last updated:** August 10, 2026 (Session 5 — Week 2 COMPLETE)
**Repo:** github.com/preethamsamatham/spotify-gcp-snowflake-pipeline

---

## 1. Project Goal

An end-to-end, fully automated data pipeline that extracts playlist data from the Spotify Web API daily, stores raw data in Google Cloud Storage, transforms it into normalized tables, and auto-loads it into Snowflake via Snowpipe — with zero manual steps after deployment. Built as a portfolio project for data engineer interviews.

## 2. Architecture

**Target state:**
Cloud Scheduler (daily cron) → Cloud Function `extract-spotify` (Python + Spotify API) → GCS `raw_data/to_process/` → Cloud Function `transform` (pandas, GCS-event triggered via Eventarc) → GCS `transformed_data/` → Pub/Sub notification → Snowpipe auto-ingest → Snowflake tables → analytics / dashboard.

**Current state (end of Session 5):**
**Extract stage fully built, deployed, and scheduled — running autonomously.** Cloud Function `extract-spotify` (gen2, python313, us-central1) deployed and ACTIVE, running as compute service account with least-privilege IAM. Cloud Scheduler job `spotify-daily-extract` ENABLED, cron `30 1 * * *` America/Chicago (1:30 AM daily). Manual + forced runs verified (200 status, ~6.3s latency, files written to bucket). First natural scheduled run pending overnight.

## 3. Tech Stack

Python 3.13 local / python313 Cloud Functions gen2 runtime · spotipy 2.26 · pandas · google-cloud-storage · google-cloud-secret-manager · functions-framework · GCP (Cloud Functions gen2/Cloud Run, Cloud Storage, Cloud Scheduler, Pub/Sub, Eventarc, Secret Manager, Cloud Build) · Snowflake on GCP (trial, Week 4) · Git/GitHub · VS Code · Windows 11.

## 4. Key Design Decisions (with rationale)

| Decision | Choice | Why |
|---|---|---|
| Cloud provider | GCP (not AWS like most tutorials) | Multi-cloud differentiation; free credit; Snowflake supports GCP |
| Region | us-central1 | Must match Snowflake trial region for Snowpipe; full service availability |
| Auth flow | Authorization Code (OAuth) with refresh token | Feb 2026 API changes removed Client Credentials for playlist metadata; refresh token enables unattended runs |
| Cloud auth pattern | Manual `refresh_access_token()` → `Spotify(auth=...)`, NOT `auth_manager=` | Cloud env has no browser and ephemeral filesystem; can't rely on `.cache`. Proven by deleting `.cache` locally and confirming auth still works |
| Raw file format | NDJSON, built in-memory (`"\n".join`) and `upload_from_string` | Snowflake-native; no local temp file needed in cloud |
| Extract philosophy | Save full raw items (wrapper incl. `added_at`) | Faithful capture; re-transform anytime |
| Data model | 4 normalized tables incl. bridge table | Preserves all artists per song; denormalize later in views if needed |
| Secrets | Secret Manager, one per credential, functions read `versions/latest` | No .env/.cache in cloud; encrypted, IAM-controlled, audited; rotation = new version, zero code change |
| IAM | Least privilege: per-secret `secretAccessor`, bucket-scoped `objectAdmin`, `run.invoker` for scheduler SA | Blast radius contained if credentials leak; core cloud security principle |
| Function name vs entry point | GCP resource `extract-spotify` (hyphens, free choice) vs `--entry-point=extract_spotify` (underscores, must match `def` in code) | Two different naming systems; entry point is a lookup key into main.py |
| Extract trigger type | HTTP + Cloud Scheduler (POST) | Time is the trigger ("every morning") — nothing in the system changed, the clock ticked |
| Transform trigger type (planned) | Cloud Storage event (Eventarc) | Event is the trigger ("a file appeared") — reacts, not scheduled |
| Data source | Own public playlist (180 Hindi songs, 2009–2013 Bollywood) | Editorial playlists blocked for dev-mode apps; owned = controlled source |
| Deploy hygiene (planned) | `.gcloudignore` to exclude extract.py, raw_data/, *.md | Lean deployment bundle — only main.py + requirements.txt ship |

## 5. Data Model (SCHEMA.md)

```
SONGS         song_id (PK), name, duration_ms, explicit, track_number, album_id (FK), added_at
ARTISTS       artist_id (PK), name
ALBUMS        album_id (PK), name, release_date, total_tracks
SONG_ARTISTS  song_id + artist_id (composite PK) — bridge for many-to-many
```

Notes: `type` dropped from ARTISTS (constant, zero info). Album artists ≠ track artists; relationships modeled at song level. `popularity` unavailable (Feb 2026 dev-mode) — analytics pivot to release-date eras, durations, artist frequency, added_at timeline.

## 6. Code Structure

- **main.py** — deployed artifact (production). Functions: `get_secret()`, `get_spotify_client()` (cacheless cloud auth), `get_all_tracks()` (pagination + null guard), `extract_spotify(request)` (entry point: in-memory NDJSON → GCS upload → receipt). Module constant `PLAYLIST_ID`.
- **extract.py** — local dev tool. Used for token refresh when the 180-day refresh token expires (re-auth in browser → new `.cache` → load new secret version) and for local experiments. NOT deployed. (Future: rename to `local_auth.py` for clarity.)

## 7. Session Log

**Session 1 (Aug 4) — Environment + Spotify app.** Dev app, VS Code + Python 3.13 + Git, venv, .gitignore-before-commit, GitHub repo, requirements.txt encoding fix.

**Session 2 (Aug 4–5) — Extraction + schema.** extract.py via iterative review; Client Credentials → SpotifyOAuth migration; pagination 180/180; null guard; NDJSON; run receipt. Five-round schema review → 4-table model.

**Session 3 (Aug 5) — GCP foundation.** gcloud verified, project `python-gcp-snowflake`, billing, 8 APIs, bucket `spotify-etl-preetham` (us-central1), manual upload, first secret container.

**Session 4 (Aug 5–6, late) — Secrets + credential incident.** Refresh token exposed in screenshot → full rotation. BQ/AQ mix-up → corrected via v2 + destroy v1. All 3 secrets loaded. Cloud Function build sheet received.

**Session 5 (Aug 10) — main.py built, deployed, scheduled.** Wrote `get_secret()` (verified reading vault). Built cacheless `get_spotify_client()` — proven cloud-safe by the delete-`.cache`-and-rerun test. Added `get_all_tracks`, entry point with in-memory NDJSON → GCS upload. Local test via functions-framework (localhost:8080) → 2 files written. IAM: per-secret secretAccessor ×3, bucket objectAdmin, run.invoker for scheduler. Deployed gen2 (ACTIVE). `gcloud functions call` → 200, file in bucket. Cloud Scheduler job created (`30 1 * * *`, Central). Learned: forced runs (`jobs run`), Scheduler logs vs Cloud Logging, request logs vs application logs, userAgent identifies trigger source, HTTP methods, trigger types.

## 8. Debugging Log (interview war stories)

1. **PowerShell `>` encoding** — UTF-16 broke requirements.txt. Fix: `Out-File -Encoding utf8`.
2. **`src refspec main`** — pushed before first commit exists.
3. **Spotify Feb 2026 migration** — 401 on Client Credentials; migrated to Auth Code; adapted to `/items` + `item` key renames by inspecting responses.
4. **Liked Songs ≠ playlist** — needs user-scoped auth; `37i9dQZF1...` blocked.
5. **Windows filename colons** — `datetime.now()` default → OSError. Fix: `strftime`.
6. **File write inside loop** — wrote list 180×; output looked correct. "One action or N?"
7. **Inverted boolean guard** — `is not None: continue` → "Saved 0", caught by run receipt.
8. **Credential exposure + rotation** — revoke, re-auth, v2, destroy v1. Vault makes rotation free; minimal scope contained blast radius.
9. **BQ/AQ token mix-up** — loaded access token instead of refresh; caught by prefix.
10. **False-pass local test (cache)** — new auth code "worked" locally only because `.cache` still existed; the manual-exchange code was dead-code after an early `return`. Caught by ordering review + the delete-`.cache` test. Lesson: a green checkmark can pass for a reason that won't exist in production; test by removing the dependency you're not supposed to need.
11. **Library not installed vs import typo** — `cannot import name 'secretmanager'` was a missing package (`google-cloud-secret-manager`), not a typo. Lesson: pip name (hyphens) ≠ import name (dots); requirements.txt must list every import or the cloud build fails identically.

## 9. Needs Work / Learning Gaps (honest self-assessment)

- **Relational modeling** — clicked via diagram; redo 4-table sketch from memory ~Aug 16; practice a second domain.
- **Boolean logic care** — read guards aloud vs intent.
- **Review checklist completion** — tick every prior finding before resubmitting (recurred through the main.py build: dead code after return, wrong pattern kept).
- **Sequencing bugs** — used variable before defining it; code after `return`. Watch for define-before-use and single-exit.
- **Precision under fatigue** — schedule precision work early; verify via receipts.
- **Understanding-before-running** — improving: this session asked "what/why" before running (cron, HTTP methods, triggers, --source, --entry-point) rather than after. Keep front-loading.
- **Ahead:** pandas (Week 3 transform), Cloud Storage event triggers / Eventarc, SQL DDL + Snowflake objects (Week 4).

## 10. Open Items / Next Steps

1. **Confirm first natural scheduled run** (~1:30 AM) — check bucket for ~01:30 file + Cloud Logging entry with `Google-Cloud-Scheduler` userAgent.
2. Commit main.py + updated requirements + TDD to repo.
3. Optional polish: add `.gcloudignore`; consider renaming extract.py → local_auth.py; clean up test files in bucket.
4. **Week 3 — transform function:** pandas reads raw NDJSON from `to_process/`, builds 4 tables per schema (incl. `drop_duplicates(subset=[...])` for the composite-key bridge), writes CSVs to `transformed_data/`, moves raw file to `processed/`. Triggered by GCS finalize event (Eventarc) on the `to_process/` prefix — beware re-trigger loops (prefix filter).
5. **Week 4 — Snowflake:** start trial (us-central1!), storage integration (GCS), file format, external stages, Pub/Sub notification integration, Snowpipe auto-ingest with `AUTO_INGEST=TRUE`.
6. **Week 5 — analytics + polish:** queries (eras, durations, artist frequency, added_at timeline), README + architecture diagram, monitoring/alert on function failure, cost notes.

## 11. Confirmed Constraints

- Spotify Premium required for dev-mode app (owner has ✓). Dev-mode: 5-user cap, reduced endpoints, no popularity field.
- Snowflake trial: 30 days — start ~Week 4.
- Refresh token: 180-day lifetime (rotated Aug 6; ~Feb 2027 expiry — re-auth via extract.py locally + add new secret version).
- Secret Manager: `spotify-refresh-token` (v2 enabled, v1 destroyed), `spotify-client-id` (v1), `spotify-client-secret` (v1). Read `versions/latest`.
- Deployed function: `extract-spotify` gen2, us-central1, run.app URL + cloudfunctions.net URL, `--no-allow-unauthenticated`, 60s timeout, 256M.
- Scheduler: `spotify-daily-extract`, `30 1 * * *`, America/Chicago, POST, OIDC as compute SA.
