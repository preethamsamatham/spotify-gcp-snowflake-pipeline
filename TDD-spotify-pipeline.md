# Technical Design Document — Spotify → GCP → Snowflake Pipeline

**Author:** Preetham Samatham
**Last updated:** August 20, 2026 (Session 8 — Week 4 COMPLETE: Snowflake load stage automated, pipeline runs end to end)
**Repo:** github.com/preethamsamatham/spotify-gcp-snowflake-pipeline

---

## 1. Project Goal

An end-to-end, fully automated data pipeline that extracts playlist data from the Spotify Web API daily, stores raw data in Google Cloud Storage, transforms it into normalized tables, and auto-loads it into Snowflake via Snowpipe — with zero manual steps after deployment. Built as a portfolio project for data engineer / data scientist interviews.

## 2. Architecture

**Target state:**
Cloud Scheduler (daily cron) → Cloud Function `extract-spotify` → GCS `raw_data/to_process/` → Cloud Function `transform-spotify` (pandas, GCS-event via Eventarc) → GCS `transformed_data/` → Pub/Sub notification → Snowpipe auto-ingest → Snowflake tables → analytics.

**Current state (end of Session 6):**
**Two-stage pipeline running autonomously.** Extract (scheduled 2 AM) drops a raw file → the finalize event triggers Transform → four normalized CSVs land in `transformed_data/`, raw file archived to `processed/`. Verified end-to-end via a scheduler force-run: extract wrote `spotify_raw_20260811_073156.json`, transform auto-fired and produced the four `_20260811_073156.csv` tables — no manual steps between stages.

## 3. Tech Stack

Python 3.13 local / python313 gen2 runtime · spotipy 2.26 · pandas · google-cloud-storage · google-cloud-secret-manager · functions-framework · GCP (Cloud Functions gen2/Cloud Run, Cloud Storage, Cloud Scheduler, Pub/Sub, **Eventarc**, Secret Manager, Cloud Build) · Snowflake on GCP (trial, Week 4) · Git/GitHub · VS Code · Windows 11.

## 4. Key Design Decisions (with rationale)

| Decision | Choice | Why |
|---|---|---|
| Cloud provider | GCP (not AWS) | Multi-cloud differentiation; free credit; Snowflake supports GCP |
| Region | us-central1 | Must match Snowflake trial region for Snowpipe |
| Auth flow | Authorization Code + refresh token | Feb 2026 API changes removed Client Credentials for playlist metadata |
| Cloud auth pattern | Manual `refresh_access_token()` → `Spotify(auth=…)` | No browser/`.cache` in cloud; proven via delete-`.cache` test |
| Raw format | NDJSON, in-memory upload | Snowflake-native; no local temp file |
| Extract philosophy | Save full raw items incl. `added_at` | Faithful capture; re-transform anytime |
| Data model | 4 normalized tables incl. bridge | Preserves all artists per song |
| Secrets | Secret Manager, read `versions/latest` | Rotation = new version, zero code change |
| IAM | Least privilege per resource | Blast radius contained if credentials leak |
| **Extract trigger** | **HTTP + Cloud Scheduler (POST)** | **Time is the trigger — the clock ticked** |
| **Transform trigger** | **Cloud Storage finalize event (Eventarc)** | **Event is the trigger — reacts to a file landing; scales to N files as N invocations** |
| **Transform I/O** | **DataFrame → CSV string in memory → GCS; no local disk** | **Cloud filesystem is ephemeral; mirror of the extract's in-memory NDJSON pattern** |
| **Dedup strategy** | **`drop_duplicates(subset=[PK])` per table; bridge uses composite `[song_id, artist_id]`** | **Artists/albums repeat across tracks; PK must be unique. Bridge pair is unique, not either column alone** |
| **Output naming** | **Timestamped CSVs matching the raw file's stamp** | **Lineage: any CSV traces to its source pull; Snowpipe sees each as a new load** |
| **File-move** | **Copy-then-delete to `processed/` after transform** | **GCS has no atomic move; keeps `to_process/` a true work queue; recoverable state** |
| **Loop prevention** | **`startswith("raw_data/to_process/")` guard in the event handler** | **Function writes to the same bucket it watches; own outputs re-fire the trigger. Guard ignores them (verified: 5 self-events rejected per run)** |
| Deploy layout | Each function in its own folder with `main.py` + `requirements.txt` | Can't have two `main.py` in one folder; clean separation of deps |
| Data source | Own public playlist (180 Hindi songs) | Editorial playlists blocked for dev-mode |

## 5. Data Model (SCHEMA.md) — verified counts from 180-song playlist

```
SONGS         song_id (PK), name, duration_ms, explicit, track_number, album_id (FK), added_at   →  180 rows
ARTISTS       artist_id (PK), name                                                                →  188 rows (deduped)
ALBUMS        album_id (PK), name, release_date, total_tracks                                     →  128 rows (deduped)
SONG_ARTISTS  song_id + artist_id (composite PK) — bridge for many-to-many                        →  435 rows
```

Counts tell a story: 128 albums < 180 songs (shared soundtracks); 435 song-artist links ≈ 2.4 artists/song (Bollywood credits singers + composers). `type` dropped from ARTISTS (constant). `popularity` unavailable (Feb 2026 dev-mode) — analytics pivot to eras, durations, artist frequency, added_at timeline.

## 6. Code Structure

- **main.py** (root) — extract function (deployed). Vault auth, pagination, NDJSON → GCS.
- **extract.py** — local dev tool for token refresh + experiments. Not deployed.
- **transform.py** — local dev/working file for the transform logic.
- **transform_function/main.py** — transform function (deployed). Event entry point `transform_trigger` + all builders/helpers + `transform_gcs` orchestrator.
- **transform_function/requirements.txt** — functions-framework, google-cloud-storage, pandas (unpinned).
- **.gcloudignore**, **requirements.txt** (extract), **SCHEMA.md**, **TDD** (md + html).

Transform functions: `load_raw_from_gcs`, `build_songs`, `build_albums`, `build_artists`, `build_song_artists`, `write_csv_to_gcs`, `move_blob`, `transform_gcs` (orchestrator), `transform_trigger` (event entry point). `transform_gcs` has two callers — the event trigger (production) and `__main__` (local test) — same worker, two ignitions.

## 7. Session Log

**Session 1 (Aug 4)** — Environment + Spotify app.
**Session 2 (Aug 4–5)** — Extraction script + 5-round schema review → 4-table model.
**Session 3 (Aug 5)** — GCP foundation: project, billing, 8 APIs, bucket, first manual upload, secret container.
**Session 4 (Aug 5–6)** — Secrets loaded; credential exposure + rotation; BQ/AQ fix.
**Session 5 (Aug 10)** — Extract Cloud Function built, deployed (gen2), scheduled; first autonomous run confirmed.
**Session 6 (Aug 11) — Transform stage built, deployed, event-triggered, chained to extract.**
- Mapped all 4 tables field-by-field from the JSON (worked through paths manually).
- Built builders one at a time with counts: songs 180, albums 128 (dedup proven <180), artists 188, song_artists 435.
- Learned the dedup subtlety: single-column PK for songs/albums/artists, composite `[song_id, artist_id]` for the bridge.
- Adapted local transform to GCS: `load_raw_from_gcs` (download_as_text + splitlines), `write_csv_to_gcs` (`to_csv(index=False)` → string → upload_from_string), `move_blob` (copy_blob + delete).
- Timestamped output naming for lineage (parse stamp from raw filename).
- Packaged into `transform_function/` with event entry point `transform_trigger` (@cloud_event) + loop-prevention guard.
- Deployed with Cloud Storage finalize trigger; hit Eventarc IAM errors → granted GCS service agent `pubsub.publisher` + compute SA `eventarc.eventReceiver` → deploy ACTIVE.
- Triggered by dropping a file into `to_process/`: transform auto-fired, produced 4 CSVs, moved raw file, and rejected 5 self-generated events via the guard (all in the logs).
- Force-ran the scheduler → confirmed full **extract→transform chain** with zero manual steps between stages.

## 8. Doubts Asked & Answered (running reference)

**Session 5:** get_secret line-by-line (ADC, resource path, bytes→decode) · `versions/latest` vs `2` (transparent rotation) · get_spotify_client uses refresh token to get access token · `--source=.` uploads all but excluded files · `--entry-point` must match a def · HTTP methods (POST = has side effects) · trigger types (pull vs push) · cron 5 fields + specials · logs (scheduler "did it trigger?" vs function "what happened?") · console vs CLI · timezone picker searches by country · LF/CRLF warning.

**Session 6:**
- **Why move_blob at all?** GCS has no move; keeps `to_process/` a true work queue, prevents reprocessing, makes state recoverable.
- **What does `bucket.copy_blob(src, bucket, dest_blob)` do?** Built-in server-side copy — args are (source blob, destination bucket, new name); data never round-trips through your machine. Then `src.delete()` completes the "move."
- **Why `to_csv(index=False)` with no filename?** No filename → returns CSV as a string (in memory) instead of writing a disk file; suits the ephemeral cloud filesystem. `index=False` drops pandas' auto 0,1,2 index column.
- **Do the CSVs get date-stamped?** Yes — parsed from the raw filename so outputs share the raw file's timestamp (lineage). Fixed names would overwrite and Snowpipe might not re-ingest.
- **Should transform code go in main.py?** No — extract's `main.py` is separate. Transform gets its own folder; at deploy the working `transform.py` is copied to `transform_function/main.py`.
- **Are we calling `transform_gcs` twice?** No — one definition, two callers (event trigger for prod, `__main__` for local test). Cloud never runs `__main__`.
- **If I delete the `__main__` block does it still work?** Yes (cloud ignores it) — but keep it for local testing. Note: don't confuse the `__main__` block with the *file* main.py, which must keep that name.
- **Which file does the transform process when there are many?** The event names it — `cloud_event.data["name"]` is the triggering file. N files = N independent invocations. The function is woken *by* a file, never chooses among them.

## 9. Debugging War Stories (interview-ready)

1. PowerShell `>` → UTF-16 broke requirements.txt. Fix: `Out-File -Encoding utf8`.
2. `src refspec main` — pushed before first commit.
3. Spotify Feb 2026 migration — 401 → Auth Code flow; `/items` + `item` key renames.
4. Liked Songs ≠ playlist — user-scoped auth; `37i9dQZF1…` blocked.
5. Windows filename colons — `strftime` fix.
6. File write inside loop — wrote list 180×; "one action or N?"
7. Inverted boolean guard — "Saved 0", caught by receipt.
8. Credential exposure + rotation — revoke, re-auth, v2, destroy v1; vault makes rotation free.
9. BQ/AQ token mix-up — access vs refresh; verify by prefix.
10. False-pass local test (cache) — code "worked" only because `.cache` existed; dead-code after early return. Delete-cache test caught it.
11. Library not installed vs typo — `google-cloud-secret-manager` missing; pip name ≠ import name.
12. Commit failed on missing file — `git add` of absent file aborts the add; run `git status` first.
13. Scheduler booked next-day slot — created past today's cron time; use Force run to test.
14. **Guessed version pins** — `pandas==3.0.5` doesn't exist; would fail the cloud build. Unpin unless you've verified the version.
15. **Eventarc trigger IAM** — deploy failed on `storage.buckets.get` denied. Fix: GCS service agent needs `roles/pubsub.publisher`; trigger SA needs `roles/eventarc.eventReceiver`. The deploy validates these upfront and fails fast. Classic first-time-Eventarc rite of passage.
16. **Storage-trigger infinite loop (prevented)** — function writes to the bucket it watches, so its 4 CSV writes + 1 archive move re-fire the trigger (5 self-events/run). The `startswith("raw_data/to_process/")` guard rejects them — verified in logs.

## 10. Needs Work / Learning Gaps

- **Relational modeling** — clicked via diagram; redo 4-table sketch from memory ~Aug 16; practice a second domain. (Reinforced this session by building the tables — going well.)
- **Boolean logic care** — read guards aloud vs intent.
- **Review checklist completion** — tick prior findings before resubmitting.
- **Sequencing bugs** — define-before-use, single-exit.
- **File pasting glitch** — pasted files came through empty this session; worked around by reading from disk. Not a skill gap, just a tooling note.
- **Understanding-before-running** — strong this session; kept asking what/why (move_blob, copy_blob, to_csv, multi-file events) before deploying.
- **Ahead:** SQL DDL + Snowflake objects (Week 4), Snowpipe, storage/notification integrations.

## 11. Open Items / Next Steps

1. Commit Session 6 work: `transform_function/`, `transform.py`, `.gcloudignore`, TDD (md+html). Run `git status` first.
2. Optional polish: delete dead `load_raw` + unused `import os` + duplicate import in `transform_function/main.py`; clean old test CSVs/files from bucket.
3. **Week 4 — Snowflake:** start trial (us-central1!). Create DB/schema + 4 tables (DDL). Storage integration (GCS) + grant Snowflake SA `Storage Object Viewer`. File format (CSV, skip header) + external stages on `transformed_data/`. Pub/Sub notification integration + Snowpipe pipes with `AUTO_INGEST=TRUE`. Test full chain to Snowflake.
4. **Week 5 — analytics + polish:** queries (eras, durations, artist frequency, added_at timeline), README + architecture diagram, monitoring/alert on function failure, cost notes, `.gitattributes`, embed screenshots in README, add `.gcloudignore` to transform folder.

---

# WEEK 4 — Snowflake Load Stage (Sessions 7–8, Aug 19–20)

## W4.1 What Week 4 accomplished

Transformed CSVs in `gcs://spotify-etl-preetham/transformed_data/` now auto-load into Snowflake, deduplicated to one current row per key, via a scheduled task using a staging + MERGE pattern. This closes the pipeline end to end:

```
Spotify API → extract (scheduled 2 AM) → GCS raw → transform (event-triggered) → GCS CSVs
  → Snowflake external stage → scheduled TASK (COPY into staging → MERGE into final)
  → clean, queryable, deduplicated warehouse tables
```

The extract→transform chain ran **autonomously for 10 straight days** (Aug 10–19), each day dropping four timestamped CSVs — all loadable, all collapsed into clean tables by MERGE.

## W4.2 Final verified table state (one row per key)

| Table | Rows | Unique keys | Key |
|---|---|---|---|
| songs | 180 | 180 | song_id |
| albums | 131 | 131 | album_id |
| artists | 193 | 193 | artist_id |
| song_artists | 445 | 445 | song_id + artist_id (composite) |

Counts grew vs Week 3 (128 albums / 188 artists) because the playlist genuinely gained songs over the 10 autonomous days — real change captured over time.

## W4.3 Snowflake objects created (all in `snowflake_setup.sql`)

- **Warehouse** `spotify_wh` — XSMALL, AUTO_SUSPEND=60, AUTO_RESUME=TRUE (separation of compute from storage; auto-suspend controls cost).
- **Database** `spotify_db`, **schema** `raw`.
- **Four final tables** — typed columns (STRING/INTEGER/BOOLEAN/TIMESTAMP_NTZ). `release_date` kept STRING (Spotify returns partial/year-only dates that break DATE parsing). Composite PK on song_artists.
- **Four staging tables** `*_staging` — `CREATE TRANSIENT TABLE ... LIKE <final>` (transient = no time-travel/fail-safe → cheaper; correct for throwaway landing data).
- **Storage integration** `spotify_gcs_int` — EXTERNAL_STAGE, GCS, scoped to `transformed_data/` prefix (least privilege). SA `k16l50000@gcpuscentral1-1dfa...` granted `roles/storage.objectViewer` on bucket.
- **File format** `spotify_csv_format` — CSV, SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY='"' (handles commas in quoted song names).
- **External stage** `spotify_stage` — points at `transformed_data/` via the storage integration + file format. `LIST @spotify_stage` proved Snowflake can read the bucket.
- **Notification integration** `spotify_pubsub_int` — QUEUE / GCP_PUBSUB (built for the abandoned Snowpipe path). SA `k26l50000@gcpuscentral1-1dfa...`.
- **Task** `load_spotify_task` — SCHEDULE='5 MINUTE', runs the staging+MERGE block.

## W4.4 The load pattern (staging + MERGE) — the core engineering

Problem: extract pulls the full playlist daily → each CSV is a near-duplicate snapshot. Loading directly stacked ~20× duplicates (measured: **3,672 rows for 180 unique songs**).

Solution — the professional upsert pattern:
```
GCS CSVs → COPY into STAGING (raw, all snapshots)
         → MERGE into FINAL (dedup to newest-per-key, upsert)
         → staging truncated next run
```

Per-table MERGE:
- `USING (SELECT * FROM <staging> QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY added_at DESC)=1)` — window-function dedup keeps only the newest snapshot per key.
- `ON target.<key> = source.<key>` — match condition.
- `WHEN MATCHED THEN UPDATE` — existing key → refresh to latest. `WHEN NOT MATCHED THEN INSERT` — new key → add.

Bridge (song_artists) MERGE differs: composite match `ON t.song_id=s.song_id AND t.artist_id=s.artist_id`, `SELECT DISTINCT` (no "latest" concept), only `WHEN NOT MATCHED` (the two columns are the whole row).

Proven: MERGE collapsed 1,836 staging rows → 180 clean song rows. Re-running the task on a clean target leaves counts unchanged = idempotent.

## W4.5 The recurring task — critical design point

`load_spotify_task` (5-minute schedule) does, in a `BEGIN…END` block: truncate staging → COPY new files into staging → four MERGEs into finals. Created suspended; activated with `ALTER TASK … RESUME`.

**The task contains NO `FORCE=TRUE`.** It relies on COPY's load history to skip already-loaded files, so each run ingests only new daily CSVs. `FORCE=TRUE` was used only for one-time manual backfills; in the recurring task it would reload everything every 5 minutes and duplicate endlessly.

## W4.6 Debugging Log — Week 4 (interview war stories 17–24)

17. **Wrong cloud at signup.** First Snowflake trial defaulted to **AWS_US_EAST_2**, not GCP — caught via `SELECT CURRENT_REGION()` and the storage-integration SA reading `awsuseast2`. Recreated on **GCP_US_CENTRAL1** (matching the bucket) using a `+alias` email to bypass the one-trial-per-email limit. Lesson: verify `CURRENT_REGION()` right after signup; same cloud/region avoids cross-cloud egress and simplifies notifications.
18. **Snowsight runs only the statement under the cursor.** Repeated "object does not exist" errors traced to Snowsight running a single statement, not the worksheet. Habit: Ctrl+A before Run; cursor-in-statement to run one.
19. **Regex PATTERN substring bug (recurred from Week 3, in SQL).** `PATTERN='.*artists_.*\.csv'` also matched `song_artists_…csv` → artists loaded 1261 rows. Fix: anchor with a path separator `.*/artists_.*\.csv`.
20. **Snowpipe Pub/Sub bind failure — the big one (unresolved; pivoted).** Built the full path (topic, bucket notification, subscription, notification integration). `CREATE PIPE … AUTO_INGEST=TRUE` repeatedly failed: *Could not monitor … PERMISSION_DENIED*. Verified the SA had `pubsub.subscriber` (subscription + project + topic) AND `pubsub.viewer` (project) — all readable via `get-iam-policy`; confirmed integration's subscription name + SA matched; confirmed subscription attached to the right topic; waited out propagation (incl. 10 min); recreated the integration (SA unchanged); confirmed personal Gmail (no org policy). All correct, bind still failed — **root cause never identified.** Pivoted to a scheduled COPY task. *Framing:* "Built Snowpipe end to end, hit a persistent cross-cloud IAM bind error, pivoted to a scheduled task — same outcome; I can discuss event-driven vs batch."
21. **Task SUCCEEDED but tables re-duplicated.** Counts ballooned (songs 2016/180). Cause: **finals still held dupes from earlier manual `FORCE=TRUE` loads** — MERGE dedups the *source* but can't dedup rows already in the *target*. Fix: reset to clean, then MERGE maintains cleanliness. Lesson: **MERGE keeps a target clean only if it started clean.**
22. **COPY load-history "0 files" confusion.** Re-running COPY returned 0 rows — because COPY remembers loaded files and skips them (idempotency by design). Diagnosed with `FORCE=TRUE` as a one-time tool; also revealed the 10 days of autonomous runs.
23. **`BEGIN…END` task parse error.** `CREATE OR REPLACE TASK … END;` threw `unexpected 'END'`. Fixed by selecting/running the whole block as one unit and collapsing each MERGE to a single line.
24. **Snowflake reuses the same SA per account.** Recreating the notification integration did NOT mint a new service account (stayed `k26l…`) — so existing grants remained valid. (Ruled out the "wrong SA after recreate" theory.)

## W4.7 Concepts Learned / Doubts Answered — Week 4

- **Why a warehouse vs object storage?** GCS stores files cheaply but has no query engine (no joins/aggregations/types). A warehouse loads data into a columnar, typed, indexed format for fast SQL + BI tools. Pattern: GCS = lake, Snowflake = warehouse, the task = the bridge.
- **Warehouse vs transactional DB?** OLAP (analytics; Snowflake/BigQuery/Redshift/Synapse) vs OLTP (app row-reads; Postgres/Cloud SQL/RDS/Azure SQL). This project = analytics → warehouse.
- **Native warehouse per cloud:** GCP→BigQuery, AWS→Redshift/Athena, Azure→Synapse. Snowflake is cloud-agnostic → portable, in-demand.
- **Snowflake architecture headline:** separation of storage and compute; virtual warehouses scale independently against shared data; pay for each separately; no contention. Three layers: storage, compute, cloud services.
- **Does Snowflake enforce PRIMARY KEY?** No — PK/FK are optimizer/documentation metadata; NOT enforced (only NOT NULL is). Dedup is handled in the load (MERGE).
- **Storage integration:** credential-less cross-cloud file access — Snowflake issues an SA identity, you grant it least-privilege read on a bucket prefix. No keys exchanged.
- **Notification integration:** the events counterpart — Snowflake's credential-less connection to a cloud event queue (GCP Pub/Sub here).
- **Internal vs external stage:** external → cloud storage you own (this project); internal → Snowflake-managed.
- **Snowpipe vs scheduled COPY task:** Snowpipe = continuous, event-driven (notification triggers load); task = polling/batch (COPY on a timer). This project uses the task.
- **MERGE / upsert:** insert-or-update keyed on a match; combined with `QUALIFY ROW_NUMBER()` to pick latest-per-key.
- **TRANSIENT tables:** no fail-safe/limited time-travel → cheaper; correct for temporary staging.
- **Pub/Sub topic vs subscription:** topic = broadcast channel (publishers send); subscription = listener (consumers read). Decouples producer (GCS) from consumer (Snowflake) — why event systems scale.
- **COPY idempotency:** COPY tracks loaded files (load history, 64-day retention) and skips them; `FORCE=TRUE` overrides — one-time backfills only.

### Deep-dive doubts (Session 8)

- **D1 — Other GCS event types besides `OBJECT_FINALIZE`?** `OBJECT_DELETE` (deleted), `OBJECT_METADATA_UPDATE` (metadata changed, not content), `OBJECT_ARCHIVE` (archived/version superseded). Filtered to FINALIZE only so deletes/metadata don't wake the loader for nothing.
- **D2 — Other notification providers/types Snowflake listens to?** Providers: `GCP_PUBSUB`, `AWS_SQS`/`AWS_SNS`, `AZURE_STORAGE_QUEUE`/Event Grid (Snowflake is cloud-agnostic). Directions/flavors: inbound auto-ingest (`QUEUE`, `INBOUND` — listen for file events), outbound error notifications (Snowpipe push errors to a queue), and email notifications (e.g., task-failure alerts).
- **D3 — What if `AUTO_INGEST = FALSE`?** The pipe does NOT listen for events; it loads only when manually triggered via Snowflake's REST API (`insertFiles`) — used when *your app* controls loading (upload, then tell Snowflake to load specific files) rather than reacting to cloud events.
- **D4 — What does `FORCE` do? (`FALSE` = default).** COPY remembers every file it has loaded (load history). `FORCE=FALSE` (default) = skip already-loaded files → re-running loads nothing (0 rows) — correct for a recurring job (only new files). `FORCE=TRUE` = ignore history, reload regardless — a one-time tool. Load history retained 64 days. The MERGE design is immune to reloads anyway (upsert, no new dup row).
- **D5 — What is a "one-time backfill"?** Loading pre-existing *historical* data once to catch a table up to the present, vs ongoing loading of new data. Here: 10 days of CSVs already sat in the bucket → seed the tables once (with FORCE), then the task handles only new daily files (no FORCE).
- **D6 — Why `QUALIFY ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY added_at DESC)=1`? Alternatives?** It keeps one row per key (the newest): partition by key, order newest-first, number rows, keep #1. `QUALIFY` filters on a window function (WHERE can't — it runs before the window computes). Alternatives: (1) subquery wrapping ROW_NUMBER + outer `WHERE rn=1` (portable, no QUALIFY); (2) `GROUP BY`+aggregate — only if you want per-column maxes, not the whole newest row; (3) `RANK()`/`DENSE_RANK()` — risky, ties share a rank → duplicates; ROW_NUMBER guarantees exactly one; (4) `DISTINCT` — only collapses *fully identical* rows (works for the bridge, not songs). Chose ROW_NUMBER for the one-row-on-ties guarantee, QUALIFY for readability.

## W4.8 GCP resources created this week (reproducibility)

- Pub/Sub topic `spotify-snowpipe-topic`; bucket notification (OBJECT_FINALIZE) → topic; subscription `spotify-snowpipe-sub`. (Built for Snowpipe; retained but unused after the pivot.)
- IAM: `k16l50000@…` → `roles/storage.objectViewer` on bucket; `k26l50000@…` → `roles/pubsub.subscriber` (subscription+project+topic) + `roles/pubsub.viewer` (project).

## W4.9 Interview headline (load stage)

"The load stage lands daily CSV snapshots in Snowflake and keeps one current row per key. I load into transient staging tables, then MERGE into the finals — the MERGE uses a window function to pick the latest snapshot per key, then upserts. A scheduled task runs it every five minutes and relies on COPY's load-history so it only ingests new files. I originally built Snowpipe event-driven ingestion via Pub/Sub but hit a persistent cross-cloud IAM bind error, so I pivoted to a scheduled COPY task — same result, and I can discuss continuous vs batch trade-offs."

---

## 12. Confirmed Constraints

- Spotify Premium required for dev-mode app (owner has ✓). Dev-mode: 5-user cap, reduced endpoints, no popularity field.
- GCP free trial: $300 credit, ~$0 used, expires Oct 22, 2026.
- Snowflake trial: **on GCP / us-central1** (first attempt was on AWS by mistake, recreated). 30 days, $400 credits ~untouched (tiny XSMALL usage). Account user shows `PINTU`/`NITHINPATEL` on the recreated account.
- Refresh token: 180-day lifetime (rotated Aug 6; ~Feb 2027 expiry).
- Secret Manager: refresh-token (v2, v1 destroyed), client-id (v1), client-secret (v1). Read `versions/latest`.
- Deployed functions: `extract-spotify` (gen2, HTTP, scheduled 2 AM) · `transform-spotify` (gen2, Cloud Storage finalize trigger on bucket `spotify-etl-preetham`).
- Snowflake: warehouse `spotify_wh`, db `spotify_db`, schema `raw`, 4 final + 4 staging tables, storage integration `spotify_gcs_int`, stage `spotify_stage`, file format `spotify_csv_format`, task `load_spotify_task` (5-min). Notification integration `spotify_pubsub_int` (unused after Snowpipe pivot).
- IAM (Week 3): GCS service agent → `roles/pubsub.publisher`; compute SA → `roles/eventarc.eventReceiver`. IAM (Week 4): `k16l50000@…` → `storage.objectViewer` on bucket; `k26l50000@…` → `pubsub.subscriber`+`pubsub.viewer` (Snowpipe attempt).
- **Pipeline status: COMPLETE end to end.** Spotify → extract (scheduled) → GCS → transform (event) → GCS → Snowflake (scheduled task, staging + MERGE) → clean deduped tables. Week 5 (analytics + README/diagram polish) remaining.
