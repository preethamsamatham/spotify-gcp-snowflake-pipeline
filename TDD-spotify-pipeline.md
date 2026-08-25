# Technical Design Document — Spotify → GCP → Snowflake Pipeline

**Author:** Preetham Samatham
**Last updated:** August 25, 2026 (Session 9 — Week 5: pipeline instrumented + **daily digest email SHIPPED end-to-end** via headless key-pair auth, Secret Manager, a Cloud Function, and SendGrid; survived and recovered from a 5-day GCS outage)
**Repo:** github.com/preethamsamatham/spotify-gcp-snowflake-pipeline

---

## 1. Project Goal

An end-to-end, fully automated data pipeline that extracts playlist data from the Spotify Web API daily, stores raw data in Google Cloud Storage, transforms it into normalized tables, and auto-loads it into Snowflake — with zero manual steps after deployment, plus instrumentation and a daily email digest that reports what each run did. Built as a portfolio project for data engineer / data scientist interviews.

> **Note on ingestion:** the original goal named Snowpipe auto-ingest. That path was built end-to-end but hit a persistent cross-cloud IAM bind error (W4 story #20) and was replaced with a scheduled COPY **task** — the shipping design. See §2 and W5 for the current architecture.

## 2. Architecture

**Shipping state (as of Week 5):**
Cloud Scheduler (daily cron, 2 AM) → Cloud Function `extract-spotify` → GCS `raw_data/to_process/` → Cloud Function `transform-spotify` (pandas, GCS-event via Eventarc) → GCS `transformed_data/` → Snowflake external stage → **scheduled COPY task** (`load_spotify_task`, daily CRON 3 AM: COPY into staging → MERGE into finals → write `load_log` + `pipeline_state`) → clean deduped Snowflake tables → **independent scheduled digest task** (`digest_task`, daily 3:20 AM: reads `pipeline_state` + `load_log`, composes a status line) → **digest Cloud Function `spotify-digest` → SendGrid email to inbox, daily 3:20 AM Central (SHIPPED, W5.9)**.

```
Scheduler → extract → GCS raw → transform → GCS csv
   → external stage → load_spotify_task (COPY→MERGE→instrument→handoff)
   → clean tables
   → digest_task (3:20 schedule) → digest Cloud Function → SendGrid email (SHIPPED)
```

**Ingestion note:** Snowpipe auto-ingest (Pub/Sub notification integration `spotify_pubsub_int`) was built but abandoned after a persistent `PERMISSION_DENIED` bind error (W4 story #20). The scheduled task is the shipping design. Not "target vs current" — this *is* the architecture.

**Monitoring rationale (why the digest is a separate task, not a chained child):** a task chained with `AFTER load_spotify_task` only fires when the parent **succeeds** — so on the one morning the pipeline breaks, no email is sent, which is exactly when you need it. An independent scheduled `digest_task` fires regardless of the load's outcome and reports SUCCEEDED / FAILED / PARTIAL / NO_RUN. This decision was validated in practice: see W5.4 (the Aug 19–24 outage).

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
| **Load cadence** | **Daily CRON 3 AM (was `5 MINUTE`)** | **Source arrives once daily at 2 AM; 5-min polling = 288 runs/day, 287 no-ops. Measured 8.4 credits/day for ~13s of real work/run (60s min-billing + 60s auto-suspend = 87% waste). Matched schedule to data arrival → ~280× credit cut (W5.3)** |
| **MERGE-count capture** | **`INSERT … SELECT … FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))` immediately after each MERGE, into `load_log`** | **MERGE row-counts are ephemeral; `RESULT_SCAN` grabs them before the next statement discards them. `SQLROWCOUNT` collapses inserts+updates into one number — useless for a "N new songs" digest** |
| **Run identity** | **`run_id` (UUID, one per run, groups the 4 rows) + `task_run_id` (inline `LAST_QUERY_ID()`, per-MERGE query id)** | **Don't infer identity from ordering. `run_id` answers "which rows are one run?"; `task_run_id` links each row to `query_history` for per-statement lineage** |
| **Digest trigger** | **Independent scheduled task (3:20 AM), NOT `AFTER` chaining** | **A chained child fires only on parent success → no alert on the day it breaks. Independent schedule reports SUCCEEDED/FAILED/PARTIAL/NO_RUN regardless (W5.2)** |
| **Load→digest handoff** | **Parent writes `(run_id, completed_at, tables_loaded)` to `pipeline_state` as its last statement; digest reads that one row** | **Explicit key beats "latest row by timestamp," which breaks under concurrent/manual runs. Same principle as `run_id` over timestamp-proximity (W5.1)** |
| **Digest signal** | **Always send; signal in the subject line** (`N new rows` / `no changes` / `⚠ FAILED` / `⚠ partial`) | **Heartbeat (silence = broken) without alert fatigue (subject triages from the notification). Beats send-only-on-change, where silence is ambiguous** |

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

1. Commit Week 5 work: updated `snowflake_setup.sql` (load_log, pipeline_state, digest_log DDL + the instrumented `load_spotify_task` + `digest_task`), the new `digest_function/` (main.py + requirements.txt), TDD (md+html). Run `git status` first. **Never commit `snowflake_key.p8`** (in `.gitignore`); delete `test_read.csv`, `sg_key.txt` if present.
2. ~~**Digest Phase 3** — Cloud Function composing the email body.~~ **DONE (W5.9).** Deployed as `spotify-digest`, reads Snowflake + GCS, composes HTML.
3. ~~**Digest Phase 4** — SendGrid wiring.~~ **DONE (W5.9).** Secrets in Secret Manager; scheduled at 3:20 AM Central via authenticated Cloud Scheduler. Delivered with SPF/DKIM pass.
4. **Rename `task_run_id` → `merge_query_id`** — inline `LAST_QUERY_ID()` made it a per-statement query id, not a task-run id; the name now misleads. `ALTER TABLE … RENAME COLUMN` on next task rebuild.
5. **Drop dead `run_ts` column** from `load_log` — artifact of an earlier attempt, nothing writes to it.
6. **Trim `completed_at` microseconds** in the email (`2026-08-24 09:33:24.510000` → `09:33:24`) — cosmetic.
7. **Transform dedup gap (real DQ finding, W5.5)** — `build_songs` emits duplicate `song_id`s (187 rows / 183 keys; the email shows 189 vs 185). MERGE collapses it downstream, but the CSV claims `song_id` is a PK and lies. Decide: fix in transform vs. document the split. Add a null/blank check for the 3 empty-name rows.
8. **Headless Snowflake auth** — ✅ SOLVED via key-pair (W5.9). Reusable for any future scheduled GCP trigger (Cloud Scheduler + Cloud Run) for the load path too.
9. **Week 5+ analytics** — queries (eras, durations, artist frequency, added_at timeline), README + architecture diagram, embed screenshots. Optional BI layer (Looker) over the analytics views.
10. **ML recommender** — content/collaborative similarity on the `song_artists` bridge; learn ML concepts while building (DIY cosine-similarity over the managed Vertex box, since the goal is understanding).

> **⏳ Binding constraint:** the Snowflake trial credits die *before* the calendar clock. At the old 8.4 credits/day burn, ~22 days of runway from ~9.5 credits used → out around **Sep 12**, ahead of the ~Sep 18 calendar expiry. The daily-CRON fix (W5.3) removes the burn problem, but capture screenshots / query outputs / row counts **while the warehouse is alive** — a dead trial is an unrunnable portfolio project, and it also gates the ML phase.

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

"The load stage lands daily CSV snapshots in Snowflake and keeps one current row per key. I load into transient staging tables, then MERGE into the finals — the MERGE uses a window function to pick the latest snapshot per key, then upserts. A scheduled task runs it and relies on COPY's load-history so it only ingests new files. I originally built Snowpipe event-driven ingestion via Pub/Sub but hit a persistent cross-cloud IAM bind error, so I pivoted to a scheduled COPY task — same result, and I can discuss continuous vs batch trade-offs."

---

# WEEK 5 — Instrumentation, Cost, & Monitoring (Session 9, Aug 20–24)

## W5.1 What Week 5 accomplished

Turned a working-but-blind pipeline into an **observable** one. The load task now records what every run did, hands its identity to a monitoring task, and a daily digest reports status by email — and the whole thing survived a real multi-day GCS outage and recovered on its own, which the monitoring caught.

```
load_spotify_task (COPY→MERGE) → load_log (per-table insert/update counts, run_id, task_run_id)
                               → pipeline_state (run_id, completed_at, tables_loaded)   ← handoff
digest_task (independent 3:20 schedule) → reads pipeline_state + load_log → digest_log (status + subject line) → [Phase 3–4: email]
```

## W5.2 New Snowflake objects

- **`load_log`** — one row per (table, run). Columns: `log_id` (IDENTITY), `table_name`, `rows_inserted`, `rows_updated`, `loaded_at` (DEFAULT CURRENT_TIMESTAMP), `run_id` (UUID — groups the 4 rows of a run), `task_run_id` (per-MERGE `LAST_QUERY_ID()` — links to `query_history`). Written by an `INSERT … SELECT … FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))` immediately after each MERGE.
- **`pipeline_state`** — the load→digest handoff. One row per completed run: `run_id`, `completed_at`, `tables_loaded`. Written as the **last** statement of `load_spotify_task`. Its *existence* is the "I finished" signal — if the block aborts mid-flight, no row is written, and the digest correctly reports the run as failed/absent.
- **`digest_log`** — what the digest *would* email (Phase-1 staging before SendGrid). Columns: `digest_id`, `fired_at`, `load_state` (COMPLETE/PARTIAL/NO_RUN), `load_run_id`, `tables_logged`, `new_rows`, `refreshed_rows`, `subject_line`.
- **`digest_task`** — independent scheduled task (CRON 3:20 AM), no `AFTER`. Reads the latest `pipeline_state` row, rolls up its `load_log` rows, classifies the outcome, composes a subject line.

## W5.3 The cost story (FinOps — the strongest interview beat this week)

Checked `warehouse_metering_history` after automation went live and found the load burning **8.4 credits/day** for ~13 seconds of real work per run:

| Day | Credits | Note |
|---|---|---|
| Aug 11 | 0.077 | manual worksheet poking |
| Aug 13 | 0.033 | manual |
| Aug 19 | 5.28 | 5-min task went live (~9 AM) |
| Aug 20 | 4.12 | partial day → ~8.4/day run-rate |

Root cause: at `5 MINUTE` cadence = **288 runs/day**, each billing a **60-second minimum** on resume + **60s `AUTO_SUSPEND`** idle after ~13s of work → **87% waste**, and 287 of 288 runs are no-ops (source arrives once daily at 2 AM).

Fix: `ALTER TASK … SET SCHEDULE = 'USING CRON 0 3 * * * America/Chicago'` → 1 run/day. **~280× reduction.**

| Schedule | Runs/day | Credits/day | Runway (~190 credits) |
|---|---|---|---|
| 5 MINUTE | 288 | 8.4 | ~22 days |
| 1 HOUR | 24 | 0.70 | ~271 days |
| Daily CRON | 1 | 0.03 | effectively unlimited |

*Interview framing:* "I polled every five minutes during development for fast feedback, then measured actual consumption and matched the schedule to the data-arrival rate — cutting credits ~280×. Polling frequency should follow the data, not developer impatience." Note: `EXECUTE TASK` still runs it on demand, so daily scheduling costs nothing during development.

## W5.4 The Aug 19–24 GCS outage — diagnosed by elimination, self-recovered

The pipeline began **failing silently on Aug 19** (first noticed via a FAILED scheduled run). Error on every `COPY`: `Failed to access remote file: access denied. Please check your credentials` — while `LIST @spotify_stage` **succeeded**. So Snowflake could list the bucket but not read object contents.

Diagnosed by ruling everything out, in order:
- **Snowflake config** — `DESC INTEGRATION` correct (right SA, right prefix, ENABLED); integration exists (`SHOW INTEGRATIONS`). ✓ not the cause.
- **Bucket IAM** — `get-iam-policy` showed `k16l50000@…` bound to `roles/storage.objectViewer`, no condition, no deny. That role *does* include `storage.objects.get`. ✓ not the cause.
- **Encryption (CMEK)** — top theory (LIST reads metadata, COPY must decrypt; Object Viewer doesn't grant KMS decrypt). `buckets describe … encryption` returned **null** — no CMEK. ✗ ruled out.
- **Billing** — $300/$300 credit, 60 days left, not suspended. ✗ ruled out.
- **Object readable at all?** — `gcloud storage cp` of the exact file **as myself** succeeded (17.8 KiB). So the object is fine; only the Snowflake SA's read path failed.
- **Audit logs** — empty (GCS data-access logging off by default), so no recorded denial to inspect.

Every layer checked out, yet COPY denied. The break coincided **exactly** with creating `spotify_pubsub_int` (the Snowpipe attempt) on Aug 19 09:27 — concurrent Pub/Sub/IAM changes. **It then healed on its own:** `pipeline_state` timestamps show the **Aug 23 and Aug 24 01:00 scheduled runs both succeeded** (`tables_loaded=4`) with no fix from me — a transient cross-cloud access failure that cleared after propagation settled. Confirmed with a manual `EXECUTE TASK` on Aug 24 → SUCCEEDED, 185 songs (up from 183), four fresh `load_log` rows, `pipeline_state` written, digest read COMPLETE.

*Interview framing:* "A storage-integration COPY started failing with access-denied while LIST worked. I diagnosed by elimination — proved it wasn't the integration config, bucket IAM, encryption, or billing, and that the object was readable by me directly — narrowing it to a transient cross-cloud access failure during concurrent IAM changes. It self-recovered after propagation, which my `pipeline_state` handoff timestamps confirmed. The lesson I keep: a pipeline with no monitoring fails in the dark — had the digest been live, I'd have had an alert the first morning."

## W5.5 Data-quality findings (surfaced from real data)

- **Dedup gap in transform.** A single `songs` CSV had **187 rows / 183 distinct `song_id`s** — four tracks appear twice (identical except `added_at`), because re-adding a track to a Spotify playlist creates a second entry. `build_songs` isn't deduping on `song_id`. The MERGE's `QUALIFY ROW_NUMBER()` collapses it downstream (finals stay clean), but the CSV presents `song_id` as a PK while containing duplicates. Decision pending (fix in transform vs. document the split); either way the digest should report *rows-in-file vs distinct-keys vs inserted/updated* so the gap is visible, not mysterious.
- **Null-name rows.** 3 rows carry empty `name` + `duration_ms=0` — unavailable-in-market or local-file tracks the API returned null-ish. They've flowed to Snowflake for weeks unnoticed. A null/blank check belongs in the digest.
- **Why the digest's file-count and insert-count will never match.** By design: a CSV is a full daily snapshot (≈187 rows) while "new rows inserted" is ≈0–3. Reporting both without explanation looks broken every morning → the digest reports rows-in-file / distinct-keys / inserted / updated per table so the difference is self-explaining.

## W5.6 Debugging Log — Week 5 (interview war stories 25–29)

25. **NOT-MATCHED-only MERGE has no `rows updated` column.** The `song_artists` capture failed with `invalid identifier '"number of rows updated"'`. Snowflake's MERGE result set only contains columns for the clauses actually present — the bridge MERGE has only `WHEN NOT MATCHED`, so no "updated" column exists. Fix: hardcode `0` for `rows_updated` (an insert-only merge can never update by definition). *Loud failure — named the line and column; ~20 min.*
26. **`CREATE OR REPLACE TASK` always lands suspended.** Several "it ran but nothing happened" cycles traced to the task being suspended after every replace. Sequence must be REPLACE → `RESUME` → `EXECUTE TASK`. Verify with `GET_DDL` (the object changed) *and* `SHOW TASKS` (state), not the absence of an error.
27. **Ctrl+A worksheet contamination.** Ctrl+A selects the *whole* worksheet, not the block in view — smuggled a stray `SELECT GET_DDL(...)` into the task body (and left a `:run_id` with no block to bind to). Habit: task definitions live in their own worksheet; verification queries in a scratch worksheet.
28. **Silent `LET … := (SELECT …)` binding failure — the nastiest.** `LET task_run_id STRING := (SELECT SYSTEM$…)` and even `:= (SELECT LAST_QUERY_ID())` returned **null** with no error — task reported SUCCEEDED, column silently null. Isolated by controlled comparison (same function inline → populated; via `LET` → null). Fix: inline `LAST_QUERY_ID()` directly into each INSERT. *Loud failures cost an hour; silent failures ship wrong data.* Caught only because the column was explicitly checked. **General lesson repeated all session: verify the state, don't trust the silence.**
29. **`SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')` returns null under `EXECUTE TASK`.** A key that only populates on the scheduled run (not manual test) is a bad key — can't be verified during development. Chose `LAST_QUERY_ID()` (always available, identical under cron and manual) over the semantically "purer" graph id.

## W5.7 Concepts learned — Week 5

- **`SQLROWCOUNT` vs `RESULT_SCAN` after MERGE.** `SQLROWCOUNT` returns one number (inserts+updates combined) for the last DML — useless when the digest needs "3 new songs" specifically. `RESULT_SCAN(LAST_QUERY_ID())` re-reads the MERGE's full result set (separate insert/update columns) before the next statement discards it.
- **MERGE result columns depend on clauses present.** Only clauses you write produce columns — a NOT-MATCHED-only MERGE has no "rows updated" column (story #25).
- **Snowflake's 60s minimum billing + AUTO_SUSPEND economics.** Every warehouse resume bills ≥60s; AUTO_SUSPEND then holds it idle. A 13s job on a 5-min cadence is ~87% waste (W5.3).
- **Task scheduling: `USING CRON` vs interval.** `'5 MINUTE'` measures from the previous run's *end* (drifts); `USING CRON` fires at wall-clock times (no drift) and **requires** a timezone (omit it → UTC → load fires before the 2 AM extract). Child tasks (`AFTER`) have no schedule and fire on predecessor success only.
- **`AUTOINCREMENT`/IDENTITY guarantees uniqueness, not contiguity.** `log_id` jumped 1→101→201; surrogate keys skip ranges. Never compute a count by subtracting IDs.
- **`RESULT_SCAN`/`LAST_QUERY_ID()` are session-scoped.** Meaningful only immediately after a successful statement in the *same* session; run standalone in a worksheet they read whatever ran last (or fail if that failed).
- **Diagnosis by elimination for cross-cloud auth.** LIST-works-COPY-fails localizes to object-read; `gcloud storage cp` as yourself proves the object is readable; `get-iam-policy` shows the *effective* binding; `buckets describe … encryption` rules out CMEK (W5.4).

## W5.8 Interview headline (monitoring)

"Once the pipeline was loading cleanly I made it observable: each MERGE's row-counts are captured into a log table via `RESULT_SCAN`, grouped by a per-run UUID, and the load task hands its run identity to an independent daily digest task that emails a status line — SUCCEEDED, FAILED, PARTIAL, or no-run. I made the digest a separate scheduled task rather than a chained child specifically so it still alerts on the morning the load fails. I also caught an 8.4-credits/day burn from over-polling and cut it ~280× by matching the schedule to the once-daily data arrival. Then a real cross-cloud access outage hit — I diagnosed it by elimination and confirmed its self-recovery from the handoff timestamps."

## W5.9 Digest email — Phases 3 & 4 (SHIPPED end-to-end)

The digest is now a live email, not just a `digest_log` row. Full delivery path, running in the cloud:

```
Cloud Scheduler (3:20 AM Central)
  → HTTP + OIDC token → Cloud Function `spotify-digest` (gen2, python312, us-central1)
     → reads secrets from Secret Manager (snowflake_key, sendgrid_api_key)
     → connects to Snowflake HEADLESS (key-pair auth, no browser/Duo)
     → queries pipeline_state + load_log (latest run)
     → lists GCS raw_data/processed/ (JSON line counts) + transformed_data/ latest-date CSVs (row counts)
     → composes HTML + subject line
     → SendGrid → inbox
```

**Proven end-to-end Aug 25:** `gcloud functions call` returned `Spotify pipeline: no changes`; email delivered to inbox with **SPF pass + DKIM pass** (SendGrid authenticated), green "COMPLETE" body showing run `9ef00652`, 4/4 tables, the per-table breakdown, and the transformed CSVs (189 file rows vs 185 table keys — the W5.5 dedup gap, visible in the email by design).

### Headless auth (the gating dependency for the whole phase)
A Cloud Function has no browser, so `authenticator=externalbrowser` (the VS Code method) can't work at 3:20 AM. Solved with **RSA key-pair auth**:
- `openssl genrsa 2048 | openssl pkcs8 -topk8 … -out snowflake_key.p8 -nocrypt` (private, PKCS#8 — the only format Snowflake accepts) + `openssl rsa -pubout` (public).
- `ALTER USER PINTU SET RSA_PUBLIC_KEY='<body>'` registers the public half; `DESC USER` shows `RSA_PUBLIC_KEY_FP` + `HAS_KEYPAIR=true`.
- The function signs a JWT with the private key; Snowflake verifies against the public key. **No shared secret** — Snowflake never sees the private key. MFA/Duo is bypassed because key-pair is a different authenticator (PINTU has `HAS_MFA=false` anyway).
- Proven with a local `SELECT CURRENT_USER()` test **before** any function code — returned `PINTU`, no browser popped.

### Secret handling
- `snowflake_key.p8` and the SendGrid API key live in **Secret Manager** (`gcloud secrets create`), never in code or the repo. `snowflake_key.p8` is in `.gitignore` (`*.p8`).
- The function's runtime SA (`429687740825-compute@…`, the same one `transform-spotify` uses) was granted `roles/secretmanager.secretAccessor` on **only those two secrets** (least privilege). It already had `storage.objectAdmin` on the bucket.
- Scheduler → function auth uses an **OIDC token**; the SA also needs `roles/run.invoker` on the underlying Cloud Run service (gen2 functions run on Cloud Run).

### Local/cloud dual-mode
One `main.py` runs both ways, switched by the `RUNTIME` env var: unset → reads the local `.p8`, writes `digest_preview.html` (no send); `RUNTIME=cloud` (set at deploy) → reads Secret Manager, sends via SendGrid. This let the read/compose logic be proven on the laptop before a single cloud deploy — same discipline as verifying every layer before building on it.

### Account identifier gotcha
The Snowflake account has three identifiers for one account: `GIHJUIA-ZR03463` (Snowsight display / the one that authenticates in the connector), `PAPZGUL-NT81381` (a stale identifier from early notes — does NOT work for JWT), `PW76915` (locator, what `CURRENT_ACCOUNT()` returns). The connector uses `GIHJUIA-ZR03463`. Rule: use the identifier that's *proven* to connect, not the one that looks canonical.

## W5.10 Debugging Log — Phase 3/4 (war stories 30–33)

30. **Hand-transcribing an RSA key dropped 6 characters.** The public key, joined by hand, lost its trailing `IDAQAB` (the RSA exponent block) → `ALTER USER` rejected it as `Invalid Public key`. Fix: never hand-transcribe — pipe it: `grep -v "PUBLIC KEY" snowflake_key.pub | tr -d '\n'`. The machine produces the exact 392-char body; a human loses characters at line boundaries. *This is the canonical form in Snowflake's own docs, for exactly this reason.*
31. **`JWT token is invalid` = account identifier, not the key.** First headless attempt failed the JWT because the wrong account identifier was passed (connection reached Snowflake fine — pure signature/issuer rejection). Fixed by using `GIHJUIA-ZR03463` (the one that authenticates). *A reached-but-rejected auth is an identifier problem, not a network or key problem.*
32. **Secret upload with `printf` failed silently in PowerShell.** `printf` is a Unix command; PowerShell threw `not recognized`, so `sendgrid_api_key` was never stored — and the key had been pasted into the shell/chat. Fix: use a temp file (`gcloud secrets create … --data-file=sg_key.txt`, then `Remove-Item`) so the key never rides a shell command; and **regenerate any key that touched a chat/history** — treat it as compromised. *Wrong-shell command that fails is safer than one that half-succeeds; the real lesson is credential hygiene.*
33. **gen2 Cloud Function needs `run.invoker` for OIDC.** Scheduler → authenticated function returned 403 until the scheduler's SA got `roles/run.invoker` on the Cloud Run service (gen2 functions ARE Cloud Run under the hood). *`--no-allow-unauthenticated` means every caller, including your own scheduler, must be explicitly granted invoke.*

## W5.11 Concepts learned — Phase 3/4

- **Asymmetric auth (key-pair).** Private key signs, public key verifies; the two are mathematically linked but the private can't be derived from the public. Beats a password because there's no shared secret — Snowflake stores only the public half.
- **PKCS#8 vs PKCS#1.** Snowflake requires the PKCS#8 wrapping (`-topk8`); raw `genrsa` output (PKCS#1) is rejected. Same key, different envelope.
- **Secret Manager as the runtime vault.** The Cloud Function can't reach the laptop's disk, so secrets it needs at 3:20 AM must live somewhere it can — Secret Manager, pulled at runtime, encrypted at rest, access-controlled per-secret per-SA.
- **gen2 Cloud Functions run on Cloud Run.** Explains the `run.invoker` requirement and why the service shows up under both `gcloud functions` and `gcloud run`.
- **OIDC service-to-service auth.** Scheduler proves identity to an authenticated function with a signed OIDC token whose audience must equal the target URL — no API keys passed around.
- **Least-privilege service accounts.** The digest SA can read exactly two secrets and send mail; a leak can't touch anything else. Same for the SendGrid key (Mail Send scope only).

## W5.12 Interview headline (the whole platform)

"I built an end-to-end serverless data platform: Spotify → Cloud Scheduler + Cloud Functions for ingest/transform → GCS as the lake → Snowflake for storage, with idempotent staging+MERGE loading. Then I made it *observable and self-reporting* — each load's row-counts are captured to a log table, a handoff table hands the run identity to a daily Cloud Function that connects to Snowflake headlessly with key-pair auth, reads the run and the GCS artifacts, and emails an HTML digest via SendGrid, scheduled with authenticated OIDC. Secrets live in Secret Manager, the service account has least-privilege access, and I proved every layer locally before deploying. Along the way I cut warehouse credits ~280× by right-sizing the schedule and diagnosed a multi-day cross-cloud access outage by elimination."

---

## 12. Confirmed Constraints

- Spotify Premium required for dev-mode app (owner has ✓). Dev-mode: 5-user cap, reduced endpoints, no popularity field.
- GCP free trial: $300 credit, ~$0 used, expires Oct 22, 2026.
- Snowflake trial: **on GCP / us-central1** (first attempt was on AWS by mistake, recreated). 30 days, ~200 credits. **~9.5 used**; the 5-min task briefly burned 8.4/day before the daily-CRON fix (W5.3). **Credits expire before the calendar** — out ~Sep 12 vs ~Sep 18 clock; capture outputs while the warehouse is alive. Account user shows `PINTU` on the recreated account.
- Refresh token: 180-day lifetime (rotated Aug 6; ~Feb 2027 expiry).
- Secret Manager: refresh-token (v2, v1 destroyed), client-id (v1), client-secret (v1). Read `versions/latest`.
- Deployed functions: `extract-spotify` (gen2, HTTP, scheduled 2 AM) · `transform-spotify` (gen2, Cloud Storage finalize trigger on bucket `spotify-etl-preetham`).
- Snowflake: warehouse `spotify_wh`, db `spotify_db`, schema `raw`, 4 final + 4 staging tables, storage integration `spotify_gcs_int`, stage `spotify_stage`, file format `spotify_csv_format`, task `load_spotify_task` (**daily CRON 3 AM**), monitoring task `digest_task` (**daily CRON 3:20 AM**, independent). Log/state tables: `load_log`, `pipeline_state`, `digest_log`. User `PINTU` now has **key-pair auth** (`HAS_KEYPAIR=true`, FP `SHA256:MPO5…`). Account identifiers: `GIHJUIA-ZR03463` (connector), `PW76915` (locator). Notification integration `spotify_pubsub_int` unused (created Aug 19 09:27 — coincides with the W5.4 outage onset).
- **GCP digest stack (W5.9):** Cloud Function `spotify-digest` (gen2, python312, us-central1, entry `digest_pipeline`, `RUNTIME=cloud`, 512Mi/120s) · Cloud Scheduler job `spotify-digest-daily` (`20 3 * * *` America/Chicago, HTTP+OIDC) · Secret Manager secrets `snowflake_key`, `sendgrid_api_key` (+ existing `spotify-client-id`/`-secret`/`-refresh-token`) · SendGrid verified sender `mukundmk1990@gmail.com`, Mail-Send-only API key. Runtime SA `429687740825-compute@…` granted `secretmanager.secretAccessor` (2 secrets) + `run.invoker` + existing `storage.objectAdmin`.
- IAM (Week 3): GCS service agent → `roles/pubsub.publisher`; compute SA → `roles/eventarc.eventReceiver`. IAM (Week 4): `k16l50000@…` → `storage.objectViewer` on bucket (verified effective via `get-iam-policy`, W5.4); `k26l50000@…` → `pubsub.subscriber`+`pubsub.viewer` (Snowpipe attempt).
- **Pipeline status: COMPLETE + INSTRUMENTED + SELF-REPORTING end to end.** Spotify → extract (scheduled) → GCS → transform (event) → GCS → Snowflake (daily task: staging + MERGE + `load_log`/`pipeline_state`) → clean deduped tables → daily digest task → **daily HTML email via Cloud Function + SendGrid (SPF/DKIM authenticated), scheduled 3:20 AM Central.** Survived and self-recovered from a 5-day GCS access outage (W5.4). Remaining: analytics layer, ML recommender.
