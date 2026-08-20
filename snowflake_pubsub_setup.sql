USE WAREHOUSE spotify_wh;
USE DATABASE SPOTIFY_DB;
USE SCHEMA RAW;

SELECT 'songs' AS tbl, COUNT(*) FROM songs
UNION ALL SELECT 'artist', COUNT(*) FROM artists
UNION ALL SELECT 'song_artists', COUNT(*) FROM song_artists;


DROP PIPE IF EXISTS songs_pipe;
DROP PIPE IF EXISTS albums_pipe;
DROP PIPE IF EXISTS artists_pipe;
DROP PIPE IF EXISTS song_artists_pipe;

DESC NOTIFICATION INTEGRATION spotify_pubsub_int;
SHOW PIPES;

DROP INTEGRATION IF EXISTS spotify_pubsub_int;

CREATE NOTIFICATION INTEGRATION spotify_pubsub_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/python-gcp-snowflake/subscriptions/spotify-snowpipe-sub';

  DESC NOTIFICATION INTEGRATION spotify_pubsub_int;

  CREATE PIPE songs_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'SPOTIFY_PUBSUB_INT'
AS
  COPY INTO songs FROM @spotify_stage
  PATTERN = '.*/songs_.*\.csv' FILE_FORMAT = spotify_csv_format;

  DROP PIPE IF EXISTS songs_pipe;

  CREATE OR REPLACE TASK load_spotify_task
  WAREHOUSE = spotify_wh
  SCHEDULE = '5 MINUTE'
AS
BEGIN
  COPY INTO songs        FROM @spotify_stage PATTERN='.*/songs_.*\.csv'        FILE_FORMAT=spotify_csv_format;
  COPY INTO albums       FROM @spotify_stage PATTERN='.*/albums_.*\.csv'       FILE_FORMAT=spotify_csv_format;
  COPY INTO artists      FROM @spotify_stage PATTERN='.*/artists_.*\.csv'      FILE_FORMAT=spotify_csv_format;
  COPY INTO song_artists FROM @spotify_stage PATTERN='.*/song_artists_.*\.csv' FILE_FORMAT=spotify_csv_format;
END;

ALTER TASK load_spotify_task RESUME;
SHOW TASKS;

SELECT 'songs' AS tbl, COUNT(*) AS "rows" FROM songs
UNION ALL SELECT 'artists', COUNT(*) FROM artists
UNION ALL SELECT 'albums', COUNT(*) FROM albums
UNION ALL SELECT 'song_artists', COUNT(*) FROM song_artists;


SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(information_schema.task_history())
WHERE name = 'LOAD_SPOTIFY_TASK'
ORDER BY scheduled_time DESC
LIMIT 5;

COPY INTO songs
  FROM @spotify_stage
  PATTERN = '.*/songs_.*\.csv'
  FILE_FORMAT = spotify_csv_format
  FORCE = TRUE;

  SELECT COUNT(*) AS total_rows, COUNT(DISTINCT song_id) AS unique_songs FROM songs;

CREATE TRANSIENT TABLE IF NOT EXISTS songs_staging LIKE songs;
CREATE TRANSIENT TABLE IF NOT EXISTS albums_staging LIKE albums;
CREATE TRANSIENT TABLE IF NOT EXISTS artists_staging LIKE artists;
CREATE TRANSIENT TABLE IF NOT EXISTS song_artists_staging LIKE song_artists;



    SELECT COUNT(*) FROM songs_staging;

    COPY INTO songs_staging
  FROM @spotify_stage
  PATTERN = '.*/songs_.*\.csv'
  FILE_FORMAT = spotify_csv_format
  FORCE = TRUE;

  SELECT COUNT(*) FROM songs_staging;

  TRUNCATE TABLE songs;

  MERGE INTO songs AS target
USING (
    SELECT * FROM songs_staging
    QUALIFY ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY added_at DESC) = 1
) AS source
ON target.song_id = source.song_id
WHEN MATCHED THEN UPDATE SET
    target.name = source.name,
    target.duration_ms = source.duration_ms,
    target.explicit = source.explicit,
    target.track_number = source.track_number,
    target.album_id = source.album_id,
    target.added_at = source.added_at
WHEN NOT MATCHED THEN INSERT
    (song_id, name, duration_ms, explicit, track_number, album_id, added_at)
    VALUES
    (source.song_id, source.name, source.duration_ms, source.explicit, source.track_number, source.album_id, source.added_at);

    SELECT COUNT(*) AS total, COUNT(DISTINCT song_id) AS unique_songs FROM songs;

COPY INTO albums_staging FROM @spotify_stage PATTERN='.*/albums_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
COPY INTO artists_staging FROM @spotify_stage PATTERN='.*/artists_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
COPY INTO song_artists_staging FROM @spotify_stage PATTERN='.*/song_artists_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;


TRUNCATE TABLE albums;
TRUNCATE TABLE artists;
TRUNCATE TABLE song_artists;


-- ALBUMS (key: album_id)
MERGE INTO albums AS target
USING (SELECT * FROM albums_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY album_id ORDER BY album_id) = 1) AS source
ON target.album_id = source.album_id
WHEN MATCHED THEN UPDATE SET target.name=source.name, target.release_date=source.release_date, target.total_tracks=source.total_tracks
WHEN NOT MATCHED THEN INSERT (album_id,name,release_date,total_tracks) VALUES (source.album_id,source.name,source.release_date,source.total_tracks);

-- ARTISTS (key: artist_id)
MERGE INTO artists AS target
USING (SELECT * FROM artists_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id) = 1) AS source
ON target.artist_id = source.artist_id
WHEN MATCHED THEN UPDATE SET target.name=source.name
WHEN NOT MATCHED THEN INSERT (artist_id,name) VALUES (source.artist_id,source.name);

-- SONG_ARTISTS (composite key: song_id + artist_id, no columns to update)
MERGE INTO song_artists AS target
USING (SELECT DISTINCT song_id, artist_id FROM song_artists_staging) AS source
ON target.song_id = source.song_id AND target.artist_id = source.artist_id
WHEN NOT MATCHED THEN INSERT (song_id,artist_id) VALUES (source.song_id,source.artist_id);

SELECT 'songs' t, COUNT(*) total, COUNT(DISTINCT song_id) uniq FROM songs
UNION ALL SELECT 'albums', COUNT(*), COUNT(DISTINCT album_id) FROM albums
UNION ALL SELECT 'artists', COUNT(*), COUNT(DISTINCT artist_id) FROM artists;

SELECT COUNT(*) AS total,
       COUNT(DISTINCT song_id || artist_id) AS unique_pairs
FROM song_artists;

CREATE OR REPLACE TASK load_spotify_task
  WAREHOUSE = spotify_wh
  SCHEDULE = '5 MINUTE'
AS
BEGIN
  -- clear staging from last run
  TRUNCATE TABLE songs_staging;
  TRUNCATE TABLE albums_staging;
  TRUNCATE TABLE artists_staging;
  TRUNCATE TABLE song_artists_staging;

  -- load new files into staging
  COPY INTO songs_staging FROM @spotify_stage PATTERN='.*/songs_.*\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO albums_staging FROM @spotify_stage PATTERN='.*/albums_.*\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO artists_staging FROM @spotify_stage PATTERN='.*/artists_.*\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO song_artists_staging FROM @spotify_stage PATTERN='.*/song_artists_.*\.csv' FILE_FORMAT=spotify_csv_format;

CREATE OR REPLACE TASK load_spotify_task
  WAREHOUSE = spotify_wh
  SCHEDULE = '5 MINUTE'
AS
BEGIN
  TRUNCATE TABLE songs_staging;
  TRUNCATE TABLE albums_staging;
  TRUNCATE TABLE artists_staging;
  TRUNCATE TABLE song_artists_staging;
  COPY INTO songs_staging FROM @spotify_stage PATTERN='.*/songs_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO albums_staging FROM @spotify_stage PATTERN='.*/albums_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO artists_staging FROM @spotify_stage PATTERN='.*/artists_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO song_artists_staging FROM @spotify_stage PATTERN='.*/song_artists_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  MERGE INTO songs t USING (SELECT * FROM songs_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY added_at DESC)=1) s ON t.song_id=s.song_id WHEN MATCHED THEN UPDATE SET t.name=s.name,t.duration_ms=s.duration_ms,t.explicit=s.explicit,t.track_number=s.track_number,t.album_id=s.album_id,t.added_at=s.added_at WHEN NOT MATCHED THEN INSERT (song_id,name,duration_ms,explicit,track_number,album_id,added_at) VALUES (s.song_id,s.name,s.duration_ms,s.explicit,s.track_number,s.album_id,s.added_at);
  MERGE INTO albums t USING (SELECT * FROM albums_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY album_id ORDER BY album_id)=1) s ON t.album_id=s.album_id WHEN MATCHED THEN UPDATE SET t.name=s.name,t.release_date=s.release_date,t.total_tracks=s.total_tracks WHEN NOT MATCHED THEN INSERT (album_id,name,release_date,total_tracks) VALUES (s.album_id,s.name,s.release_date,s.total_tracks);
  MERGE INTO artists t USING (SELECT * FROM artists_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id)=1) s ON t.artist_id=s.artist_id WHEN MATCHED THEN UPDATE SET t.name=s.name WHEN NOT MATCHED THEN INSERT (artist_id,name) VALUES (s.artist_id,s.name);
  MERGE INTO song_artists t USING (SELECT DISTINCT song_id,artist_id FROM song_artists_staging) s ON t.song_id=s.song_id AND t.artist_id=s.artist_id WHEN NOT MATCHED THEN INSERT (song_id,artist_id) VALUES (s.song_id,s.artist_id);
END;

ALTER TASK load_spotify_task RESUME;

SHOW TASKS;

SELECT name, state, error_message, completed_time
FROM TABLE(information_schema.task_history())
WHERE name = 'LOAD_SPOTIFY_TASK'
ORDER BY scheduled_time DESC
LIMIT 3;

SELECT 'songs' t, COUNT(*) total, COUNT(DISTINCT song_id) uniq FROM songs
UNION ALL SELECT 'albums', COUNT(*), COUNT(DISTINCT album_id) FROM albums
UNION ALL SELECT 'artists', COUNT(*), COUNT(DISTINCT artist_id) FROM artists;


-- clean finals
TRUNCATE TABLE songs;
TRUNCATE TABLE albums;
TRUNCATE TABLE artists;
TRUNCATE TABLE song_artists;

-- clean + reload staging with FORCE (one-time manual)
TRUNCATE TABLE songs_staging;
TRUNCATE TABLE albums_staging;
TRUNCATE TABLE artists_staging;
TRUNCATE TABLE song_artists_staging;

COPY INTO songs_staging FROM @spotify_stage PATTERN='.*/songs_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
COPY INTO albums_staging FROM @spotify_stage PATTERN='.*/albums_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
COPY INTO artists_staging FROM @spotify_stage PATTERN='.*/artists_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
COPY INTO song_artists_staging FROM @spotify_stage PATTERN='.*/song_artists_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;


SELECT 'songs' t, COUNT(*) total, COUNT(DISTINCT song_id) uniq FROM songs
UNION ALL SELECT 'albums', COUNT(*), COUNT(DISTINCT album_id) FROM albums
UNION ALL SELECT 'artists', COUNT(*), COUNT(DISTINCT artist_id) FROM artists;


SELECT COUNT(*) FROM songs_staging;

COPY INTO songs_staging FROM @spotify_stage PATTERN='.*/songs_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;

-- 1. reload staging with FORCE
TRUNCATE TABLE songs_staging;
COPY INTO songs_staging FROM @spotify_stage PATTERN='.*/songs_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;

-- 2. confirm staging loaded
SELECT COUNT(*) FROM songs_staging;   -- expect ~1836

-- 3. merge into (already-truncated, empty) songs
MERGE INTO songs t USING (SELECT * FROM songs_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY added_at DESC)=1) s
  ON t.song_id=s.song_id
  WHEN MATCHED THEN UPDATE SET t.name=s.name,t.duration_ms=s.duration_ms,t.explicit=s.explicit,t.track_number=s.track_number,t.album_id=s.album_id,t.added_at=s.added_at
  WHEN NOT MATCHED THEN INSERT (song_id,name,duration_ms,explicit,track_number,album_id,added_at) VALUES (s.song_id,s.name,s.duration_ms,s.explicit,s.track_number,s.album_id,s.added_at);

-- 4. confirm songs is clean
SELECT COUNT(*) total, COUNT(DISTINCT song_id) uniq FROM songs;   -- expect 180 / 180


-- clear + reload the three staging tables with FORCE
TRUNCATE TABLE albums_staging;
TRUNCATE TABLE artists_staging;
TRUNCATE TABLE song_artists_staging;

COPY INTO albums_staging FROM @spotify_stage PATTERN='.*/albums_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
COPY INTO artists_staging FROM @spotify_stage PATTERN='.*/artists_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
COPY INTO song_artists_staging FROM @spotify_stage PATTERN='.*/song_artists_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;

-- ensure the three finals are empty (they were truncated earlier, but be safe)
TRUNCATE TABLE albums;
TRUNCATE TABLE artists;
TRUNCATE TABLE song_artists;

-- merge each
MERGE INTO albums t USING (SELECT * FROM albums_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY album_id ORDER BY album_id)=1) s ON t.album_id=s.album_id WHEN MATCHED THEN UPDATE SET t.name=s.name,t.release_date=s.release_date,t.total_tracks=s.total_tracks WHEN NOT MATCHED THEN INSERT (album_id,name,release_date,total_tracks) VALUES (s.album_id,s.name,s.release_date,s.total_tracks);

MERGE INTO artists t USING (SELECT * FROM artists_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id)=1) s ON t.artist_id=s.artist_id WHEN MATCHED THEN UPDATE SET t.name=s.name WHEN NOT MATCHED THEN INSERT (artist_id,name) VALUES (s.artist_id,s.name);

MERGE INTO song_artists t USING (SELECT DISTINCT song_id,artist_id FROM song_artists_staging) s ON t.song_id=s.song_id AND t.artist_id=s.artist_id WHEN NOT MATCHED THEN INSERT (song_id,artist_id) VALUES (s.song_id,s.artist_id);


SELECT 'songs' t, COUNT(*) total, COUNT(DISTINCT song_id) uniq FROM songs
UNION ALL SELECT 'albums', COUNT(*), COUNT(DISTINCT album_id) FROM albums
UNION ALL SELECT 'artists', COUNT(*), COUNT(DISTINCT artist_id) FROM artists
UNION ALL SELECT 'song_artists', COUNT(*), COUNT(DISTINCT song_id||artist_id) FROM song_artists;

EXECUTE TASK load_spotify_task;


