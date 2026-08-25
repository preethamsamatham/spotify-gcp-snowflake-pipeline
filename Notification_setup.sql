SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
 FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())  
 WHERE NAME = 'LOAD_SPOTIFY_TASK'
 ORDER BY SCHEDULED_TIME DESC
 LIMIT 20;

 CREATE TABLE IF NOT EXISTS SPOTIFY_DB.RAW.LOAD_LOG(
    LOG_ID INTEGER AUTOINCREMENT,
    TABLE_NAME STRING,
    ROWS_INSERTED INTEGER,
    ROWS_UPDATED INTEGER,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
 );

 SHOW TABLES LIKE 'LOAD_LOG';

TRUNCATE TABLE songs_staging;
COPY INTO songs_staging FROM @spotify_stage PATTERN='.*/songs_.*\.csv' FILE_FORMAT=spotify_csv_format FORCE=TRUE;
SELECT COUNT(*) FROM songs_staging;


MERGE INTO songs t
USING (SELECT * FROM songs_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY added_at DESC)=1) s
ON t.song_id=s.song_id
WHEN MATCHED THEN UPDATE SET t.name=s.name,t.duration_ms=s.duration_ms,t.explicit=s.explicit,t.track_number=s.track_number,t.album_id=s.album_id,t.added_at=s.added_at
WHEN NOT MATCHED THEN INSERT (song_id,name,duration_ms,explicit,track_number,album_id,added_at) VALUES (s.song_id,s.name,s.duration_ms,s.explicit,s.track_number,s.album_id,s.added_at);

INSERT INTO load_log (table_name, rows_inserted, rows_updated)
SELECT 'songs', "number of rows inserted", "number of rows updated"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


SELECT * FROM load_log;

ALTER TABLE load_log ADD COLUMN run_ts TIMESTAMP_LTZ;
ALTER TASK load_spotify_task SUSPEND;

SELECT * FROM TABLE(information_schema.task_history(task_name=>'LOAD_SPOTIFY_TASK'))
ORDER BY scheduled_time DESC LIMIT 3;

SELECT * FROM load_log ORDER BY log_id DESC LIMIT 8;

SELECT DATE_TRUNC('day', start_time) AS day, SUM(credits_used) AS credits
FROM TABLE(information_schema.warehouse_metering_history(
  DATE_RANGE_START => DATEADD('day', -14, CURRENT_DATE())))
WHERE warehouse_name = 'SPOTIFY_WH'
GROUP BY 1 ORDER BY 1 DESC;

ALTER TABLE spotify_db.raw.load_log ADD COLUMN run_id STRING;
ALTER TASK load_spotify_task SUSPEND;
SHOW TASKS LIKE 'load_spotify_task';




ALTER TASK load_spotify_task RESUME;
EXECUTE TASK load_spotify_task;

SELECT state, error_message, scheduled_from, query_start_time
FROM TABLE(information_schema.task_history(task_name=>'LOAD_SPOTIFY_TASK'))
ORDER BY scheduled_time DESC LIMIT 3;

SELECT * FROM load_log ORDER BY log_id DESC LIMIT 8;

SELECT SUM(rows_inserted) AS new_rows
FROM load_log
WHERE run_id = 'afe5c686-fccb-4ec1-9f94-a7806227d48b';

SELECT state, error_code, error_message, scheduled_from, query_start_time
FROM TABLE(information_schema.task_history(task_name=>'LOAD_SPOTIFY_TASK'))
ORDER BY scheduled_time DESC LIMIT 3;


SELECT * FROM load_log ORDER BY log_id DESC LIMIT 8;

SELECT GET_DDL('TASK','spotify_db.raw.load_spotify_task');


SELECT run_id,
       MIN(loaded_at) AS run_time,
       COUNT(*) AS tables_logged,
       SUM(rows_inserted) AS new_rows,
       SUM(rows_updated) AS refreshed_rows
FROM load_log
GROUP BY run_id
ORDER BY run_time DESC;

ALTER TABLE spotify_db.raw.load_log ADD COLUMN task_run_id STRING;

SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID');

CREATE OR REPLACE TASK load_spotify_task
  WAREHOUSE = spotify_wh
  SCHEDULE = 'USING CRON 0 3 * * * America/Chicago'
AS
BEGIN
  LET run_id STRING := UUID_STRING();
  TRUNCATE TABLE songs_staging;
  TRUNCATE TABLE albums_staging;
  TRUNCATE TABLE artists_staging;
  TRUNCATE TABLE song_artists_staging;
  COPY INTO songs_staging FROM @spotify_stage PATTERN='.*/songs_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO albums_staging FROM @spotify_stage PATTERN='.*/albums_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO artists_staging FROM @spotify_stage PATTERN='.*/artists_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  COPY INTO song_artists_staging FROM @spotify_stage PATTERN='.*/song_artists_.*\\.csv' FILE_FORMAT=spotify_csv_format;
  MERGE INTO songs t USING (SELECT * FROM songs_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY added_at DESC)=1) s ON t.song_id=s.song_id WHEN MATCHED THEN UPDATE SET t.name=s.name,t.duration_ms=s.duration_ms,t.explicit=s.explicit,t.track_number=s.track_number,t.album_id=s.album_id,t.added_at=s.added_at WHEN NOT MATCHED THEN INSERT (song_id,name,duration_ms,explicit,track_number,album_id,added_at) VALUES (s.song_id,s.name,s.duration_ms,s.explicit,s.track_number,s.album_id,s.added_at);
  INSERT INTO load_log (table_name,run_id,task_run_id,rows_inserted,rows_updated) SELECT 'songs',:run_id,LAST_QUERY_ID(),"number of rows inserted","number of rows updated" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  MERGE INTO albums t USING (SELECT * FROM albums_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY album_id ORDER BY album_id)=1) s ON t.album_id=s.album_id WHEN MATCHED THEN UPDATE SET t.name=s.name,t.release_date=s.release_date,t.total_tracks=s.total_tracks WHEN NOT MATCHED THEN INSERT (album_id,name,release_date,total_tracks) VALUES (s.album_id,s.name,s.release_date,s.total_tracks);
  INSERT INTO load_log (table_name,run_id,task_run_id,rows_inserted,rows_updated) SELECT 'albums',:run_id,LAST_QUERY_ID(),"number of rows inserted","number of rows updated" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  MERGE INTO artists t USING (SELECT * FROM artists_staging QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id)=1) s ON t.artist_id=s.artist_id WHEN MATCHED THEN UPDATE SET t.name=s.name WHEN NOT MATCHED THEN INSERT (artist_id,name) VALUES (s.artist_id,s.name);
  INSERT INTO load_log (table_name,run_id,task_run_id,rows_inserted,rows_updated) SELECT 'artists',:run_id,LAST_QUERY_ID(),"number of rows inserted","number of rows updated" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  MERGE INTO song_artists t USING (SELECT DISTINCT song_id,artist_id FROM song_artists_staging) s ON t.song_id=s.song_id AND t.artist_id=s.artist_id WHEN NOT MATCHED THEN INSERT (song_id,artist_id) VALUES (s.song_id,s.artist_id);
  INSERT INTO load_log (table_name,run_id,task_run_id,rows_inserted,rows_updated) SELECT 'song_artists',:run_id,LAST_QUERY_ID(),"number of rows inserted",0 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  INSERT INTO spotify_db.raw.pipeline_state (run_id,completed_at,tables_loaded)
    SELECT :run_id, CURRENT_TIMESTAMP(), COUNT(*) FROM spotify_db.raw.load_log WHERE run_id=:run_id;
END;


CREATE OR REPLACE TABLE spotify_db.raw.digest_log (
  digest_id      NUMBER AUTOINCREMENT,
  fired_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  load_state     STRING,
  load_run_id    STRING,
  tables_logged  NUMBER,
  new_rows       NUMBER,
  refreshed_rows NUMBER,
  subject_line   STRING
);

CREATE OR REPLACE TASK digest_task
  WAREHOUSE = spotify_wh
  SCHEDULE = 'USING CRON 20 3 * * * America/Chicago'
AS
BEGIN
  LET latest_run STRING := (SELECT run_id FROM spotify_db.raw.pipeline_state
                            WHERE completed_at >= DATEADD('hour',-2,CURRENT_TIMESTAMP())
                            ORDER BY completed_at DESC LIMIT 1);

  INSERT INTO spotify_db.raw.digest_log
    (load_state, load_run_id, tables_logged, new_rows, refreshed_rows, subject_line)
  SELECT
    CASE WHEN agg.tables_logged IS NULL THEN 'NO_RUN'
         WHEN agg.tables_logged < 4     THEN 'PARTIAL'
         ELSE 'COMPLETE' END,
    :latest_run,
    agg.tables_logged,
    agg.new_rows,
    agg.refreshed_rows,
    CASE WHEN agg.tables_logged IS NULL
           THEN 'ALERT Spotify pipeline: no load run detected'
         WHEN agg.tables_logged < 4
           THEN 'ALERT Spotify pipeline: partial load (' || agg.tables_logged || '/4 tables)'
         WHEN agg.new_rows > 0
           THEN 'Spotify pipeline: ' || agg.new_rows || ' new rows'
         ELSE 'Spotify pipeline: no changes' END
  FROM (
    SELECT COUNT(*) AS tables_logged,
           SUM(rows_inserted) AS new_rows,
           SUM(rows_updated)  AS refreshed_rows
    FROM spotify_db.raw.load_log
    WHERE run_id = :latest_run
  ) agg;
END;



CREATE OR REPLACE TABLE spotify_db.raw.pipeline_state (
  run_id       STRING,
  completed_at TIMESTAMP_NTZ,
  tables_loaded NUMBER
);

SELECT CURRENT_ROLE();
DESC INTEGRATION spotify_gcs_int;