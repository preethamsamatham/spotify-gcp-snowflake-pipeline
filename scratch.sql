ALTER TASK load_spotify_task RESUME;
ALTER TASK digest_task RESUME;
EXECUTE TASK load_spotify_task;
EXECUTE TASK digest_task;

SELECT * FROM spotify_db.raw.pipeline_state ORDER BY completed_at DESC LIMIT 3;
SELECT * FROM spotify_db.raw.digest_log ORDER BY digest_id DESC LIMIT 2;

SELECT * FROM songs;

SELECT name, state, scheduled_from, query_start_time, error_message
FROM TABLE(information_schema.task_history())
WHERE database_name='SPOTIFY_DB' AND name='LOAD_SPOTIFY_TASK'
ORDER BY scheduled_time DESC LIMIT 3;

SELECT log_id, table_name, run_id, task_run_id
FROM spotify_db.raw.load_log
ORDER BY log_id DESC LIMIT 4;

DESC INTEGRATION spotify_gcs_int;

LIST @spotify_stage;

SELECT DISTINCT
  REGEXP_SUBSTR(metadata$filename, '\\d{8}') AS file_date,
  COUNT(*) AS files
FROM @spotify_stage
GROUP BY 1 ORDER BY 1 DESC;


SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
  'spotify_gcs_int',
  'gcs://spotify-etl-preetham/transformed_data/',
  'validate_test.txt',
  'all'
);

SHOW INTEGRATIONS;

USE ROLE ACCOUNTADMIN;
SELECT CURRENT_ROLE();

LIST @spotify_stage;

COPY INTO songs_staging
FROM @spotify_stage
PATTERN='.*/songs_20260814_070011\.csv'
FILE_FORMAT=spotify_csv_format
FORCE=TRUE;

SELECT *
FROM TABLE(information_schema.copy_history(
  table_name=>'SONGS_STAGING',
  start_time=>DATEADD('hour',-1,CURRENT_TIMESTAMP())
));

SELECT COUNT(*) FROM songs_staging;

SELECT COUNT(*) AS total, COUNT(DISTINCT song_id) AS distinct_ids
FROM songs_staging;

EXECUTE TASK load_spotify_task;

SELECT state, error_message, query_start_time
FROM TABLE(information_schema.task_history(task_name=>'LOAD_SPOTIFY_TASK'))
ORDER BY scheduled_time DESC LIMIT 2;

SELECT * FROM spotify_db.raw.load_log ORDER BY log_id DESC LIMIT 4;

SELECT * FROM spotify_db.raw.pipeline_state ORDER BY completed_at DESC LIMIT 3;

SHOW USERS LIKE 'PINTU';
ALTER USER PINTU SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw8jN/qZ8q1UW3PcPqAzLiDMCFSehGP173O4wnUJqajNqBB/xR9fGJSb/Jx6skHrQSxfCYZeqS+XhpLAqKEuCvL04PuYOiM7qBEMUFnR/+7TzINNappiZ8HrMLpK50bSdXMaD5gnTa1nvnbSsuAC7NeoSObf7bMTTQ5cpyeuknJ61d77EOAkRNacynzkXy0CSiN04JuhadTVAD3F2Wnt1ZfUzOD/qDuPExwJ/gEEPBNk9sieSgLB1Qew3jDWmn+l04Txm33PHZkNaB2bpBKjSBfc6El9yMEgndv/vDUs7G9/b2buZIJIhkJAwbAYE8oqntMv7k5mKnE2Z3abXR7tNawIDAQAB';

DESC USER PINTU;

SELECT * FROM spotify_db.raw.pipeline_state ORDER BY completed_at DESC LIMIT 1;

SELECT table_name, rows_inserted, rows_updated, run_id
FROM spotify_db.raw.load_log
ORDER BY log_id DESC LIMIT 4;

SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME(), CURRENT_ACCOUNT();

SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))