SELECT CURRENT_REGION();

-- warehouse
CREATE WAREHOUSE IF NOT EXISTS spotify_wh
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- database + schema
CREATE DATABASE IF NOT EXISTS spotify_db;
CREATE SCHEMA IF NOT EXISTS spotify_db.raw;
USE DATABASE spotify_db;
USE SCHEMA raw;

-- four tables
CREATE TABLE IF NOT EXISTS songs (song_id STRING PRIMARY KEY, name STRING, duration_ms INTEGER, explicit BOOLEAN, track_number INTEGER, album_id STRING, added_at TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS artists (artist_id STRING PRIMARY KEY, name STRING);
CREATE TABLE IF NOT EXISTS albums (album_id STRING PRIMARY KEY, name STRING, release_date STRING, total_tracks INTEGER);
CREATE TABLE IF NOT EXISTS song_artists (song_id STRING, artist_id STRING, PRIMARY KEY (song_id, artist_id));

-- storage integration (note the correct name this time!)
CREATE STORAGE INTEGRATION spotify_gcs_int
  TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'GCS' ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://spotify-etl-preetham/transformed_data/');

DESC STORAGE INTEGRATION spotify_gcs_int;

CREATE FILE FORMAT IF NOT EXISTS spotify_db.raw.spotify_csv_format
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

CREATE STAGE IF NOT EXISTS spotify_stage
  URL = 'gcs://spotify-etl-preetham/transformed_data/'
  STORAGE_INTEGRATION = spotify_gcs_int
  FILE_FORMAT = spotify_csv_format;

 LIST @spotify_stage;

 COPY INTO songs
  FROM @spotify_stage
  PATTERN = '.*songs_.*\.csv'
  FILE_FORMAT = spotify_csv_format;

COPY INTO albums
  FROM @spotify_stage
  PATTERN = '.*albums_.*\.csv'
  FILE_FORMAT = spotify_csv_format;

COPY INTO artists
  FROM @spotify_stage
  PATTERN = '.*/artists_.*\.csv'
  FILE_FORMAT = spotify_csv_format;

COPY INTO song_artists
  FROM @spotify_stage
  PATTERN = '.*song_artists_.*\.csv'
  FILE_FORMAT = spotify_csv_format;

SELECT COUNT(*) FROM songs;
SELECT COUNT(*) FROM artists;
SELECT COUNT(*) FROM albums;
SELECT COUNT(*) FROM song_artists;  


TRUNCATE TABLE songs;
TRUNCATE TABLE artists;
TRUNCATE TABLE albums;
TRUNCATE TABLE song_artists;