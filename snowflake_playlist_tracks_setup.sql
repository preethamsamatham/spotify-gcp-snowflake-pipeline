-- Playlist snapshot persistence for the BigQuery / Looker Studio serving layer.
-- This migration is intentionally non-destructive.
-- Review before running; it does not deploy Cloud Functions or modify the load task.

USE ROLE SYSADMIN;
USE DATABASE SPOTIFY_DB;
USE SCHEMA RAW;

-- One load workspace for newly produced playlist_tracks_<timestamp>.csv files.
-- TRANSIENT is appropriate because these rows can be rebuilt from GCS.
CREATE TRANSIENT TABLE IF NOT EXISTS PLAYLIST_TRACKS_STAGING (
    SNAPSHOT_ID STRING NOT NULL,
    PLAYLIST_ID STRING NOT NULL,
    POSITION INTEGER NOT NULL,
    SONG_ID STRING NOT NULL,
    ADDED_AT TIMESTAMP_TZ,
    EXTRACTED_AT TIMESTAMP_TZ NOT NULL,
    SOURCE_FILE STRING,
    LOADED_AT TIMESTAMP_TZ
)
COMMENT = 'Transient load workspace for playlist snapshot entries';

-- Historical playlist versions. Repeated song IDs are valid when their
-- playlist positions differ. Including PLAYLIST_ID makes the grain safe if
-- the project later supports more than one playlist.
CREATE TABLE IF NOT EXISTS PLAYLIST_TRACKS (
    PLAYLIST_ID STRING NOT NULL,
    SNAPSHOT_ID STRING NOT NULL,
    POSITION INTEGER NOT NULL,
    SONG_ID STRING NOT NULL,
    ADDED_AT TIMESTAMP_TZ,
    SNAPSHOT_FIRST_OBSERVED_AT TIMESTAMP_TZ NOT NULL,
    SNAPSHOT_LAST_OBSERVED_AT TIMESTAMP_TZ NOT NULL,
    SOURCE_FILE STRING,
    LOADED_AT TIMESTAMP_TZ NOT NULL,
    CONSTRAINT PK_PLAYLIST_TRACKS
        PRIMARY KEY (PLAYLIST_ID, SNAPSHOT_ID, POSITION)
)
COMMENT = 'One row per playlist, Spotify snapshot, and playlist position';

-- Verification only.
DESCRIBE TABLE PLAYLIST_TRACKS_STAGING;
DESCRIBE TABLE PLAYLIST_TRACKS;
