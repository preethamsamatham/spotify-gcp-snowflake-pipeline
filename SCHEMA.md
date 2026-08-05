SONGS         song_id (PK), name, duration_ms, explicit, track_number, album_id, added_at
ARTISTS       artist_id (PK), name
ALBUMS        album_id (PK), name, release_date, total_tracks
SONG_ARTISTS  song_id + artist_id (composite PK)