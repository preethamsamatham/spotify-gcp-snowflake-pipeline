import os
import pandas as pd
import json
from google.cloud import storage
import functions_framework

@functions_framework.cloud_event
def transform_trigger(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    if not file_name.startswith("raw_data/to_process/"):
        print(f"Ignoring {file_name} (not in to_process/)")
        return

    transform_gcs(bucket_name, file_name)

def load_raw(filename):
    """Load raw JSON data from a file into a pandas DataFrame."""
    with open(filename, 'r', encoding='utf-8') as f:
        data = [json.loads(line) for line in f]
        # print(f"Number of tracks loaded: {len(data)}")
    return data

def build_songs(data):
    songs = []
    for record in data:
        if record['item'] is None:  # Check if the track is not None
            continue
        song = {
     "song_id":      record["item"]["id"],
     "name":         record["item"]["name"],
     "duration_ms":  record["item"]["duration_ms"],
     "explicit":     record["item"]["explicit"],
     "track_number": record["item"]["track_number"],
     "album_id":     record["item"]["album"]["id"],
     "added_at":     record["added_at"],
  }
        songs.append(song)

    return pd.DataFrame(songs)

def build_albums(data):
    albums = []
    for record in data:
        if record['item'] is None:  # Check if the track is not None
            continue
        album = {
            "album_id": record["item"]["album"]["id"],
            "name": record["item"]["album"]["name"],
            "release_date": record["item"]["album"]["release_date"],
            "total_tracks": record["item"]["album"]["total_tracks"]
        }
        albums.append(album)
    return pd.DataFrame(albums).drop_duplicates(subset=["album_id"])

def build_artists(data):
    artists = []
    for record in data:
        if record["item"] is None:
            continue
        for artist in record["item"]["artists"]:   # ← inner loop over the list
            artists.append({
                "artist_id": artist["id"],
                "name":      artist["name"],
            })
    return pd.DataFrame(artists).drop_duplicates(subset=["artist_id"])

def build_song_artists(data):
    song_artists = []
    for record in data:
        if record["item"] is None:
            continue
        for artist in record["item"]["artists"]:
            song_artists.append({
                "song_id":   record["item"]["id"],
                "artist_id": artist["id"],
            })
    return pd.DataFrame(song_artists).drop_duplicates(subset=["song_id", "artist_id"])

def load_raw_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()
    data = [json.loads(line) for line in content.splitlines()]
    return data


def write_csv_to_gcs(df, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_string = df.to_csv(index=False)
    blob.upload_from_string(csv_string, content_type="text/csv") 
    
     

def transform_gcs(bucket_name, input_blob_name):
    # 1. read raw from GCS (your helper)
    data = load_raw_from_gcs(bucket_name, input_blob_name)
    
    # 2. build the four tables (your existing functions, unchanged)
    songs = build_songs(data)
    albums = build_albums(data)
    artists = build_artists(data)
    song_artists = build_song_artists(data)

    # 3. write each to GCS under transformed_data/ (your helper)
    # 3. write each to GCS under transformed_data/ with the raw file's timestamp
    stamp = input_blob_name.split("spotify_raw_")[1].replace(".json", "")

    write_csv_to_gcs(songs,        bucket_name, f"transformed_data/songs_{stamp}.csv")
    write_csv_to_gcs(albums,       bucket_name, f"transformed_data/albums_{stamp}.csv")
    write_csv_to_gcs(artists,      bucket_name, f"transformed_data/artists_{stamp}.csv")
    write_csv_to_gcs(song_artists, bucket_name, f"transformed_data/song_artists_{stamp}.csv")

    # 4. receipt
    print(f"Transformed {input_blob_name} → "
          f"{len(songs)} songs, {len(albums)} albums, "
          f"{len(artists)} artists, {len(song_artists)} song-artist links")  

    # 5. move the processed raw file out of the work queue
    dest = input_blob_name.replace("to_process/", "processed/")
    move_blob(bucket_name, input_blob_name, dest)  

def move_blob(bucket_name, source_blob, dest_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    src = bucket.blob(source_blob)
    bucket.copy_blob(src, bucket, dest_blob)
    src.delete()            

if __name__ == "__main__":
    transform_gcs("spotify-etl-preetham", "raw_data/to_process/spotify_raw_20260810_070007.json")
    