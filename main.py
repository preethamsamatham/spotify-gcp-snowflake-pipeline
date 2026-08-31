from google.cloud import secretmanager
import spotipy 
from spotipy.oauth2 import SpotifyOAuth
import functions_framework
from google.cloud import storage
import json
from datetime import datetime


PLAYLIST_ID = "1y4gcj5nrSvezfKmuOpExp" 
def get_secret(secret_name):
    project_id = "python-gcp-snowflake"  # Replace with your GCP project ID
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")

def get_playlist_snapshot_id(sp, playlist_id):
    playlist = sp.playlist(playlist_id, fields="snapshot_id")
    return playlist["snapshot_id"]

def get_spotify_client():
    client_id = get_secret("spotify-client-id")
    client_secret = get_secret("spotify-client-secret")
    refresh_token = get_secret("spotify-refresh-token")

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri="http://127.0.0.1:8888/callback",
        scope="playlist-read-private"
    )
    token_info = auth_manager.refresh_access_token(refresh_token)
    access_token = token_info["access_token"]
    return spotipy.Spotify(auth=access_token)

def get_all_tracks(sp, playlist_id):
    results = sp.playlist_items(playlist_id)
    tracks = []
    while results:
        for item in results['items']:
            if item['item'] is None: 
                continue # Check if the track is not None
            tracks.append(item)
        if results['next']:
            results = sp.next(results)
        else:
            results = None
    return tracks

@functions_framework.http
def extract_spotify(request):   
    sp = get_spotify_client()
    snapshot_id = get_playlist_snapshot_id(sp, PLAYLIST_ID)
    tracks = get_all_tracks(sp, PLAYLIST_ID)
    ndjson = "\n".join(json.dumps(track, ensure_ascii=False) for track in tracks)
    filename = f"raw_data/to_process/spotify_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    storage_client = storage.Client()
    bucket = storage_client.bucket("spotify-etl-preetham")
    blob = bucket.blob(filename)
    blob.upload_from_string(ndjson)
    return (
        f"Saved {len(tracks)} tracks for snapshot {snapshot_id} "
        f"to gs://spotify-etl-preetham/{filename}"
        )

if __name__ == "__main__":
    sp = get_spotify_client()
    results = sp.playlist_items(PLAYLIST_ID, limit=1)
    print(results["items"][0]["item"]["name"])