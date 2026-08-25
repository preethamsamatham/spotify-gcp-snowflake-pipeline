from google.cloud import secretmanager
import spotipy 
from spotipy.oauth2 import SpotifyOAuth
import functions_framework
from google.cloud import storage
import json
from datetime import datetime, timezone


PLAYLIST_ID = "1y4gcj5nrSvezfKmuOpExp" 
def get_secret(secret_name):
    project_id = "python-gcp-snowflake"  # Replace with your GCP project ID
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")

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

def get_playlist_snapshot_id(sp, playlist_id):
    playlist = sp.playlist(playlist_id, fields="snapshot_id")
    return playlist["snapshot_id"]


def get_all_tracks(sp, playlist_id, snapshot_id, extracted_at):
    results = sp.playlist_items(playlist_id)
    tracks = []

    while results:
        page_offset = results["offset"]

        for page_index, item in enumerate(results["items"]):
            if item["item"] is None:
                continue

            enriched_item = dict(item)
            enriched_item["pipeline_metadata"] = {
                "playlist_id": playlist_id,
                "snapshot_id": snapshot_id,
                "position": page_offset + page_index,
                "extracted_at": extracted_at,
            }
            tracks.append(enriched_item)

        if results["next"]:
            results = sp.next(results)
        else:
            results = None

    return tracks

@functions_framework.http
def extract_spotify(request):
    sp = get_spotify_client()
    extracted_at = datetime.now(timezone.utc)
    extracted_at_iso = extracted_at.isoformat().replace("+00:00", "Z")
    snapshot_id = get_playlist_snapshot_id(sp, PLAYLIST_ID)
    tracks = get_all_tracks(
        sp,
        PLAYLIST_ID,
        snapshot_id,
        extracted_at_iso,
    )
    ndjson = "\n".join(json.dumps(track, ensure_ascii=False) for track in tracks)
    filename = f"raw_data/to_process/spotify_raw_{extracted_at.strftime('%Y%m%d_%H%M%S')}.json"
    storage_client = storage.Client()
    bucket = storage_client.bucket("spotify-etl-preetham")
    blob = bucket.blob(filename)
    blob.upload_from_string(ndjson)
    return (
        f"Saved {len(tracks)} playlist entries "
        f"for snapshot {snapshot_id} "
        f"to gs://spotify-etl-preetham/{filename}"
    )

if __name__ == "__main__":
    sp = get_spotify_client()
    results = sp.playlist_items(PLAYLIST_ID, limit=1)
    print(results["items"][0]["item"]["name"])