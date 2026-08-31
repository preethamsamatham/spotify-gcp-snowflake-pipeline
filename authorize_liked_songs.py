import os

import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth


load_dotenv()

auth_manager = SpotifyOAuth(
    client_id=os.environ["SPOTIFY_CLIENT_ID"],
    client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-library-read",
    cache_path=".cache-liked-songs",
    show_dialog=True,
)

spotify = spotipy.Spotify(auth_manager=auth_manager)

def get_all_liked_songs(spotify_client):
    page = spotify_client.current_user_saved_tracks(limit=50)

    api_total = page["total"]
    saved_items = []
    page_count = 0

    while page:
        page_count += 1
        saved_items.extend(page["items"])

        if page["next"]:
            page = spotify_client.next(page)
        else:
            page = None

    return saved_items, api_total, page_count


saved_items, api_total, page_count = get_all_liked_songs(spotify)

valid_items = [
    item
    for item in saved_items
    if item.get("track") and item["track"].get("id")
]

unique_song_ids = {
    item["track"]["id"]
    for item in valid_items
}

print("Liked Songs authorization and pagination succeeded.")
print(f"Spotify API total: {api_total}")
print(f"Downloaded items: {len(saved_items)}")
print(f"Valid Spotify tracks: {len(valid_items)}")
print(f"Unique track IDs: {len(unique_song_ids)}")
print(f"Pages requested: {page_count}")