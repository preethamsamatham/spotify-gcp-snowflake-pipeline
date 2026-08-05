import os
import json
from dotenv import load_dotenv
import datetime 

import spotipy
from spotipy.oauth2 import SpotifyOAuth

load_dotenv()

def get_spotify_client():
    client_id = os.environ["SPOTIFY_CLIENT_ID"]
    client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri="http://127.0.0.1:8888/callback",
        scope="playlist-read-private",
    )
    return spotipy.Spotify(auth_manager=auth_manager)


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

if __name__ == "__main__":
    sp = get_spotify_client()
    playlist_id = "1y4gcj5nrSvezfKmuOpExp" # Example playlist ID
    all_tracks = get_all_tracks(sp, playlist_id)
    os.makedirs("raw_data", exist_ok=True)
    filename = os.path.join("raw_data", f"spotify_raw_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(all_tracks)}.json")
    with open(filename, "w", encoding="utf-8") as f:
        for track in all_tracks:
            f.write(json.dumps(track, ensure_ascii=False) + "\n")
    print(f"Saved {len(all_tracks)} tracks to {filename}")        

    