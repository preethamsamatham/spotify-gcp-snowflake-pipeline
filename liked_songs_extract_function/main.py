import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import functions_framework
import spotipy
from google.cloud import secretmanager, storage
from spotipy.cache_handler import MemoryCacheHandler
from spotipy.oauth2 import SpotifyOAuth


PROJECT_ID = os.getenv(
    "GCP_PROJECT",
    "python-gcp-snowflake",
)

BUCKET_NAME = os.getenv(
    "GCS_BUCKET",
    "spotify-etl-preetham",
)

SPOTIFY_SCOPE = "user-library-read"

SPOTIFY_REDIRECT_URI = (
    "http://127.0.0.1:8888/callback"
)

SOURCE_TYPE = "liked_songs"
SOURCE_ID = "current_user_library"

RAW_PREFIX = (
    "raw_data/liked_songs/to_process"
)


def get_secret(secret_name):
    client = (
        secretmanager
        .SecretManagerServiceClient()
    )

    secret_path = (
        f"projects/{PROJECT_ID}/secrets/"
        f"{secret_name}/versions/latest"
    )

    response = client.access_secret_version(
        request={"name": secret_path}
    )

    return response.payload.data.decode("UTF-8")


def get_spotify_client():
    client_id = get_secret(
        "spotify-client-id"
    )

    client_secret = get_secret(
        "spotify-client-secret"
    )

    refresh_token = get_secret(
        "spotify-liked-songs-refresh-token"
    )

    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=SPOTIFY_REDIRECT_URI,
        scope=SPOTIFY_SCOPE,
        cache_handler=MemoryCacheHandler(),
    )

    token_info = (
        auth_manager.refresh_access_token(
            refresh_token
        )
    )

    return spotipy.Spotify(
        auth=token_info["access_token"],
        requests_timeout=30,
        retries=3,
    )


def get_all_liked_songs(spotify_client):
    page = (
        spotify_client
        .current_user_saved_tracks(limit=50)
    )

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


def validate_liked_songs(
    saved_items,
    api_total,
    page_count,
):
    downloaded_count = len(saved_items)

    if api_total == 0:
        raise ValueError(
            "Spotify reported zero Liked Songs. "
            "Refusing to publish an empty snapshot."
        )

    if downloaded_count != api_total:
        raise ValueError(
            "Incomplete Liked Songs extraction: "
            f"Spotify reported {api_total}, but "
            f"only {downloaded_count} were downloaded."
        )

    expected_pages = max(
        1,
        (api_total + 49) // 50,
    )

    if page_count != expected_pages:
        raise ValueError(
            "Unexpected pagination result: "
            f"expected {expected_pages} pages, "
            f"but downloaded {page_count}."
        )

    invalid_items = [
        item
        for item in saved_items
        if not item.get("track")
        or not item["track"].get("id")
    ]

    if invalid_items:
        raise ValueError(
            "Liked Songs extraction contains "
            f"{len(invalid_items)} invalid tracks."
        )

    track_ids = [
        item["track"]["id"]
        for item in saved_items
    ]

    unique_track_count = len(set(track_ids))

    if unique_track_count != downloaded_count:
        duplicate_count = (
            downloaded_count
            - unique_track_count
        )

        raise ValueError(
            "Liked Songs extraction contains "
            f"{duplicate_count} duplicate track IDs."
        )

    return {
        "downloaded_count": downloaded_count,
        "unique_track_count": unique_track_count,
        "page_count": page_count,
    }


def build_raw_records(
    saved_items,
    batch_id,
    extracted_at,
):
    records = []

    for extraction_order, saved_item in enumerate(
        saved_items
    ):
        records.append(
            {
                "source_type": SOURCE_TYPE,
                "source_id": SOURCE_ID,
                "batch_id": batch_id,
                "extracted_at": (
                    extracted_at.isoformat()
                ),
                "extraction_order": (
                    extraction_order
                ),
                "added_at": saved_item["added_at"],
                "track": saved_item["track"],
            }
        )

    return records


def upload_raw_records(
    records,
    batch_id,
    extracted_at,
):
    timestamp = extracted_at.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    object_name = (
        f"{RAW_PREFIX}/"
        f"liked_songs_raw_{timestamp}_"
        f"{batch_id}.ndjson"
    )

    ndjson = "\n".join(
        json.dumps(
            record,
            ensure_ascii=False,
        )
        for record in records
    )

    ndjson += "\n"

    storage_client = storage.Client(
        project=PROJECT_ID
    )

    bucket = storage_client.bucket(
        BUCKET_NAME
    )

    blob = bucket.blob(object_name)

    blob.metadata = {
        "source_type": SOURCE_TYPE,
        "batch_id": batch_id,
        "record_count": str(len(records)),
    }

    blob.upload_from_string(
        ndjson,
        content_type="application/x-ndjson",
    )

    return object_name


@functions_framework.http
def extract_liked_songs(request):
    del request

    extracted_at = datetime.now(
        timezone.utc
    )

    batch_id = str(uuid4())

    spotify_client = get_spotify_client()

    saved_items, api_total, page_count = (
        get_all_liked_songs(
            spotify_client
        )
    )

    metrics = validate_liked_songs(
        saved_items,
        api_total,
        page_count,
    )

    records = build_raw_records(
        saved_items,
        batch_id,
        extracted_at,
    )

    object_name = upload_raw_records(
        records,
        batch_id,
        extracted_at,
    )

    response = {
        "status": "success",
        "source_type": SOURCE_TYPE,
        "batch_id": batch_id,
        "extracted_at": (
            extracted_at.isoformat()
        ),
        "api_total": api_total,
        **metrics,
        "gcs_uri": (
            f"gs://{BUCKET_NAME}/"
            f"{object_name}"
        ),
    }

    print(
        json.dumps(
            {
                "event": (
                    "liked_songs_extract_complete"
                ),
                **response,
            }
        )
    )

    return (
        json.dumps(response),
        200,
        {
            "Content-Type": (
                "application/json"
            )
        },
    )