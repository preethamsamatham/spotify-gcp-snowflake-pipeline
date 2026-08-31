import csv
import io
import json
from datetime import datetime

import functions_framework
from google.cloud import storage


RAW_PREFIX = (
    "raw_data/liked_songs/to_process/"
)

PROCESSED_PREFIX = (
    "raw_data/liked_songs/processed/"
)

TRANSFORMED_PREFIX = (
    "transformed_data/liked_songs/"
)


CATALOG_COLUMNS = [
    "batch_id",
    "extracted_at",
    "source_type",
    "source_id",
    "extraction_order",
    "liked_at",
    "liked_date",
    "song_id",
    "song_name",
    "duration_ms",
    "duration_seconds",
    "explicit",
    "track_number",
    "disc_number",
    "is_local",
    "song_spotify_url",
    "album_id",
    "album_name",
    "album_type",
    "album_release_date",
    "album_release_year",
    "album_total_tracks",
    "album_image_url",
    "artist_ids",
    "artist_names",
    "artists_json",
]


def read_raw_records(blob):
    content = blob.download_as_text()

    return [
        json.loads(line)
        for line in content.splitlines()
        if line.strip()
    ]


def validate_raw_records(records, blob):
    if not records:
        raise ValueError(
            "Liked Songs raw file is empty."
        )

    expected_count = int(
        (blob.metadata or {}).get(
            "record_count",
            len(records),
        )
    )

    if len(records) != expected_count:
        raise ValueError(
            "Raw record-count mismatch: "
            f"metadata says {expected_count}, "
            f"but the file contains {len(records)}."
        )

    batch_ids = {
        record.get("batch_id")
        for record in records
    }

    extracted_values = {
        record.get("extracted_at")
        for record in records
    }

    source_types = {
        record.get("source_type")
        for record in records
    }

    if len(batch_ids) != 1 or None in batch_ids:
        raise ValueError(
            "Raw file must contain exactly "
            "one non-null batch_id."
        )

    if (
        len(extracted_values) != 1
        or None in extracted_values
    ):
        raise ValueError(
            "Raw file must contain exactly "
            "one non-null extracted_at value."
        )

    if source_types != {"liked_songs"}:
        raise ValueError(
            "Raw file contains an unexpected "
            f"source type: {source_types}"
        )

    song_ids = [
        (record.get("track") or {}).get("id")
        for record in records
    ]

    if any(not song_id for song_id in song_ids):
        raise ValueError(
            "Raw file contains a track "
            "without a Spotify song ID."
        )

    if len(set(song_ids)) != len(song_ids):
        raise ValueError(
            "Raw file contains duplicate "
            "Spotify song IDs."
        )

    return {
        "batch_id": next(iter(batch_ids)),
        "extracted_at": next(
            iter(extracted_values)
        ),
    }


def release_year(release_date):
    if not release_date:
        return None

    year_text = str(release_date)[:4]

    if year_text.isdigit():
        return int(year_text)

    return None


def first_album_image(album):
    images = album.get("images") or []

    if not images:
        return None

    return images[0].get("url")


def build_catalog_rows(records):
    rows = []

    for record in records:
        track = record["track"]
        album = track.get("album") or {}
        artists = track.get("artists") or []

        artist_values = [
            {
                "artist_id": artist.get("id"),
                "artist_name": artist.get("name"),
            }
            for artist in artists
            if artist.get("id")
        ]

        duration_ms = track.get("duration_ms")

        duration_seconds = (
            round(duration_ms / 1000.0, 3)
            if duration_ms is not None
            else None
        )

        liked_at = record.get("added_at")

        rows.append(
            {
                "batch_id": record["batch_id"],
                "extracted_at": (
                    record["extracted_at"]
                ),
                "source_type": (
                    record["source_type"]
                ),
                "source_id": record["source_id"],
                "extraction_order": (
                    record["extraction_order"]
                ),
                "liked_at": liked_at,
                "liked_date": (
                    liked_at[:10]
                    if liked_at
                    else None
                ),
                "song_id": track.get("id"),
                "song_name": track.get("name"),
                "duration_ms": duration_ms,
                "duration_seconds": (
                    duration_seconds
                ),
                "explicit": track.get("explicit"),
                "track_number": (
                    track.get("track_number")
                ),
                "disc_number": (
                    track.get("disc_number")
                ),
                "is_local": track.get("is_local"),
                "song_spotify_url": (
                    (track.get("external_urls") or {})
                    .get("spotify")
                ),
                "album_id": album.get("id"),
                "album_name": album.get("name"),
                "album_type": album.get(
                    "album_type"
                ),
                "album_release_date": album.get(
                    "release_date"
                ),
                "album_release_year": (
                    release_year(
                        album.get("release_date")
                    )
                ),
                "album_total_tracks": album.get(
                    "total_tracks"
                ),
                "album_image_url": (
                    first_album_image(album)
                ),
                "artist_ids": ", ".join(
                    artist["artist_id"]
                    for artist in artist_values
                ),
                "artist_names": ", ".join(
                    artist["artist_name"]
                    for artist in artist_values
                    if artist["artist_name"]
                ),
                "artists_json": json.dumps(
                    artist_values,
                    ensure_ascii=False,
                ),
            }
        )

    return rows


def rows_to_csv(rows):
    output = io.StringIO(newline="")

    writer = csv.DictWriter(
        output,
        fieldnames=CATALOG_COLUMNS,
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    return output.getvalue()


def move_raw_object(
    bucket,
    source_blob,
    destination_name,
):
    bucket.copy_blob(
        source_blob,
        bucket,
        destination_name,
    )

    source_blob.delete()


@functions_framework.cloud_event
def transform_liked_songs(cloud_event):
    event_data = cloud_event.data

    bucket_name = event_data["bucket"]
    object_name = event_data["name"]

    if (
        not object_name.startswith(RAW_PREFIX)
        or not object_name.endswith(".ndjson")
    ):
        print(
            json.dumps(
                {
                    "event": "ignored_object",
                    "object_name": object_name,
                }
            )
        )
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(object_name)

    source_blob.reload()

    records = read_raw_records(source_blob)

    batch_metadata = validate_raw_records(
        records,
        source_blob,
    )

    catalog_rows = build_catalog_rows(records)

    if len(catalog_rows) != len(records):
        raise ValueError(
            "Transformed row count does not "
            "match the validated raw row count."
        )

    extracted_at = datetime.fromisoformat(
        batch_metadata["extracted_at"].replace(
            "Z",
            "+00:00",
        )
    )

    timestamp = extracted_at.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    batch_id = batch_metadata["batch_id"]

    output_name = (
        f"{TRANSFORMED_PREFIX}"
        f"liked_song_catalog_{timestamp}_"
        f"{batch_id}.csv"
    )

    output_blob = bucket.blob(output_name)

    output_blob.metadata = {
        "source_type": "liked_songs",
        "batch_id": batch_id,
        "record_count": str(
            len(catalog_rows)
        ),
    }

    output_blob.upload_from_string(
        rows_to_csv(catalog_rows),
        content_type="text/csv",
    )

    processed_name = (
        f"{PROCESSED_PREFIX}"
        f"{object_name.rsplit('/', 1)[-1]}"
    )

    move_raw_object(
        bucket,
        source_blob,
        processed_name,
    )

    print(
        json.dumps(
            {
                "event": (
                    "liked_songs_transform_complete"
                ),
                "batch_id": batch_id,
                "input_rows": len(records),
                "output_rows": len(catalog_rows),
                "output_uri": (
                    f"gs://{bucket_name}/"
                    f"{output_name}"
                ),
                "processed_raw_uri": (
                    f"gs://{bucket_name}/"
                    f"{processed_name}"
                ),
            }
        )
    )