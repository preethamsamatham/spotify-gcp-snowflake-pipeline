import os
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from google.cloud import bigquery, secretmanager


GCP_PROJECT = os.getenv("GCP_PROJECT", "python-gcp-snowflake")
PIPELINE_NAME = "spotify_snowflake_to_bigquery"
BQ_DATASET = os.getenv("BQ_DATASET", "spotify_analytics")
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "GIHJUIA-ZR03463")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "BQ_SYNC_USER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "BQ_SYNC_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "BQ_SYNC_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "SPOTIFY_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")
SNOWFLAKE_KEY_SECRET = os.getenv(
    "SNOWFLAKE_KEY_SECRET",
    "bq-sync-snowflake-key",
)


SOURCE_MODELS = {
    "song_catalog": {
        "view": "SPOTIFY_DB.ANALYTICS.VW_SONG_CATALOG",
        "key": "SONG_ID",
    },
    "artist_summary": {
        "view": "SPOTIFY_DB.ANALYTICS.VW_ARTIST_SUMMARY",
        "key": "ARTIST_ID",
    },
    "liked_song_catalog": {
        "view": "SPOTIFY_DB.ANALYTICS.VW_LIKED_SONG_CATALOG",
        "key": "SONG_ID",
    },
    "liked_artist_summary": {
        "view": "SPOTIFY_DB.ANALYTICS.VW_LIKED_ARTIST_SUMMARY",
        "key": "ARTIST_ID",
    },
    "liked_song_batch_summary": {
        "view": "SPOTIFY_DB.ANALYTICS.VW_LIKED_SONG_BATCH_SUMMARY",
        "key": "BATCH_ID",
    },
}


def field(name, field_type, mode="NULLABLE"):
    return bigquery.SchemaField(name, field_type, mode=mode)


BIGQUERY_SCHEMAS = {
    "song_catalog": [
        field("song_id", "STRING", "REQUIRED"),
        field("song_name", "STRING"),
        field("duration_ms", "INTEGER"),
        field("duration_seconds", "FLOAT"),
        field("explicit", "BOOLEAN"),
        field("track_number", "INTEGER"),
        field("added_at", "TIMESTAMP"),
        field("added_date", "DATE"),
        field("album_id", "STRING"),
        field("album_name", "STRING"),
        field("release_date_raw", "STRING"),
        field("release_year", "INTEGER"),
        field("album_total_tracks", "INTEGER"),
        field("artist_names", "STRING"),
        field("artist_count", "INTEGER"),
    ],
    "artist_summary": [
        field("artist_id", "STRING", "REQUIRED"),
        field("artist_name", "STRING"),
        field("song_count", "INTEGER"),
        field("album_count", "INTEGER"),
        field("avg_song_duration_seconds", "FLOAT"),
        field("explicit_song_count", "INTEGER"),
        field("explicit_song_percentage", "FLOAT"),
        field("first_song_added_at", "TIMESTAMP"),
        field("latest_song_added_at", "TIMESTAMP"),
    ],
    "liked_song_catalog": [
        field("batch_id", "STRING", "REQUIRED"),
        field("extracted_at", "TIMESTAMP", "REQUIRED"),
        field("source_type", "STRING", "REQUIRED"),
        field("source_id", "STRING", "REQUIRED"),
        field("extraction_order", "INTEGER", "REQUIRED"),
        field("liked_at", "TIMESTAMP"),
        field("liked_date", "DATE"),
        field("song_id", "STRING", "REQUIRED"),
        field("song_name", "STRING"),
        field("duration_ms", "INTEGER"),
        field("duration_seconds", "FLOAT"),
        field("explicit", "BOOLEAN"),
        field("track_number", "INTEGER"),
        field("disc_number", "INTEGER"),
        field("is_local", "BOOLEAN"),
        field("song_spotify_url", "STRING"),
        field("album_id", "STRING"),
        field("album_name", "STRING"),
        field("album_type", "STRING"),
        field("album_release_date", "STRING"),
        field("album_release_year", "INTEGER"),
        field("album_total_tracks", "INTEGER"),
        field("album_image_url", "STRING"),
        field("artist_ids", "STRING"),
        field("artist_names", "STRING"),
        field("artists_json", "STRING"),
    ],
    "liked_artist_summary": [
        field("artist_id", "STRING", "REQUIRED"),
        field("artist_name", "STRING"),
        field("song_count", "INTEGER"),
        field("album_count", "INTEGER"),
        field("avg_song_duration_seconds", "FLOAT"),
        field("explicit_song_count", "INTEGER"),
        field("explicit_song_percentage", "FLOAT"),
        field("first_song_liked_at", "TIMESTAMP"),
        field("latest_song_liked_at", "TIMESTAMP"),
    ],
    "liked_song_batch_summary": [
        field("batch_id", "STRING", "REQUIRED"),
        field("extracted_at", "TIMESTAMP", "REQUIRED"),
        field("source_type", "STRING"),
        field("source_id", "STRING"),
        field("row_count", "INTEGER"),
        field("unique_song_count", "INTEGER"),
        field("earliest_liked_at", "TIMESTAMP"),
        field("latest_liked_at", "TIMESTAMP"),
        field("null_song_id_count", "INTEGER"),
        field("is_valid_batch", "BOOLEAN"),
    ],
}


def get_secret_bytes(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = (
        f"projects/{GCP_PROJECT}/secrets/"
        f"{secret_id}/versions/latest"
    )
    response = client.access_secret_version(request={"name": name})
    return response.payload.data


def get_snowflake_private_key():
    private_key = serialization.load_pem_private_key(
        get_secret_bytes(SNOWFLAKE_KEY_SECRET),
        password=None,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=get_snowflake_private_key(),
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def print_snowflake_identity(connection):
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA()
            """
        )
        print("Snowflake connection successful")
        print("Identity:", cursor.fetchone())
    finally:
        cursor.close()


def inspect_and_validate_source_views(connection):
    cursor = connection.cursor()
    try:
        for target_name, model in SOURCE_MODELS.items():
            source_view = model["view"]
            key_column = model["key"]

            cursor.execute(f"SELECT * FROM {source_view} LIMIT 0")
            actual_columns = [column[0] for column in cursor.description]
            expected_columns = [
                schema_field.name.upper()
                for schema_field in BIGQUERY_SCHEMAS[target_name]
            ]
            if actual_columns != expected_columns:
                raise ValueError(
                    f"Column mismatch for {source_view}. "
                    f"Expected {expected_columns}; got {actual_columns}"
                )

            cursor.execute(
                f"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT({key_column}) AS non_null_keys,
                    COUNT(DISTINCT {key_column}) AS distinct_keys
                FROM {source_view}
                """
            )
            total_rows, non_null_keys, distinct_keys = cursor.fetchone()
            print(
                f"Source: {source_view} | Target: {target_name} | "
                f"Rows: {total_rows} | Non-null keys: {non_null_keys} | "
                f"Distinct keys: {distinct_keys}"
            )

            if total_rows == 0:
                raise ValueError(f"{source_view} contains no rows")
            if non_null_keys != total_rows:
                raise ValueError(
                    f"{source_view} contains null {key_column} values"
                )
            if distinct_keys != total_rows:
                raise ValueError(
                    f"{source_view} contains duplicate {key_column} values"
                )
    finally:
        cursor.close()


def normalize_value(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def extract_source_rows(connection):
    extracted_models = {}
    cursor = connection.cursor()
    try:
        for target_name, model in SOURCE_MODELS.items():
            cursor.execute(f"SELECT * FROM {model['view']}")
            column_names = [
                column[0].lower()
                for column in cursor.description
            ]
            rows = [
                {
                    column_name: normalize_value(value)
                    for column_name, value
                    in zip(column_names, raw_row)
                }
                for raw_row in cursor.fetchall()
            ]
            extracted_models[target_name] = rows
            print(f"Extracted {len(rows)} rows for {target_name}")
    finally:
        cursor.close()
    return extracted_models


def get_bigquery_client():
    return bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


def load_bigquery_staging_tables(client, extracted_models):
    staging_tables = {}
    for target_name, rows in extracted_models.items():
        table_id = (
            f"{GCP_PROJECT}.{BQ_DATASET}.{target_name}_staging"
        )
        config = bigquery.LoadJobConfig(
            schema=BIGQUERY_SCHEMAS[target_name],
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        print(f"Loading {len(rows)} rows into {table_id}")
        job = client.load_table_from_json(
            rows,
            table_id,
            job_config=config,
            location=BQ_LOCATION,
        )
        job.result()

        loaded_rows = client.get_table(table_id).num_rows
        if loaded_rows != len(rows):
            raise ValueError(
                f"Row-count mismatch for {table_id}: "
                f"expected {len(rows)}, loaded {loaded_rows}"
            )
        print(f"Loaded {loaded_rows} rows into {table_id}")
        staging_tables[target_name] = table_id
    return staging_tables


def validate_bigquery_staging_tables(
    client,
    staging_tables,
    extracted_models,
):
    for target_name, table_id in staging_tables.items():
        key_column = SOURCE_MODELS[target_name]["key"].lower()
        expected_rows = len(extracted_models[target_name])
        sql = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(`{key_column}`) AS non_null_keys,
                COUNT(DISTINCT `{key_column}`) AS distinct_keys
            FROM `{table_id}`
        """
        row = next(client.query(sql, location=BQ_LOCATION).result())
        total_rows = row["total_rows"]
        non_null_keys = row["non_null_keys"]
        distinct_keys = row["distinct_keys"]
        print(
            f"Validated {table_id} | Expected: {expected_rows} | "
            f"Rows: {total_rows} | Non-null keys: {non_null_keys} | "
            f"Distinct keys: {distinct_keys}"
        )
        if total_rows != expected_rows:
            raise ValueError(f"Row-count validation failed for {table_id}")
        if non_null_keys != total_rows:
            raise ValueError(f"Null-key validation failed for {table_id}")
        if distinct_keys != total_rows:
            raise ValueError(f"Duplicate-key validation failed for {table_id}")


def ensure_bigquery_production_tables(client, staging_tables):
    production_tables = {}
    for target_name, staging_table_id in staging_tables.items():
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{target_name}"
        client.query(
            f"CREATE TABLE IF NOT EXISTS `{table_id}` "
            f"LIKE `{staging_table_id}`",
            location=BQ_LOCATION,
        ).result()
        production_tables[target_name] = table_id
        print(f"Production table ready: {table_id}")
    return production_tables


def ensure_bigquery_sync_status_table(client):
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.sync_status"
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{table_id}` (
            pipeline_name STRING,
            batch_id STRING,
            last_successful_sync TIMESTAMP,
            song_catalog_rows INT64,
            artist_summary_rows INT64,
            liked_song_catalog_rows INT64,
            liked_artist_summary_rows INT64,
            liked_song_batch_summary_rows INT64,
            liked_source_batch_id STRING,
            liked_source_extracted_at TIMESTAMP
        )
        """,
        location=BQ_LOCATION,
    ).result()

    new_columns = {
        "liked_song_catalog_rows": "INT64",
        "liked_artist_summary_rows": "INT64",
        "liked_song_batch_summary_rows": "INT64",
        "liked_source_batch_id": "STRING",
        "liked_source_extracted_at": "TIMESTAMP",
    }
    for column_name, column_type in new_columns.items():
        client.query(
            f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS "
            f"{column_name} {column_type}",
            location=BQ_LOCATION,
        ).result()

    print(f"Sync-status table ready: {table_id}")
    return table_id


def publish_bigquery_production_tables(
    client,
    staging_tables,
    production_tables,
    status_table_id,
    sync_batch_id,
):
    statements = ["BEGIN TRANSACTION;"]
    for target_name in SOURCE_MODELS:
        staging_id = staging_tables[target_name]
        production_id = production_tables[target_name]
        statements.append(f"DELETE FROM `{production_id}` WHERE TRUE;")
        statements.append(
            f"INSERT INTO `{production_id}` "
            f"SELECT * FROM `{staging_id}`;"
        )

    statements.append(
        f"""
        DELETE FROM `{status_table_id}`
        WHERE pipeline_name = @pipeline_name;

        INSERT INTO `{status_table_id}` (
            pipeline_name,
            batch_id,
            last_successful_sync,
            song_catalog_rows,
            artist_summary_rows,
            liked_song_catalog_rows,
            liked_artist_summary_rows,
            liked_song_batch_summary_rows,
            liked_source_batch_id,
            liked_source_extracted_at
        )
        SELECT
            @pipeline_name,
            @sync_batch_id,
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `{staging_tables['song_catalog']}`),
            (SELECT COUNT(*) FROM `{staging_tables['artist_summary']}`),
            (SELECT COUNT(*) FROM `{staging_tables['liked_song_catalog']}`),
            (SELECT COUNT(*) FROM `{staging_tables['liked_artist_summary']}`),
            (SELECT COUNT(*) FROM `{staging_tables['liked_song_batch_summary']}`),
            (
                SELECT batch_id
                FROM `{staging_tables['liked_song_batch_summary']}`
                WHERE is_valid_batch = TRUE
                ORDER BY extracted_at DESC, batch_id DESC
                LIMIT 1
            ),
            (
                SELECT extracted_at
                FROM `{staging_tables['liked_song_batch_summary']}`
                WHERE is_valid_batch = TRUE
                ORDER BY extracted_at DESC, batch_id DESC
                LIMIT 1
            );
        """
    )
    statements.append("COMMIT TRANSACTION;")

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "pipeline_name",
                "STRING",
                PIPELINE_NAME,
            ),
            bigquery.ScalarQueryParameter(
                "sync_batch_id",
                "STRING",
                sync_batch_id,
            ),
        ]
    )
    client.query(
        "\n".join(statements),
        job_config=config,
        location=BQ_LOCATION,
    ).result()
    print(
        "Atomic production publication completed for "
        f"sync batch {sync_batch_id}"
    )


def main():
    sync_batch_id = str(uuid4())
    print("Sync batch ID:", sync_batch_id)

    connection = get_snowflake_connection()
    try:
        print_snowflake_identity(connection)
        inspect_and_validate_source_views(connection)
        extracted_models = extract_source_rows(connection)
    finally:
        connection.close()

    for target_name, rows in extracted_models.items():
        print(f"Prepared {target_name}: {len(rows)} rows")

    client = get_bigquery_client()
    staging_tables = load_bigquery_staging_tables(
        client,
        extracted_models,
    )
    validate_bigquery_staging_tables(
        client,
        staging_tables,
        extracted_models,
    )
    print("All BigQuery staging validations passed")

    production_tables = ensure_bigquery_production_tables(
        client,
        staging_tables,
    )
    status_table_id = ensure_bigquery_sync_status_table(client)
    publish_bigquery_production_tables(
        client,
        staging_tables,
        production_tables,
        status_table_id,
        sync_batch_id,
    )


if __name__ == "__main__":
    main()
