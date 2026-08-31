import os
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from google.cloud import bigquery, secretmanager


GCP_PROJECT = os.getenv(
    "GCP_PROJECT",
    "python-gcp-snowflake",
)

PIPELINE_NAME = (
    "spotify_snowflake_to_bigquery"
)

BQ_DATASET = os.getenv(
    "BQ_DATASET",
    "spotify_analytics",
)
BQ_LOCATION = os.getenv(
    "BQ_LOCATION",
    "us-central1",
)

SNOWFLAKE_ACCOUNT = os.getenv(
    "SNOWFLAKE_ACCOUNT",
    "GIHJUIA-ZR03463",
)
SNOWFLAKE_USER = os.getenv(
    "SNOWFLAKE_USER",
    "BQ_SYNC_USER",
)
SNOWFLAKE_ROLE = os.getenv(
    "SNOWFLAKE_ROLE",
    "BQ_SYNC_ROLE",
)
SNOWFLAKE_WAREHOUSE = os.getenv(
    "SNOWFLAKE_WAREHOUSE",
    "BQ_SYNC_WH",
)
SNOWFLAKE_DATABASE = os.getenv(
    "SNOWFLAKE_DATABASE",
    "SPOTIFY_DB",
)
SNOWFLAKE_SCHEMA = os.getenv(
    "SNOWFLAKE_SCHEMA",
    "ANALYTICS",
)
SNOWFLAKE_KEY_SECRET = os.getenv(
    "SNOWFLAKE_KEY_SECRET",
    "bq-sync-snowflake-key",
)


SOURCE_MODELS = {
    "song_catalog": {
        "view": (
            "SPOTIFY_DB.ANALYTICS."
            "VW_SONG_CATALOG"
        ),
        "key": "SONG_ID",
    },
    "artist_summary": {
        "view": (
            "SPOTIFY_DB.ANALYTICS."
            "VW_ARTIST_SUMMARY"
        ),
        "key": "ARTIST_ID",
    },
}


BIGQUERY_SCHEMAS = {
    "song_catalog": [
        bigquery.SchemaField(
            "song_id",
            "STRING",
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            "song_name",
            "STRING",
        ),
        bigquery.SchemaField(
            "duration_ms",
            "INTEGER",
        ),
        bigquery.SchemaField(
            "duration_seconds",
            "FLOAT",
        ),
        bigquery.SchemaField(
            "explicit",
            "BOOLEAN",
        ),
        bigquery.SchemaField(
            "track_number",
            "INTEGER",
        ),
        bigquery.SchemaField(
            "added_at",
            "TIMESTAMP",
        ),
        bigquery.SchemaField(
            "added_date",
            "DATE",
        ),
        bigquery.SchemaField(
            "album_id",
            "STRING",
        ),
        bigquery.SchemaField(
            "album_name",
            "STRING",
        ),
        bigquery.SchemaField(
            "release_date_raw",
            "STRING",
        ),
        bigquery.SchemaField(
            "release_year",
            "INTEGER",
        ),
        bigquery.SchemaField(
            "album_total_tracks",
            "INTEGER",
        ),
        bigquery.SchemaField(
            "artist_names",
            "STRING",
        ),
        bigquery.SchemaField(
            "artist_count",
            "INTEGER",
        ),
    ],
    "artist_summary": [
        bigquery.SchemaField(
            "artist_id",
            "STRING",
            mode="REQUIRED",
        ),
        bigquery.SchemaField(
            "artist_name",
            "STRING",
        ),
        bigquery.SchemaField(
            "song_count",
            "INTEGER",
        ),
        bigquery.SchemaField(
            "album_count",
            "INTEGER",
        ),
        bigquery.SchemaField(
            "avg_song_duration_seconds",
            "FLOAT",
        ),
        bigquery.SchemaField(
            "explicit_song_count",
            "INTEGER",
        ),
        bigquery.SchemaField(
            "explicit_song_percentage",
            "FLOAT",
        ),
        bigquery.SchemaField(
            "first_song_added_at",
            "TIMESTAMP",
        ),
        bigquery.SchemaField(
            "latest_song_added_at",
            "TIMESTAMP",
        ),
    ],
}


def get_secret_bytes(secret_id):
    client = (
        secretmanager
        .SecretManagerServiceClient()
    )

    secret_name = (
        f"projects/{GCP_PROJECT}/secrets/"
        f"{secret_id}/versions/latest"
    )

    response = client.access_secret_version(
        request={"name": secret_name}
    )

    return response.payload.data


def get_snowflake_private_key():
    pem_bytes = get_secret_bytes(
        SNOWFLAKE_KEY_SECRET
    )

    private_key = (
        serialization.load_pem_private_key(
            pem_bytes,
            password=None,
        )
    )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=(
            serialization.NoEncryption()
        ),
    )


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=(
            get_snowflake_private_key()
        ),
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

        identity = cursor.fetchone()

        print(
            "Snowflake connection successful"
        )
        print("Identity:", identity)

    finally:
        cursor.close()


def inspect_and_validate_source_views(
    connection,
):
    cursor = connection.cursor()

    try:
        for target_name, model in (
            SOURCE_MODELS.items()
        ):
            source_view = model["view"]
            key_column = model["key"]

            cursor.execute(
                f"""
                SELECT *
                FROM {source_view}
                LIMIT 0
                """
            )

            columns = [
                column[0]
                for column in cursor.description
            ]

            cursor.execute(
                f"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT({key_column})
                        AS non_null_keys,
                    COUNT(
                        DISTINCT {key_column}
                    ) AS distinct_keys
                FROM {source_view}
                """
            )

            (
                total_rows,
                non_null_keys,
                distinct_keys,
            ) = cursor.fetchone()

            print(
                f"Source: {source_view} | "
                f"Target: {target_name} | "
                f"Rows: {total_rows} | "
                f"Non-null keys: "
                f"{non_null_keys} | "
                f"Distinct keys: "
                f"{distinct_keys}"
            )

            print("Columns:", columns)

            if total_rows == 0:
                raise ValueError(
                    f"{source_view} contains "
                    f"no rows"
                )

            if non_null_keys != total_rows:
                raise ValueError(
                    f"{source_view} contains "
                    f"null {key_column} values"
                )

            if distinct_keys != total_rows:
                raise ValueError(
                    f"{source_view} contains "
                    f"duplicate {key_column} "
                    f"values"
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
            value = value.replace(
                tzinfo=timezone.utc
            )

        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    return value


def extract_source_rows(connection):
    extracted_models = {}
    cursor = connection.cursor()

    try:
        for target_name, model in (
            SOURCE_MODELS.items()
        ):
            source_view = model["view"]

            cursor.execute(
                f"""
                SELECT *
                FROM {source_view}
                """
            )

            column_names = [
                column[0].lower()
                for column in cursor.description
            ]

            raw_rows = cursor.fetchall()
            normalized_rows = []

            for raw_row in raw_rows:
                normalized_row = {
                    column_name: (
                        normalize_value(value)
                    )
                    for column_name, value
                    in zip(
                        column_names,
                        raw_row,
                    )
                }

                normalized_rows.append(
                    normalized_row
                )

            extracted_models[target_name] = (
                normalized_rows
            )

            print(
                f"Extracted "
                f"{len(normalized_rows)} rows "
                f"for {target_name}"
            )

    finally:
        cursor.close()

    return extracted_models


def get_bigquery_client():
    return bigquery.Client(
        project=GCP_PROJECT,
        location=BQ_LOCATION,
    )


def load_bigquery_staging_tables(
    client,
    extracted_models,
):
    staging_tables = {}

    for target_name, rows in (
        extracted_models.items()
    ):
        staging_table_name = (
            f"{target_name}_staging"
        )

        staging_table_id = (
            f"{GCP_PROJECT}."
            f"{BQ_DATASET}."
            f"{staging_table_name}"
        )

        job_config = bigquery.LoadJobConfig(
            schema=(
                BIGQUERY_SCHEMAS[target_name]
            ),
            create_disposition=(
                bigquery.CreateDisposition
                .CREATE_IF_NEEDED
            ),
            write_disposition=(
                bigquery.WriteDisposition
                .WRITE_TRUNCATE
            ),
        )

        print(
            f"Loading {len(rows)} rows into "
            f"{staging_table_id}"
        )

        load_job = (
            client.load_table_from_json(
                rows,
                staging_table_id,
                job_config=job_config,
                location=BQ_LOCATION,
            )
        )

        load_job.result()

        staging_table = client.get_table(
            staging_table_id
        )

        loaded_rows = staging_table.num_rows

        print(
            f"Loaded {loaded_rows} rows into "
            f"{staging_table_id}"
        )

        if loaded_rows != len(rows):
            raise ValueError(
                f"BigQuery row-count mismatch "
                f"for {staging_table_id}: "
                f"expected {len(rows)}, "
                f"loaded {loaded_rows}"
            )

        staging_tables[target_name] = (
            staging_table_id
        )

    return staging_tables


def validate_bigquery_staging_tables(
    client,
    staging_tables,
    extracted_models,
):
    for target_name, staging_table_id in (
        staging_tables.items()
    ):
        key_column = (
            SOURCE_MODELS[target_name]
            ["key"]
            .lower()
        )

        expected_rows = len(
            extracted_models[target_name]
        )

        validation_sql = f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(`{key_column}`)
                    AS non_null_keys,
                COUNT(
                    DISTINCT `{key_column}`
                ) AS distinct_keys
            FROM `{staging_table_id}`
        """

        validation_job = client.query(
            validation_sql,
            location=BQ_LOCATION,
        )

        validation_row = next(
            validation_job.result()
        )

        total_rows = (
            validation_row["total_rows"]
        )
        non_null_keys = (
            validation_row["non_null_keys"]
        )
        distinct_keys = (
            validation_row["distinct_keys"]
        )

        print(
            f"Validated "
            f"{staging_table_id} | "
            f"Expected: {expected_rows} | "
            f"Rows: {total_rows} | "
            f"Non-null keys: "
            f"{non_null_keys} | "
            f"Distinct keys: "
            f"{distinct_keys}"
        )

        if total_rows != expected_rows:
            raise ValueError(
                f"Row-count validation failed "
                f"for {staging_table_id}"
            )

        if non_null_keys != total_rows:
            raise ValueError(
                f"Null-key validation failed "
                f"for {staging_table_id}"
            )

        if distinct_keys != total_rows:
            raise ValueError(
                f"Duplicate-key validation "
                f"failed for "
                f"{staging_table_id}"
            )


def ensure_bigquery_production_tables(
    client,
    staging_tables,
):
    production_tables = {}

    for target_name, staging_table_id in (
        staging_tables.items()
    ):
        production_table_id = (
            f"{GCP_PROJECT}."
            f"{BQ_DATASET}."
            f"{target_name}"
        )

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS
                `{production_table_id}`
            LIKE `{staging_table_id}`
        """

        client.query(
            create_sql,
            location=BQ_LOCATION,
        ).result()

        production_tables[target_name] = (
            production_table_id
        )

        print(
            f"Production table ready: "
            f"{production_table_id}"
        )

    return production_tables

def ensure_bigquery_sync_status_table(
    client,
):
    status_table_id = (
        f"{GCP_PROJECT}."
        f"{BQ_DATASET}."
        f"sync_status"
    )

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS
            `{status_table_id}`
        (
            pipeline_name STRING,
            batch_id STRING,
            last_successful_sync TIMESTAMP,
            song_catalog_rows INT64,
            artist_summary_rows INT64
        )
    """

    client.query(
        create_sql,
        location=BQ_LOCATION,
    ).result()

    print(
        f"Sync-status table ready: "
        f"{status_table_id}"
    )

    return status_table_id

def publish_bigquery_production_tables(
    client,
    staging_tables,
    production_tables,
    status_table_id,
    batch_id,
):
    statements = ["BEGIN TRANSACTION;"]

    for target_name in SOURCE_MODELS:
        staging_table_id = (
            staging_tables[target_name]
        )
        production_table_id = (
            production_tables[target_name]
        )

        statements.append(
            f"""
            DELETE FROM
                `{production_table_id}`
            WHERE TRUE;
            """
        )

        statements.append(
            f"""
            INSERT INTO
                `{production_table_id}`
            SELECT *
            FROM `{staging_table_id}`;
            """
        )

    statements.append(
        f"""
        DELETE FROM `{status_table_id}`
        WHERE pipeline_name = @pipeline_name;

        INSERT INTO `{status_table_id}`
        (
            pipeline_name,
            batch_id,
            last_successful_sync,
            song_catalog_rows,
            artist_summary_rows
        )
        SELECT
            @pipeline_name,
            @batch_id,
            CURRENT_TIMESTAMP(),
            (
                SELECT COUNT(*)
                FROM `{
                    staging_tables["song_catalog"]
                }`
            ),
            (
                SELECT COUNT(*)
                FROM `{
                    staging_tables["artist_summary"]
                }`
            );
        """
    )

    statements.append("COMMIT TRANSACTION;")

    publication_sql = "\n".join(
        statements
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "pipeline_name",
                "STRING",
                PIPELINE_NAME,
            ),
            bigquery.ScalarQueryParameter(
                "batch_id",
                "STRING",
                batch_id,
            ),
        ]
    )

    client.query(
        publication_sql,
        job_config=job_config,
        location=BQ_LOCATION,
    ).result()

    print(
        "Atomic production publication "
        f"completed for batch {batch_id}"
    )

def main():
    connection = get_snowflake_connection()
    batch_id = str(uuid4())
    print("Batch ID:", batch_id)

    try:
        print_snowflake_identity(connection)

        inspect_and_validate_source_views(
            connection
        )

        extracted_models = (
            extract_source_rows(connection)
        )

    finally:
        connection.close()

    for target_name, rows in (
        extracted_models.items()
    ):
        print(
            f"Prepared {target_name}: "
            f"{len(rows)} "
            f"BigQuery-compatible rows"
        )

    bigquery_client = get_bigquery_client()

    staging_tables = (
        load_bigquery_staging_tables(
            bigquery_client,
            extracted_models,
        )
    )

    print("Staging load completed")
    print(
        "Staging tables:",
        staging_tables,
    )

    validate_bigquery_staging_tables(
        bigquery_client,
        staging_tables,
        extracted_models,
    )

    print(
        "All BigQuery staging "
        "validations passed"
    )

    production_tables = (
        ensure_bigquery_production_tables(
            bigquery_client,
            staging_tables,
        )
    )

    print(
        "Production tables:",
        production_tables,
    )
    status_table_id = (
        ensure_bigquery_sync_status_table(
            bigquery_client
        )
    )
    publish_bigquery_production_tables(
        bigquery_client,
        staging_tables,
        production_tables,
        status_table_id,
        batch_id,
    )

if __name__ == "__main__":
    main()