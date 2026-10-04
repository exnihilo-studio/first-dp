import pendulum
from airflow.decorators import dag, task

# Values come from data_product_definition.yml / the generated Terraform in out/.
BUCKET_NAME = "gn-test-bucket"
JOB_PROJECT_ID = (
    "gn-first-dp-dev-ens"  # DP project: BigQuery load jobs run (and are billed) here
)
BQ_TABLE = "gn-dev-dwh-ens.dev_first_dp.first_table"  # project.dataset.table, owned by the DWH project
# Only the data product's SA can write to the bucket and load into BigQuery. The Composer SA is
# allowed to impersonate it, so every task impersonates it instead of using the Composer SA directly.
DP_SERVICE_ACCOUNT = (
    "first-dp-dev-composer-sa@gn-first-dp-dev-ens.iam.gserviceaccount.com"
)
SCOPES = [
    "https://www.googleapis.com/auth/devstorage.read_write",
    "https://www.googleapis.com/auth/bigquery",
]

# Same 3 columns as the `first_table` schema; created_at is a TIMESTAMP.
CSV_CONTENT = (
    "id,name,created_at\n1,alice,2026-01-01T10:00:00Z\n2,bob,2026-01-02T11:30:00Z\n"
)


def _dp_credentials():
    import google.auth
    from google.auth import impersonated_credentials

    source_credentials, _ = google.auth.default()
    return impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=DP_SERVICE_ACCOUNT,
        target_scopes=SCOPES,
    )


@task()
def write_csv_to_gcs(**context) -> str:
    from google.cloud import storage

    object_name = f"first_table/{context['ds_nodash']}/first_table.csv"
    client = storage.Client(project=JOB_PROJECT_ID, credentials=_dp_credentials())
    client.bucket(BUCKET_NAME).blob(object_name).upload_from_string(
        CSV_CONTENT, content_type="text/csv"
    )

    uri = f"gs://{BUCKET_NAME}/{object_name}"
    print(f"Wrote {uri}")
    return uri


@task()
def load_csv_into_bigquery(gcs_uri: str) -> int:
    from google.cloud import bigquery

    client = bigquery.Client(project=JOB_PROJECT_ID, credentials=_dp_credentials())
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=[
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        ],
        # Truncate so re-running the DAG doesn't duplicate rows.
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
    )

    load_job = client.load_table_from_uri(gcs_uri, BQ_TABLE, job_config=job_config)
    load_job.result()

    rows = client.get_table(BQ_TABLE).num_rows
    print(f"Loaded {gcs_uri} into {BQ_TABLE}, table now has {rows} rows")
    return rows


@dag(
    dag_id="csv_to_gcs_to_bigquery",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    description="Writes a small CSV to the DP's GCS bucket, then loads it into BigQuery",
    schedule=None,
    catchup=False,
    tags=["genesis", "example", "gcs", "bigquery"],
)
def csv_to_gcs_to_bigquery_dag():
    load_csv_into_bigquery(write_csv_to_gcs())


dag = csv_to_gcs_to_bigquery_dag()
