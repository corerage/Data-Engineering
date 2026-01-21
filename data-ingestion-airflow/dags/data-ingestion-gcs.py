import os
import logging
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
dataset_file = "yellow_tripdata_2021-01.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace(".csv.gz", ".parquet")
BIG_QUERY_DATASET = os.environ.get("BIG_QUERY_DATASET", "example_dataset")


def convert_csv_to_parquet(source_file: str, dest_file: str):
    """Convert CSV to Parquet format."""
    import pyarrow.csv as cv
    import pyarrow.parquet as pq

    if not source_file.endswith(".csv.gz") and not source_file.endswith(".csv"):
        logging.error("Only csv.gz or csv files can be converted to parquet")
        return

    tb = cv.read_csv(source_file)
    pq.write_table(tb, dest_file)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    from google.cloud import storage

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    logging.info(f"File {source_file_name} uploaded to {destination_blob_name}.")


def create_bigquery_external_table(project_id, dataset_id, bucket, parquet_file):
    """Create BigQuery external table using google-cloud-bigquery library."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset_id}.external_table"

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [f"gs://{bucket}/parquet/{parquet_file}"]

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config

    # Create or replace the table
    table = client.create_table(table, exists_ok=True)

    logging.info(f"Created external table {table_id}")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=convert_csv_to_parquet,
        op_kwargs={
            "source_file": f"{path_to_local_home}/{dataset_file}",
            "dest_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_blob,
        op_kwargs={
            "bucket_name": BUCKET,
            "source_file_name": f"{path_to_local_home}/{parquet_file}",
            "destination_blob_name": f"parquet/{parquet_file}",
        },
    )

    big_query_external_table_task = PythonOperator(
        task_id="big_query_external_table_task",
        python_callable=create_bigquery_external_table,
        op_kwargs={
            "project_id": PROJECT_ID,
            "dataset_id": BIG_QUERY_DATASET,
            "bucket": BUCKET,
            "parquet_file": parquet_file,
        },
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> big_query_external_table_task
    )
