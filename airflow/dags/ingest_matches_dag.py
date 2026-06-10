import json
import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

logger = logging.getLogger(__name__)

# ─── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "teeradon",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ─── DAG ──────────────────────────────────────────────────
with DAG(
    dag_id="ingest_football_matches",
    description="Ingest football matches from API to GCS Raw Zone",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["football", "ingestion", "phase1"],
) as dag:
    def fetch_matches(**context) -> None:
        api_key = Variable.get("football_api_key")
        execution_date = context["ds"]

        sys.path.insert(0, "/opt/airflow")
        from ingestion.football_api_client import FootballAPIClient

        client = FootballAPIClient(api_key=api_key)

        # ดึงทั้ง season แทนแค่วันเดียว
        raw_data = client.get_matches(
            competition="PL"
            # ไม่ใส่ date_from / date_to = ดึงทั้ง season
        )

        local_path = f"/tmp/matches_{execution_date}.json"

        with open(local_path, "w") as f:
            json.dump({
                "ingested_at": datetime.utcnow().isoformat(),
                "execution_date": execution_date,
                "source": "football-data.org",
                "competition": "PL",
                "data": raw_data
            }, f, indent=2)

        match_count = len(raw_data.get("matches", []))
        logger.info(f"Saved {match_count} matches to {local_path}")

        context["ti"].xcom_push(key="local_path", value=local_path)
        context["ti"].xcom_push(key="execution_date", value=execution_date)

    def upload_to_gcs(**context) -> None:
        from google.cloud import storage

        ti = context["ti"]
        local_path = ti.xcom_pull(
            task_ids="fetch_matches_from_api",
            key="local_path"
        )
        execution_date = ti.xcom_pull(
            task_ids="fetch_matches_from_api",
            key="execution_date"
        )

        bucket_name = Variable.get("gcs_raw_bucket")
        project_id = Variable.get("gcp_project_id")   # ← ดึงจาก Variable

        year, month, day = execution_date.split("-")
        gcs_path = f"matches/year={year}/month={month}/day={day}/matches.json"

        # ส่ง project ตรงๆ — ไม่รอ auto detect
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)

        logger.info(f"Uploaded to gs://{bucket_name}/{gcs_path}")

        os.remove(local_path)
        logger.info(f"Cleaned up local file: {local_path}")

    def download_from_gcs(**context) -> None:
        """Download raw JSON จาก GCS มาไว้ที่ /tmp"""
        from google.cloud import storage

        ti = context["ti"]
        execution_date = ti.xcom_pull(
            task_ids="fetch_matches_from_api",
            key="execution_date"
        )

        bucket_name = Variable.get("gcs_raw_bucket")
        project_id = Variable.get("gcp_project_id")

        year, month, day = execution_date.split("-")
        gcs_path = f"matches/year={year}/month={month}/day={day}/matches.json"
        local_path = f"/tmp/matches_{execution_date}.json"

        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        blob.download_to_filename(local_path)

        logger.info(f"Downloaded to {local_path}")
        ti.xcom_push(key="local_json_path", value=local_path)
        ti.xcom_push(key="local_parquet_path", value=f"/tmp/matches_parquet_{execution_date}")


    def upload_parquet_to_gcs(**context) -> None:
        """Upload Parquet files ขึ้น GCS Processed Zone"""
        from google.cloud import storage
        import os

        ti = context["ti"]
        execution_date = ti.xcom_pull(
            task_ids="fetch_matches_from_api",
            key="execution_date"
        )
        local_parquet_path = ti.xcom_pull(
            task_ids="download_raw_from_gcs",
            key="local_parquet_path"
        )

        bucket_name = Variable.get("gcs_processed_bucket")
        project_id = Variable.get("gcp_project_id")

        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)

        # Upload ทุกไฟล์ใน parquet directory
        for root, dirs, files in os.walk(local_parquet_path):
            for file in files:
                local_file = os.path.join(root, file)
                # สร้าง GCS path จาก local path
                relative_path = os.path.relpath(local_file, "/tmp")
                gcs_path = f"matches/{relative_path}"

                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_file)
                logger.info(f"Uploaded {gcs_path}")

    def run_spark_job(**context) -> None:
        """รัน Spark job ใน Airflow container"""
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        import sys

        ti = context["ti"]
        local_json_path = ti.xcom_pull(
            task_ids="download_raw_from_gcs",
            key="local_json_path"
        )
        local_parquet_path = ti.xcom_pull(
            task_ids="download_raw_from_gcs",
            key="local_parquet_path"
        )

        # Import Spark functions จาก jobs folder
        sys.path.insert(0, "/opt/airflow")
        from spark.jobs.clean_matches import flatten_matches, add_derived_columns, validate_data

        spark = SparkSession.builder \
            .appName("FootballMatchesCleaner") \
            .master("local[*]") \
            .getOrCreate()

        df_raw = spark.read.option("multiline", "true").json(local_json_path)
        df_flat = flatten_matches(df_raw)
        df_final = add_derived_columns(df_flat)

        if not validate_data(df_final):
            raise ValueError("Data validation failed")

        df_final.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(local_parquet_path)

        logger.info(f"✅ Spark job completed — output: {local_parquet_path}")
        spark.stop()

    def load_to_bigquery(**context) -> None:
        from google.cloud import bigquery, storage

        ti = context["ti"]
        execution_date = ti.xcom_pull(
            task_ids="fetch_matches_from_api",
            key="execution_date"
        )

        project_id = Variable.get("gcp_project_id")
        processed_bucket = Variable.get("gcs_processed_bucket")

        # List parquet files จาก GCS
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(processed_bucket)
        prefix = f"matches/matches_parquet_{execution_date}/"

        # หาทุก .parquet files
        blobs = bucket.list_blobs(prefix=prefix)
        parquet_uris = [
            f"gs://{processed_bucket}/{blob.name}"
            for blob in blobs
            if blob.name.endswith(".parquet")
        ]

        if not parquet_uris:
            raise ValueError(f"No parquet files found at {prefix}")

        logger.info(f"Found {len(parquet_uris)} parquet files")

        # Load เข้า BigQuery
        bq_client = bigquery.Client(project=project_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        table_id = f"{project_id}.football_staging.raw_matches"

        load_job = bq_client.load_table_from_uri(
            parquet_uris,
            table_id,
            job_config=job_config,
        )

        load_job.result()

        table = bq_client.get_table(table_id)
        logger.info(f"✅ Loaded {table.num_rows} rows to {table_id}")

# ─── Tasks ────────────────────────────────────────────
    task_fetch = PythonOperator(
        task_id="fetch_matches_from_api",
        python_callable=fetch_matches,
    )

    task_upload_raw = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    task_download_raw = PythonOperator(
        task_id="download_raw_from_gcs",
        python_callable=download_from_gcs,
    )

    task_spark = PythonOperator(
        task_id="spark_clean_matches",
        python_callable=run_spark_job,
    )

    task_upload_parquet = PythonOperator(
        task_id="upload_parquet_to_gcs",
        python_callable=upload_parquet_to_gcs,
    )

    task_load_bq = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    # ─── Dependencies ──────────────────────────────────────
    task_fetch >> task_upload_raw >> task_download_raw >> task_spark >> task_upload_parquet >> task_load_bq
