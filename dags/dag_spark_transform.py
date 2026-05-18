"""
DAG: northwind_spark_transform

Runs the Spark Clean + Deliver jobs via spark-submit inside the
etl_spark_master container (BashOperator → docker exec).

Tasks:
    spark_clean   — distributed quality screens  (clean_job.py)
    spark_deliver — distributed SK pipeline      (deliver_job.py)

Dependencies:  spark_clean >> spark_deliver
Schedule:      None (manual trigger only)

Usage:
    Trigger manually from Airflow UI or CLI:
        airflow dags trigger northwind_spark_transform
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

_SPARK_MASTER_CONTAINER = "etl_spark_master"
_SPARK_MASTER_URL = "spark://spark-master:7077"
_JDBC_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"
_ETL_SRC = "/opt/etl/src"
_RAW_DIR = "/opt/etl/data/raw"
_STAGING_DIR = "/opt/etl/data/staging/spark"

with DAG(
    dag_id="northwind_spark_transform",
    description="Spark Clean + Deliver pipeline (replaces Pandas phases)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["spark", "etl", "phase2"],
) as dag:

    spark_clean = BashOperator(
        task_id="spark_clean",
        bash_command=(
            "docker exec {{ params.container }} "
            "spark-submit "
            "--master {{ params.master }} "
            "--jars {{ params.jar }} "
            "{{ params.src }}/spark_jobs/clean_job.py "
            "{{ run_id }} "
            "{{ params.raw_dir }}/northwind/$(ls -t {{ params.raw_dir }}/northwind | head -1) "
            "{{ params.staging_dir }}"
        ),
        params={
            "container": _SPARK_MASTER_CONTAINER,
            "master":    _SPARK_MASTER_URL,
            "jar":       _JDBC_JAR,
            "src":       _ETL_SRC,
            "raw_dir":   _RAW_DIR,
            "staging_dir": _STAGING_DIR,
        },
    )

    spark_deliver = BashOperator(
        task_id="spark_deliver",
        bash_command=(
            "docker exec {{ params.container }} "
            "spark-submit "
            "--master {{ params.master }} "
            "--jars {{ params.jar }} "
            "{{ params.src }}/spark_jobs/deliver_job.py "
            "{{ run_id }} "
            "{{ params.staging_dir }}"
        ),
        params={
            "container":   _SPARK_MASTER_CONTAINER,
            "master":      _SPARK_MASTER_URL,
            "jar":         _JDBC_JAR,
            "src":         _ETL_SRC,
            "staging_dir": _STAGING_DIR,
        },
    )

    spark_clean >> spark_deliver
