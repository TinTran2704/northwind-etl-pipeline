"""
DAG: northwind_streaming_e2e

End-to-end streaming pipeline:
    produce_orders → wait_after_produce → consume_orders → spark_clean → spark_deliver

A TimeDeltaSensor holds execution for 30 s after the DAG run start so
that all producer messages have been flushed to Kafka before the consumer
starts polling.

Schedule:   None (manual trigger only)
Tags:       kafka, spark, streaming, e2e, phase2
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
_DATA_DIR = Path(os.getenv("ETL_DATA_DIR", "/opt/etl/data"))
_OUTPUT_DIR = _DATA_DIR / "raw" / "streaming" / "orders"
_PRODUCE_LIMIT = 100
_BATCH_SIZE = 100
_MAX_BATCHES = 1

# Spark / docker settings (reuse from dag_spark_transform)
_SPARK_MASTER_CONTAINER = "etl_spark_master"
_SPARK_MASTER_URL = "spark://spark-master:7077"
_JDBC_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"
_ETL_SRC = "/opt/etl/src"
_RAW_DIR = "/opt/etl/data/raw"
_STAGING_DIR = "/opt/etl/data/staging/spark"


def _produce_orders(**_context) -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    log = logging.getLogger(__name__)

    from src.streaming.producers.order_producer import OrderProducer

    seed_path = _DATA_DIR / "seed" / "northwind" / "orders.csv"
    producer = OrderProducer(bootstrap_servers=_KAFKA_BOOTSTRAP, seed_path=seed_path)
    try:
        n = producer.produce_from_seed(delay_range=(0.0, 0.0), limit=_PRODUCE_LIMIT)
        log.info("[DAG streaming_e2e] produced=%d", n)
    finally:
        producer.close()


def _consume_orders(**_context) -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    log = logging.getLogger(__name__)

    from src.streaming.consumers.order_consumer import OrderConsumer

    consumer = OrderConsumer(bootstrap_servers=_KAFKA_BOOTSTRAP)
    try:
        written = consumer.consume_to_parquet(
            output_dir=_OUTPUT_DIR,
            batch_size=_BATCH_SIZE,
            max_batches=_MAX_BATCHES,
        )
        log.info("[DAG streaming_e2e] consumed=%d", written)
    finally:
        consumer.close()


with DAG(
    dag_id="northwind_streaming_e2e",
    description="E2E streaming pipeline: produce → wait → consume → Spark clean → Spark deliver",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "etl",
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["kafka", "spark", "streaming", "e2e", "phase2"],
) as dag:

    produce_orders = PythonOperator(
        task_id="produce_orders",
        python_callable=_produce_orders,
    )

    # Wait 30 s after DAG run start to let producer flush before consumer polls
    wait_after_produce = TimeDeltaSensor(
        task_id="wait_after_produce",
        delta=timedelta(seconds=30),
        poke_interval=5,
    )

    consume_orders = PythonOperator(
        task_id="consume_orders",
        python_callable=_consume_orders,
    )

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
            "container":   _SPARK_MASTER_CONTAINER,
            "master":      _SPARK_MASTER_URL,
            "jar":         _JDBC_JAR,
            "src":         _ETL_SRC,
            "raw_dir":     _RAW_DIR,
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

    produce_orders >> wait_after_produce >> consume_orders >> spark_clean >> spark_deliver
