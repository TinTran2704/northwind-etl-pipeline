"""
DAG: northwind_streaming_consumer

Polls 'orders_raw' Kafka topic every 5 minutes and flushes received
messages to Parquet files under data/raw/streaming/orders/.

Tasks:
    consume_orders — consume up to 5 batches × 200 messages → Parquet

Schedule:   */5 * * * *
Catchup:    False
Tags:       kafka, streaming, phase2
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
_DATA_DIR = Path(os.getenv("ETL_DATA_DIR", "/opt/etl/data"))
_OUTPUT_DIR = _DATA_DIR / "raw" / "streaming" / "orders"
_BATCH_SIZE = 200
_MAX_BATCHES = 5


def _consume_orders(**context) -> None:
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
        log.info("[DAG streaming_consumer] total_written=%d", written)
    finally:
        consumer.close()


with DAG(
    dag_id="northwind_streaming_consumer",
    description="Consume orders_raw Kafka topic → Parquet in data/raw/streaming/orders/",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "etl",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["kafka", "streaming", "phase2"],
) as dag:

    consume_orders = PythonOperator(
        task_id="consume_orders",
        python_callable=_consume_orders,
    )
