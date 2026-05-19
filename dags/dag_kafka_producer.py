"""
DAG: northwind_kafka_producer

Manually-triggered simulator that publishes Northwind order records
to the 'orders_raw' Kafka topic.

Tasks:
    produce_orders — reads seed CSV, publishes 100 ORDER_CREATED events

Schedule:   None (manual trigger only)
Tags:       kafka, streaming, phase2
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
_PRODUCE_LIMIT = 100
_DATA_DIR = Path(os.getenv("ETL_DATA_DIR", "/opt/etl/data"))


def _produce_orders(**context) -> None:
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    log = logging.getLogger(__name__)

    from src.streaming.producers.order_producer import OrderProducer

    seed_path = _DATA_DIR / "seed" / "northwind" / "orders.csv"
    producer = OrderProducer(bootstrap_servers=_KAFKA_BOOTSTRAP, seed_path=seed_path)
    try:
        n = producer.produce_from_seed(delay_range=(0.0, 0.0), limit=_PRODUCE_LIMIT)
        log.info("[DAG kafka_producer] produced=%d", n)
    finally:
        producer.close()


with DAG(
    dag_id="northwind_kafka_producer",
    description="Simulate Northwind order events → Kafka topic orders_raw",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "etl",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["kafka", "streaming", "phase2"],
) as dag:

    produce_orders = PythonOperator(
        task_id="produce_orders",
        python_callable=_produce_orders,
    )
