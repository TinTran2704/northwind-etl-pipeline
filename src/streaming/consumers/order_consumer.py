"""
Order consumer — reads ORDER_CREATED events from Kafka and flushes to Parquet.

Topic: orders_raw
Group: etl_order_consumer_group
Spec: docs/12-phase2-kafka-spark.md §12.7
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from src.streaming.consumers.base_consumer import BaseKafkaConsumer, _DEFAULT_BOOTSTRAP
from src.streaming.schemas.order_event import OrderEvent

logger = logging.getLogger(__name__)


class OrderConsumer(BaseKafkaConsumer):
    """Consume order events from 'orders_raw' and persist batches as Parquet.

    Args:
        bootstrap_servers: Kafka broker address (default from env).
        output_dir:        Directory where Parquet files are written.
    """

    topic = "orders_raw"
    group_id = "etl_order_consumer_group"

    def __init__(
        self,
        bootstrap_servers: str = _DEFAULT_BOOTSTRAP,
    ) -> None:
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic=self.topic,
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )

    def consume_to_parquet(
        self,
        output_dir: Path,
        batch_size: int = 100,
        max_batches: Optional[int] = None,
    ) -> int:
        """Consume messages and flush each batch to a timestamped Parquet file.

        File naming: ``orders_YYYY-MM-DD-HHMMSS_<batch_num>.parquet``

        Args:
            output_dir:  Directory to write Parquet files (created if missing).
            batch_size:  Number of messages per Parquet file.
            max_batches: Stop after this many batches (``None`` = run until
                         empty topic / EOF).

        Returns:
            Total number of messages written.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        total_written = 0
        batch_num = 0

        logger.info(
            "[ORDER_CONSUMER] starting — output_dir=%s batch_size=%d max_batches=%s",
            output_dir, batch_size, max_batches,
        )

        while max_batches is None or batch_num < max_batches:
            batch = self.consume_batch(batch_size=batch_size, max_wait_sec=30.0)
            if not batch:
                logger.info("[ORDER_CONSUMER] no messages received — stopping")
                break

            records = []
            for item in batch:
                try:
                    event = OrderEvent.from_json(item["value"])
                    row = {
                        "event_id": event.event_id,
                        "event_timestamp": event.event_timestamp.isoformat(),
                        "event_type": event.event_type,
                        "source": event.source,
                        **event.payload,
                    }
                    records.append(row)
                except Exception as exc:
                    logger.warning(
                        "[ORDER_CONSUMER] failed to parse message offset=%s: %s",
                        item.get("offset"), exc,
                    )

            if not records:
                continue

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
            batch_num += 1
            out_path = output_dir / f"orders_{ts}_{batch_num:04d}.parquet"
            df = pd.DataFrame(records)
            df.to_parquet(out_path, index=False)
            total_written += len(records)

            logger.info(
                "[ORDER_CONSUMER] batch %d — wrote %d rows → %s",
                batch_num, len(records), out_path,
            )
            print(f"[ORDER_CONSUMER] Batch {batch_num} - {len(records)} rows -> {out_path.name}")

        logger.info("[ORDER_CONSUMER] finished — total_written=%d", total_written)
        return total_written
