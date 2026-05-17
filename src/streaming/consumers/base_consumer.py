"""
Base Kafka consumer — abstract wrapper over confluent_kafka.Consumer.

Handles polling, batch collection, manual offset commit, and close.
Bootstrap servers default to env KAFKA_BOOTSTRAP_SERVERS_HOST (localhost:9092).
"""

import logging
import os
import time
from abc import ABC
from typing import Any, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)

_DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092")


class BaseKafkaConsumer(ABC):
    """Abstract Kafka consumer with manual commit and lifecycle management.

    Args:
        bootstrap_servers:  Kafka broker address(es), e.g. ``"localhost:9092"``.
        topic:              Topic to subscribe to.
        group_id:           Consumer group identifier.
        auto_offset_reset:  Where to start reading when no offset exists.
    """

    def __init__(
        self,
        bootstrap_servers: str = _DEFAULT_BOOTSTRAP,
        topic: str = "",
        group_id: str = "etl-consumer-group",
        auto_offset_reset: str = "earliest",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([topic])
        logger.info(
            "[CONSUMER] group=%s topic=%s broker=%s",
            group_id, topic, bootstrap_servers,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def consume(self, timeout: float = 1.0) -> Optional[Message]:
        """Poll for a single message.

        Args:
            timeout: Seconds to block waiting for a message.

        Returns:
            A :class:`confluent_kafka.Message` on success, ``None`` on timeout.

        Raises:
            KafkaException: On a fatal consumer error.
        """
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug(
                    "[CONSUMER] EOF partition=%d offset=%d",
                    msg.partition(), msg.offset(),
                )
                return None
            raise KafkaException(msg.error())
        return msg

    def consume_batch(
        self,
        batch_size: int = 100,
        max_wait_sec: float = 30.0,
    ) -> list[dict[str, Any]]:
        """Collect messages until ``batch_size`` reached or ``max_wait_sec`` elapsed.

        Commits offsets manually after collecting the batch.

        Args:
            batch_size:   Maximum number of messages per batch.
            max_wait_sec: Maximum seconds to wait for a full batch.

        Returns:
            List of dicts with keys ``key``, ``value``, ``topic``,
            ``partition``, ``offset``.
        """
        batch: list[dict[str, Any]] = []
        deadline = time.monotonic() + max_wait_sec

        while len(batch) < batch_size and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            msg = self.consume(timeout=min(1.0, remaining))
            if msg is None:
                continue
            batch.append(
                {
                    "key": msg.key(),
                    "value": msg.value(),
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }
            )

        if batch:
            self._consumer.commit(asynchronous=False)
            logger.info("[CONSUMER] batch committed — size=%d", len(batch))

        return batch

    def close(self) -> None:
        """Commit pending offsets and release consumer resources."""
        self._consumer.close()
        logger.info("[CONSUMER] group=%s closed", self.group_id)
