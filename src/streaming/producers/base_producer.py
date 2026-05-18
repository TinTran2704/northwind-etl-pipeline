"""
Base Kafka producer — abstract wrapper over confluent_kafka.Producer.

Handles delivery callbacks, logging, flush, and close.
Bootstrap servers default to env KAFKA_BOOTSTRAP_SERVERS_HOST (localhost:9092).
"""

import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

from confluent_kafka import KafkaException, Producer

logger = logging.getLogger(__name__)

_DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092")


class BaseKafkaProducer(ABC):
    """Abstract Kafka producer with delivery logging and lifecycle management.

    Args:
        bootstrap_servers: Kafka broker address(es), e.g. ``"localhost:9092"``.
        topic:             Default topic to publish messages to.
        client_id:         Kafka client identifier (shown in broker logs).
    """

    def __init__(
        self,
        bootstrap_servers: str = _DEFAULT_BOOTSTRAP,
        topic: str = "",
        client_id: str = "etl-producer",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client_id = client_id
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
            }
        )
        self._delivered = 0
        self._failed = 0
        logger.info(
            "[PRODUCER] client=%s topic=%s broker=%s",
            client_id, topic, bootstrap_servers,
        )

    # ------------------------------------------------------------------
    # Internal delivery callback
    # ------------------------------------------------------------------

    def _on_delivery(self, err, msg) -> None:  # type: ignore[type-arg]
        """Called by librdkafka on message delivery (success or failure)."""
        if err:
            self._failed += 1
            logger.error(
                "[PRODUCER] delivery FAILED topic=%s key=%s error=%s",
                msg.topic(), msg.key(), err,
            )
        else:
            self._delivered += 1
            logger.debug(
                "[PRODUCER] delivery OK topic=%s partition=%d offset=%d key=%s",
                msg.topic(), msg.partition(), msg.offset(), msg.key(),
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(self, key: str, value: bytes, topic: Optional[str] = None) -> None:
        """Enqueue a message for async delivery.

        Args:
            key:   Message key (used for partitioning).
            value: Message payload bytes.
            topic: Override the default topic.

        Raises:
            KafkaException: On producer queue overflow.
        """
        target_topic = topic or self.topic
        try:
            self._producer.produce(
                topic=target_topic,
                key=key.encode("utf-8") if isinstance(key, str) else key,
                value=value,
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)  # trigger pending callbacks without blocking
        except KafkaException as exc:
            logger.error("[PRODUCER] produce error key=%s: %s", key, exc)
            raise

    def flush(self, timeout: float = 10.0) -> int:
        """Flush all queued messages and wait for delivery.

        Args:
            timeout: Seconds to wait for outstanding deliveries.

        Returns:
            Number of messages still pending (0 means all delivered).
        """
        pending = self._producer.flush(timeout)
        if pending:
            logger.warning("[PRODUCER] flush: %d messages still pending after %.1fs", pending, timeout)
        else:
            logger.info(
                "[PRODUCER] flush complete — delivered=%d failed=%d",
                self._delivered, self._failed,
            )
        return pending

    def close(self) -> None:
        """Flush and release producer resources."""
        self.flush()
        logger.info("[PRODUCER] closed — total delivered=%d failed=%d", self._delivered, self._failed)
