"""
Pydantic schema for Kafka order events.

Topic: orders_raw
Format: JSON (UTF-8 encoded bytes)
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


class OrderEvent(BaseModel):
    """Single order event published to Kafka topic 'orders_raw'.

    Args:
        event_id:        UUID v4 string.
        event_timestamp: UTC timestamp of when the event was created.
        event_type:      One of ORDER_CREATED, ORDER_UPDATED, ORDER_DELETED.
        payload:         Dict with order fields (OrderID, CustomerID, …).
        source:          Identifier of the producing system.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    event_type: Literal["ORDER_CREATED", "ORDER_UPDATED", "ORDER_DELETED"]
    payload: dict[str, Any]
    source: str = "northwind_simulator"

    def to_json(self) -> bytes:
        """Serialize the event to UTF-8 JSON bytes for Kafka.

        Returns:
            JSON-encoded bytes.
        """
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> "OrderEvent":
        """Deserialize an OrderEvent from Kafka message bytes.

        Args:
            data: UTF-8 JSON bytes from a Kafka message value.

        Returns:
            OrderEvent instance.

        Raises:
            ValidationError: If the JSON does not match the schema.
        """
        return cls.model_validate_json(data.decode("utf-8"))
