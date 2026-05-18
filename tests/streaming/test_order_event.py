"""Tests for OrderEvent Pydantic schema."""

import json

import pytest
from pydantic import ValidationError

from src.streaming.schemas.order_event import OrderEvent


class TestOrderEventSchema:
    def _make_event(self, event_type="ORDER_CREATED", **kwargs) -> OrderEvent:
        defaults = {"OrderID": 1, "CustomerID": "ALFKI"}
        defaults.update(kwargs)
        return OrderEvent(event_type=event_type, payload=defaults)

    # ------------------------------------------------------------------
    # Serialization / Deserialization
    # ------------------------------------------------------------------

    def test_serialize_deserialize_roundtrip(self):
        event = self._make_event()
        raw = event.to_json()
        restored = OrderEvent.from_json(raw)
        assert restored.event_id == event.event_id
        assert restored.event_type == event.event_type
        assert restored.payload == event.payload

    def test_to_json_returns_bytes(self):
        event = self._make_event()
        assert isinstance(event.to_json(), bytes)

    def test_to_json_is_valid_utf8(self):
        event = self._make_event()
        raw = event.to_json()
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["event_type"] == "ORDER_CREATED"

    def test_from_json_raises_on_bad_type(self):
        event = self._make_event()
        data = json.loads(event.to_json())
        data["event_type"] = "INVALID_TYPE"
        with pytest.raises(ValidationError):
            OrderEvent.from_json(json.dumps(data).encode())

    # ------------------------------------------------------------------
    # Default fields
    # ------------------------------------------------------------------

    def test_event_id_is_uuid_string(self):
        import uuid
        event = self._make_event()
        uuid.UUID(event.event_id)  # raises if not valid UUID

    def test_event_timestamp_has_timezone(self):
        from datetime import timezone
        event = self._make_event()
        assert event.event_timestamp.tzinfo is not None
        assert event.event_timestamp.tzinfo == timezone.utc

    def test_source_defaults_to_northwind_simulator(self):
        event = self._make_event()
        assert event.source == "northwind_simulator"

    def test_two_events_have_different_ids(self):
        e1 = self._make_event()
        e2 = self._make_event()
        assert e1.event_id != e2.event_id

    # ------------------------------------------------------------------
    # Literal event_type validation
    # ------------------------------------------------------------------

    def test_order_updated_accepted(self):
        event = self._make_event(event_type="ORDER_UPDATED")
        assert event.event_type == "ORDER_UPDATED"

    def test_order_deleted_accepted(self):
        event = self._make_event(event_type="ORDER_DELETED")
        assert event.event_type == "ORDER_DELETED"

    def test_payload_allows_none_values(self):
        event = OrderEvent(
            event_type="ORDER_CREATED",
            payload={"OrderID": 1, "ShippedDate": None},
        )
        assert event.payload["ShippedDate"] is None
        restored = OrderEvent.from_json(event.to_json())
        assert restored.payload["ShippedDate"] is None
