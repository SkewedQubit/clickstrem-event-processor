import json
import re
from src.producer import make_event, json_serializer, EVENT_TYPES, PRODUCTS, PAGES


def test_make_event_keys_and_types():
    ev = make_event()
    assert isinstance(ev, dict)
    # required keys
    for k in ("event_id", "user_id", "session_id", "event_type", "product_id", "ts"):
        assert k in ev


def test_event_fields_valid():
    ev = make_event()
    assert ev["event_type"] in EVENT_TYPES
    assert ev["product_id"] in PRODUCTS
    assert ev["page"] in PAGES
    # price is numeric
    assert isinstance(ev["price"], float)
    # timestamp is ISO-like
    assert re.match(r"\d{4}-\d{2}-\d{2}T", ev["ts"])


def test_json_serializer_roundtrip():
    ev = make_event()
    b = json_serializer(ev)
    assert isinstance(b, (bytes, bytearray))
    parsed = json.loads(b.decode("utf-8"))
    # event ids and types roundtrip
    assert parsed["event_id"] == ev["event_id"]
    assert parsed["event_type"] == ev["event_type"]
