import json


def test_consumer_value_deserializer_roundtrip():
    payload = {"x": 1, "y": "z"}
    b = json.dumps(payload).encode("utf-8")
    deserializer = lambda m: json.loads(m.decode("utf-8"))
    assert deserializer(b) == payload
