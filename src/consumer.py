#!/usr/bin/env python3
"""Simple Kafka consumer to print clickstream events to the console."""
import os
import json
from kafka import KafkaConsumer


def main():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "clickstream")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
    )

    print(f"Listening to topic '{topic}' on {bootstrap} (Ctrl-C to stop)")
    try:
        for msg in consumer:
            print(json.dumps(msg.value, ensure_ascii=False))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
