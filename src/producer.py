#!/usr/bin/env python3
"""Simple Kafka clickstream event producer.

Generates random e-commerce-like events and publishes them to a Kafka topic.
"""
import os
import time
import json
import random
import uuid
import argparse
import datetime
from kafka import KafkaProducer


def json_serializer(v):
    return json.dumps(v).encode("utf-8")


EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout",
    "purchase",
    "click",
    "search",
]

PRODUCTS = [f"prod-{i:04d}" for i in range(1, 501)]
PAGES = ["home", "category", "product", "cart", "checkout", "search", "landing"]
REFERRERS = ["google", "facebook", "twitter", "email", "direct", "ad", "affiliate", "organic"]


def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1,2000)}",
        "session_id": str(uuid.uuid4()),
        "event_type": random.choices(EVENT_TYPES, weights=[40, 20, 10, 5, 2, 2, 15, 6])[0],
        "product_id": random.choice(PRODUCTS),
        "price": round(random.uniform(5, 500), 2),
        "page": random.choice(PAGES),
        "referrer": random.choice(REFERRERS),
        "ts": datetime.datetime.utcnow().isoformat() + "Z",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "clickstream"))
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--count", type=int, default=0, help="total events, 0=forever")
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=json_serializer)
    sent = 0
    interval = 1.0 / args.rate if args.rate and args.rate > 0 else 0.2

    try:
        while True:
            ev = make_event()
            producer.send(args.topic, ev)
            sent += 1
            if sent % 100 == 0:
                producer.flush()
            if args.count and sent >= args.count:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"Sent {sent} events to {args.topic} on {args.bootstrap}")


if __name__ == "__main__":
    main()
