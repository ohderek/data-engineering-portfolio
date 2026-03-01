#!/usr/bin/env python3
"""
order_producer.py
─────────────────────────────────────────────────────────────────────────────
Simulates a continuous stream of e-commerce order events and publishes them
to a Kafka topic as JSON.

Events are semi-realistic:
  - Product prices have ±15% variance to mimic promotions / dynamic pricing
  - Region traffic follows a weighted distribution (us-east > us-west > eu)
  - Event type ratio is 3:1:1 (ORDER_PLACED : ORDER_CANCELLED : ORDER_UPDATED)

Usage:
    python order_producer.py --topic order-events --rate 5
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"

PRODUCTS = [
    {"id": "PROD-001", "name": "Wireless Headphones", "category": "Electronics", "base_price": 89.99},
    {"id": "PROD-002", "name": "Running Shoes",        "category": "Apparel",     "base_price": 119.95},
    {"id": "PROD-003", "name": "Coffee Maker",         "category": "Appliances",  "base_price": 54.99},
    {"id": "PROD-004", "name": "Yoga Mat",             "category": "Sports",      "base_price": 29.99},
    {"id": "PROD-005", "name": "Desk Lamp",            "category": "Home",        "base_price": 39.95},
    {"id": "PROD-006", "name": "Protein Powder",       "category": "Health",      "base_price": 44.99},
    {"id": "PROD-007", "name": "Gaming Mouse",         "category": "Electronics", "base_price": 59.99},
    {"id": "PROD-008", "name": "Water Bottle",         "category": "Sports",      "base_price": 19.99},
]

REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-southeast"]
# Weighted to mirror realistic traffic skew — us-east is the primary market
REGION_WEIGHTS = [0.35, 0.25, 0.20, 0.12, 0.08]

# 3:1:1 ratio — most events are placements; cancellations and updates are rarer
EVENT_TYPES = [
    "ORDER_PLACED", "ORDER_PLACED", "ORDER_PLACED",
    "ORDER_CANCELLED",
    "ORDER_UPDATED",
]


# ── Topic setup ───────────────────────────────────────────────────────────────

def ensure_topic(topic: str, num_partitions: int = 3) -> None:
    """Create the Kafka topic if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=5).topics

    if topic in existing:
        print(f"[kafka]    Topic '{topic}' exists ({len(existing[topic].partitions)} partitions)")
        return

    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=num_partitions, replication_factor=1)]
    )
    for t, fut in futures.items():
        try:
            fut.result()
            print(f"[kafka]    Created topic '{t}' ({num_partitions} partitions)")
        except Exception as exc:
            # Race condition — another process may have created it between the
            # list call and this create call; treat as non-fatal.
            print(f"[kafka]    Note: {exc}")


# ── Event generation ──────────────────────────────────────────────────────────

def generate_event() -> dict:
    """Return one realistic order event dict."""
    product    = random.choice(PRODUCTS)
    quantity   = random.randint(1, 4)
    unit_price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)

    return {
        "order_id":         str(uuid.uuid4()),
        "customer_id":      f"CUST-{random.randint(1000, 9999)}",
        "product_id":       product["id"],
        "product_name":     product["name"],
        "product_category": product["category"],
        "quantity":         quantity,
        "unit_price":       unit_price,
        "total_price":      round(unit_price * quantity, 2),
        "event_type":       random.choice(EVENT_TYPES),
        "region":           random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0],
        # event_timestamp is the business time embedded in the payload.
        # Spark uses this for watermarking, NOT the Kafka broker timestamp.
        "event_timestamp":  datetime.now(timezone.utc).isoformat(),
    }


# ── Producer loop ─────────────────────────────────────────────────────────────

def on_delivery(err, msg) -> None:
    if err:
        print(f"\n[error]    Delivery failed for {msg.key()}: {err}")


def main(topic: str, rate: int) -> None:
    ensure_topic(topic)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"[producer] Publishing {rate} event(s)/sec to '{topic}'. Ctrl+C to stop.\n")

    count = 0
    try:
        while True:
            for _ in range(rate):
                event = generate_event()
                producer.produce(
                    topic,
                    # Partition by order_id so all events for one order land
                    # on the same partition, preserving per-order ordering.
                    key=event["order_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                count += 1

            # Poll the delivery report queue — keeps it from growing unbounded
            producer.poll(0)
            print(f"\r[producer] {count:,} events published", end="", flush=True)
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[producer] Stopping. Flushing in-flight messages...")
        producer.flush()
        print(f"[producer] Done. Total published: {count:,}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order event Kafka producer")
    parser.add_argument("--topic", default="order-events", help="Kafka topic name")
    parser.add_argument("--rate",  default=5, type=int,    help="Events per second")
    args = parser.parse_args()

    main(args.topic, args.rate)
