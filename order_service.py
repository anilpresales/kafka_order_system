"""
Order Service
=============
Entry point for the order processing system.
Accepts an order and produces to order-created topic.

In production: this would be triggered by a REST API or web frontend.
For demo: run directly with python3 order_service.py

This is the PRODUCER — equivalent to api.py in the Temporal version.
Notice what's missing vs Temporal:
  - No workflow ID tracking
  - No built-in retry
  - No state management
  - Producer fires and forgets — no way to know what happened next
"""

import json
import uuid
import logging
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config import KAFKA_CONFIG, TOPICS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ORDER-SERVICE] %(message)s"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# TOPIC SETUP
# Create all topics if they don't exist
# In production: use Terraform or Confluent Cloud UI
# ─────────────────────────────────────────────────────────────
def create_topics():
    admin = AdminClient(KAFKA_CONFIG)
    topics_to_create = [
        NewTopic(topic, num_partitions=3, replication_factor=3)
        for topic in TOPICS.values()
    ]
    futures = admin.create_topics(topics_to_create)
    for topic, future in futures.items():
        try:
            future.result()
            log.info(f"Topic created: {topic}")
        except Exception as e:
            if "already exists" in str(e).lower():
                log.info(f"Topic already exists: {topic}")
            else:
                log.error(f"Failed to create topic {topic}: {e}")


# ─────────────────────────────────────────────────────────────
# DELIVERY CALLBACK
# Called when Kafka confirms message was written
# This is the only feedback producer gets — no workflow state
# ─────────────────────────────────────────────────────────────
def delivery_callback(err, msg):
    if err:
        log.error(f"Message delivery failed: {err}")
    else:
        log.info(
            f"Message delivered to {msg.topic()} "
            f"partition [{msg.partition()}] "
            f"offset {msg.offset()}"
        )


# ─────────────────────────────────────────────────────────────
# PLACE ORDER
# Produces order event to Kafka
# Notice: after this, order_service has NO IDEA what happens next
# Compare to Temporal where workflow.run() tracks every step
# ─────────────────────────────────────────────────────────────
def place_order(order: dict) -> str:
    producer = Producer(KAFKA_CONFIG)

    # Add metadata to the order
    order["order_id"]   = order.get("order_id", f"ord-{uuid.uuid4().hex[:8]}")
    order["created_at"] = datetime.utcnow().isoformat()
    order["status"]     = "created"
    order["retry_count"] = 0   # track retries manually — Temporal does this automatically

    message = json.dumps(order)

    log.info(f"Placing order: {order['order_id']}")
    log.info(f"Order details: SKU={order['sku']} QTY={order['qty']}")

    # Produce to order-created topic
    # Key = order_id ensures all events for same order go to same partition
    # This maintains ordering — critical for order processing
    producer.produce(
        topic=TOPICS["order_created"],
        key=order["order_id"],
        value=message,
        callback=delivery_callback
    )

    producer.flush()  # wait for delivery confirmation

    log.info(f"Order {order['order_id']} submitted to Kafka")
    log.info(f"From here — order_service has NO visibility into what happens next")
    log.info(f"This is the gap Temporal fills with workflow execution history")

    return order["order_id"]


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    # Create topics first
    log.info("Setting up Kafka topics...")
    create_topics()

    # Scenario selection
    scenario = sys.argv[1] if len(sys.argv) > 1 else "success"

    if scenario == "success":
        order = {
            "order_id":           "ord-8821",
            "sku":                "sku_999",
            "qty":                5,
            "email":              "customer@example.com",
            "phone":              "+1-555-0100",
            "shipping_address":   "123 Main St Boston MA",
            "card_last4":         "4242",
            "force_fail_payment": False,
        }
    elif scenario == "payment_fail":
        order = {
            "order_id":           "ord-9999",
            "sku":                "sku_999",
            "qty":                2,
            "email":              "customer2@example.com",
            "phone":              "+1-555-0200",
            "shipping_address":   "456 Oak Ave Cambridge MA",
            "card_last4":         "0002",
            "force_fail_payment": True,
        }
    elif scenario == "out_of_stock":
        order = {
            "order_id":           "ord-1111",
            "sku":                "sku_888",
            "qty":                99,
            "email":              "customer3@example.com",
            "phone":              "+1-555-0300",
            "shipping_address":   "789 Pine St Salem MA",
            "card_last4":         "4242",
            "force_fail_payment": False,
        }
    else:
        log.error(f"Unknown scenario: {scenario}")
        sys.exit(1)

    order_id = place_order(order)
    log.info(f"Done. Order ID: {order_id}")
    log.info(f"Now run each service to process it:")
    log.info(f"  python3 inventory_service.py")
    log.info(f"  python3 payment_service.py")