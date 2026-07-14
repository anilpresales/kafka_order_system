"""
Inventory Service
=================
Consumes from: order-created, inventory-retry
Produces to:   inventory-checked, inventory-retry, inventory-dlq, order-compensation

This is an INDEPENDENT MICROSERVICE — it has no knowledge of other services.
It only knows about its input and output topics.

Notice all the manual work needed vs Temporal:
  - Manual retry logic with backoff
  - Manual dead letter queue handling
  - Manual idempotency tracking
  - Manual compensation triggering
  - No visibility into overall order state
"""

import json
import time
import logging
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
from config import (
    KAFKA_CONFIG, CONSUMER_CONFIG, TOPICS,
    MAX_RETRY_ATTEMPTS, RETRY_BACKOFF_SECONDS
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INVENTORY-SERVICE] %(message)s"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# IN-MEMORY IDEMPOTENCY STORE
# Tracks processed order IDs to handle duplicate messages
# In production: use Redis or PostgreSQL
# Temporal handles this automatically with workflow IDs
# ─────────────────────────────────────────────────────────────
processed_orders = {}  # order_id → result


# ─────────────────────────────────────────────────────────────
# SIMULATED INVENTORY DATABASE
# In production: real database query
# ─────────────────────────────────────────────────────────────
INVENTORY_DB = {
    "sku_999": 100,
    "sku_888": 3,
}


# ─────────────────────────────────────────────────────────────
# PRODUCER HELPER
# ─────────────────────────────────────────────────────────────
def produce_message(producer, topic, key, message):
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(message),
        callback=lambda err, msg: (
            log.error(f"Delivery failed: {err}") if err
            else log.info(f"Produced to {msg.topic()} offset {msg.offset()}")
        )
    )
    producer.flush()


# ─────────────────────────────────────────────────────────────
# CORE BUSINESS LOGIC
# Check if inventory is available for the order
# ─────────────────────────────────────────────────────────────
def check_inventory(order: dict) -> dict:
    sku = order["sku"]
    qty = order["qty"]
    available = INVENTORY_DB.get(sku, 0)

    log.info(f"Checking inventory: {sku} need={qty} available={available}")

    if available < qty:
        raise ValueError(
            f"Insufficient stock for {sku}: have {available}, need {qty}"
        )

    # Deduct inventory
    INVENTORY_DB[sku] -= qty
    remaining = INVENTORY_DB[sku]
    log.info(f"Inventory reserved: {sku} remaining={remaining}")

    return {
        "status":          "ok",
        "sku":             sku,
        "qty_reserved":    qty,
        "remaining_stock": remaining
    }


# ─────────────────────────────────────────────────────────────
# PROCESS MESSAGE
# Handles idempotency, retry logic, DLQ routing
# This is all manual — Temporal provides this automatically
# ─────────────────────────────────────────────────────────────
def process_message(order: dict, producer: Producer) -> bool:
    order_id    = order["order_id"]
    retry_count = order.get("retry_count", 0)

    # ── IDEMPOTENCY CHECK ────────────────────────────────────
    # Manual dedup — Temporal does this automatically via workflow ID
    if order_id in processed_orders:
        log.warning(
            f"DUPLICATE detected: {order_id} — "
            f"already processed at {processed_orders[order_id]['processed_at']}. "
            f"Skipping."
        )
        return True  # commit offset, move on

    log.info(f"Processing order: {order_id} (attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS})")

    try:
        # ── BUSINESS LOGIC ───────────────────────────────────
        result = check_inventory(order)

        # ── SUCCESS — produce to next topic ──────────────────
        next_message = {
            **order,
            "inventory_result": result,
            "inventory_checked_at": datetime.utcnow().isoformat(),
            "status": "inventory_checked"
        }

        produce_message(
            producer,
            TOPICS["inventory_checked"],
            order_id,
            next_message
        )

        # Record in idempotency store
        processed_orders[order_id] = {
            "result":       result,
            "processed_at": datetime.utcnow().isoformat()
        }

        log.info(f"SUCCESS: Order {order_id} inventory checked — forwarded to payment")
        return True

    except ValueError as e:
        # ── BUSINESS ERROR (out of stock) ────────────────────
        # Don't retry — retrying won't fix out of stock
        log.error(f"BUSINESS ERROR for {order_id}: {e}")
        log.info(f"Sending to DLQ — retry won't help")

        # Send to dead letter queue
        dlq_message = {
            **order,
            "error":        str(e),
            "error_type":   "business_error",
            "failed_at":    datetime.utcnow().isoformat(),
            "service":      "inventory-service"
        }
        produce_message(producer, TOPICS["inventory_dlq"], order_id, dlq_message)

        # Record as processed so we don't retry
        processed_orders[order_id] = {
            "error":        str(e),
            "processed_at": datetime.utcnow().isoformat()
        }
        return True  # commit offset — business errors don't retry

    except Exception as e:
        # ── INFRASTRUCTURE ERROR ─────────────────────────────
        # Retry — this might be a transient failure
        log.error(f"INFRASTRUCTURE ERROR for {order_id}: {e}")

        if retry_count < MAX_RETRY_ATTEMPTS - 1:
            # Send to retry topic with incremented count
            retry_message = {
                **order,
                "retry_count": retry_count + 1,
                "last_error":  str(e),
                "retry_at":    datetime.utcnow().isoformat()
            }
            log.info(
                f"Sending to retry topic "
                f"(attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS})"
            )

            # Backoff before retry
            # Temporal does this automatically with RetryPolicy
            time.sleep(RETRY_BACKOFF_SECONDS * (retry_count + 1))

            produce_message(
                producer,
                TOPICS["inventory_retry"],
                order_id,
                retry_message
            )
        else:
            # Max retries exceeded — send to DLQ
            log.error(
                f"MAX RETRIES exceeded for {order_id} — "
                f"sending to DLQ"
            )
            dlq_message = {
                **order,
                "error":        str(e),
                "error_type":   "max_retries_exceeded",
                "failed_at":    datetime.utcnow().isoformat(),
                "service":      "inventory-service",
                "retry_count":  retry_count
            }
            produce_message(
                producer, TOPICS["inventory_dlq"],
                order_id, dlq_message
            )

        return False  # don't commit offset for infra errors


# ─────────────────────────────────────────────────────────────
# CONSUMER LOOP
# Polls Kafka for messages and processes them
# This is the equivalent of Temporal's worker.run()
# ─────────────────────────────────────────────────────────────
def run():
    consumer = Consumer({
        **CONSUMER_CONFIG,
        "group.id": "inventory-service-group",
    })
    producer = Producer(KAFKA_CONFIG)

    # Subscribe to both main topic and retry topic
    consumer.subscribe([
        TOPICS["order_created"],
        TOPICS["inventory_retry"]
    ])

    log.info("Inventory service started — polling for orders...")
    log.info(f"Subscribed to: {TOPICS['order_created']}, {TOPICS['inventory_retry']}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.info("Reached end of partition")
                else:
                    log.error(f"Kafka error: {msg.error()}")
                continue

            # Parse message
            try:
                order = json.loads(msg.value().decode("utf-8"))
                log.info(
                    f"Received message from {msg.topic()} "
                    f"partition [{msg.partition()}] "
                    f"offset {msg.offset()}"
                )
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse message: {e}")
                consumer.commit(message=msg)
                continue

            # Process message
            success = process_message(order, producer)

            # Only commit offset after successful processing
            # This is manual — Temporal handles checkpointing automatically
            if success:
                consumer.commit(message=msg)
                log.info(f"Offset committed for order {order.get('order_id')}")
            else:
                log.warning(
                    f"Not committing offset — "
                    f"will retry order {order.get('order_id')}"
                )

    except KeyboardInterrupt:
        log.info("Shutting down inventory service...")
    finally:
        consumer.close()
        log.info("Consumer closed")


if __name__ == "__main__":
    run()