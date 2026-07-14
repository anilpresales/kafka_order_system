"""
Payment Service
===============
Consumes from: inventory-checked, payment-retry
Produces to:   payment-processed, payment-retry, payment-dlq, order-compensation

Key difference from Temporal:
  When payment fails AFTER inventory was reserved, we need to manually
  trigger a compensation event. In Temporal, this is handled in the
  workflow's except block automatically. Here we have to:
  1. Detect the failure
  2. Produce a compensation event to order-compensation topic
  3. Hope that inventory-service is listening and will restock
  There is NO guarantee this compensation will succeed.
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
    format="%(asctime)s [PAYMENT-SERVICE] %(message)s"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# IDEMPOTENCY STORE
# ─────────────────────────────────────────────────────────────
processed_payments = {}


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
# ─────────────────────────────────────────────────────────────
def process_payment(order: dict) -> dict:
    order_id  = order["order_id"]
    qty       = order["qty"]
    amount    = round(qty * 49.99, 2)
    card_last4 = order.get("card_last4", "4242")

    log.info(f"Processing payment: ${amount} on card ending {card_last4}")

    # Simulate payment failure
    if order.get("force_fail_payment"):
        raise ValueError(f"Payment declined for {order_id} — insufficient funds")

    transaction_id = f"TXN-{order_id}"
    log.info(f"Payment approved: {transaction_id} — ${amount}")

    return {
        "status":         "approved",
        "transaction_id": transaction_id,
        "amount":         amount,
        "card_last4":     card_last4
    }


# ─────────────────────────────────────────────────────────────
# TRIGGER COMPENSATION
# This is the SAGA pattern — manually implemented
# In Temporal: just call restock_inventory() in the except block
# Here: produce a message and HOPE inventory service picks it up
# This is the fragility Temporal eliminates
# ─────────────────────────────────────────────────────────────
def trigger_compensation(order: dict, producer: Producer, reason: str):
    log.warning(
        f"TRIGGERING COMPENSATION for {order['order_id']} — "
        f"reason: {reason}"
    )
    log.warning(
        f"Payment failed AFTER inventory was reserved — "
        f"need to restock {order['qty']} units of {order['sku']}"
    )

    compensation_message = {
        **order,
        "compensation_type":   "restock_inventory",
        "compensation_reason": reason,
        "triggered_at":        datetime.utcnow().isoformat(),
        "triggered_by":        "payment-service",
        # PROBLEM: We don't know if inventory service will receive this
        # PROBLEM: We don't know if the restock will succeed
        # PROBLEM: There's no retry if compensation fails
        # Temporal handles all of this automatically
    }

    produce_message(
        producer,
        TOPICS["order_compensation"],
        order["order_id"],
        compensation_message
    )

    log.warning(
        f"Compensation event produced — but no guarantee it will be processed"
    )
    log.warning(
        f"This is the gap Temporal fills with guaranteed saga compensation"
    )


# ─────────────────────────────────────────────────────────────
# PROCESS MESSAGE
# ─────────────────────────────────────────────────────────────
def process_message(order: dict, producer: Producer) -> bool:
    order_id    = order["order_id"]
    retry_count = order.get("retry_count", 0)

    # ── IDEMPOTENCY CHECK ────────────────────────────────────
    if order_id in processed_payments:
        log.warning(
            f"DUPLICATE payment detected: {order_id} — "
            f"already processed. Skipping."
        )
        return True

    log.info(
        f"Processing payment for order: {order_id} "
        f"(attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS})"
    )

    try:
        result = process_payment(order)

        # ── SUCCESS ──────────────────────────────────────────
        next_message = {
            **order,
            "payment_result":    result,
            "payment_processed_at": datetime.utcnow().isoformat(),
            "status":            "payment_processed"
        }

        produce_message(
            producer,
            TOPICS["payment_processed"],
            order_id,
            next_message
        )

        processed_payments[order_id] = {
            "result":       result,
            "processed_at": datetime.utcnow().isoformat()
        }

        log.info(
            f"SUCCESS: Order {order_id} payment processed — "
            f"forwarded to warehouse"
        )
        return True

    except ValueError as e:
        # ── PAYMENT DECLINED — trigger compensation ───────────
        log.error(f"PAYMENT DECLINED for {order_id}: {e}")

        # Inventory was already reserved — need to restock
        # This is the saga compensation pattern
        trigger_compensation(order, producer, str(e))

        # Send to DLQ
        dlq_message = {
            **order,
            "error":      str(e),
            "error_type": "payment_declined",
            "failed_at":  datetime.utcnow().isoformat(),
            "service":    "payment-service"
        }
        produce_message(producer, TOPICS["payment_dlq"], order_id, dlq_message)

        processed_payments[order_id] = {
            "error":        str(e),
            "processed_at": datetime.utcnow().isoformat()
        }
        return True

    except Exception as e:
        # ── INFRASTRUCTURE ERROR — retry ─────────────────────
        log.error(f"INFRASTRUCTURE ERROR for {order_id}: {e}")

        if retry_count < MAX_RETRY_ATTEMPTS - 1:
            retry_message = {
                **order,
                "retry_count": retry_count + 1,
                "last_error":  str(e),
                "retry_at":    datetime.utcnow().isoformat()
            }
            log.info(
                f"Retrying payment "
                f"(attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS})"
            )
            time.sleep(RETRY_BACKOFF_SECONDS * (retry_count + 1))
            produce_message(
                producer,
                TOPICS["payment_retry"],
                order_id,
                retry_message
            )
        else:
            log.error(f"MAX RETRIES exceeded — sending to DLQ and triggering compensation")
            # Even on infra failure after max retries, compensate
            trigger_compensation(order, producer, f"Max retries exceeded: {e}")
            dlq_message = {
                **order,
                "error":       str(e),
                "error_type":  "max_retries_exceeded",
                "failed_at":   datetime.utcnow().isoformat(),
                "service":     "payment-service",
                "retry_count": retry_count
            }
            produce_message(producer, TOPICS["payment_dlq"], order_id, dlq_message)

        return False


# ─────────────────────────────────────────────────────────────
# CONSUMER LOOP
# ─────────────────────────────────────────────────────────────
def run():
    consumer = Consumer({
        **CONSUMER_CONFIG,
        "group.id": "payment-service-group",
    })
    producer = Producer(KAFKA_CONFIG)

    consumer.subscribe([
        TOPICS["inventory_checked"],
        TOPICS["payment_retry"]
    ])

    log.info("Payment service started — polling for inventory-checked events...")

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

            try:
                order = json.loads(msg.value().decode("utf-8"))
                log.info(
                    f"Received from {msg.topic()} "
                    f"partition [{msg.partition()}] "
                    f"offset {msg.offset()}"
                )
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse message: {e}")
                consumer.commit(message=msg)
                continue

            success = process_message(order, producer)

            if success:
                consumer.commit(message=msg)
                log.info(f"Offset committed for order {order.get('order_id')}")
            else:
                log.warning(f"Not committing offset for {order.get('order_id')}")

    except KeyboardInterrupt:
        log.info("Shutting down payment service...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()