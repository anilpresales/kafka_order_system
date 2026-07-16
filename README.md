# Order Processing — Kafka Microservices Version

## Background

This is the SAME order processing workflow as the Temporal version,
built with Kafka microservices. It shows exactly what you have to
build manually when you don't have Temporal.

At Confluent, I worked with customers like Henry Schein building
event-driven order processing on Kafka. The core problem was always
the same — Kafka moves data reliably but doesn't track WHERE a
specific order is in its lifecycle. When payment succeeded but
warehouse notification failed, engineers had to piece together state
from logs across multiple services at 2am.

---

## Architecture

```
                        ┌─────────────────────┐
                        │    Order Service     │
                        │  Producer — entry    │
                        └──────────┬──────────┘
                                   │
                                   ▼
                        ┌─────────────────────┐
                        │    order-created     │  ← main topic
                        └──────────┬──────────┘
                                   │
                                   ▼
          ┌──────────┐   ┌─────────────────────┐   ┌──────────────┐
          │inventory │◄──│  Inventory Service   │──►│inventory-dlq │
          │-retry    │   │  Consumer + producer │   │out of stock  │
          └────┬─────┘   └──────────┬──────────┘   └──────────────┘
               │ loops back         │ success
               └───────────┐        ▼
                            │  ┌─────────────────────┐
                            │  │  inventory-checked   │  ← main topic
                            │  └──────────┬──────────┘
                            │             │
                            │             ▼
          ┌──────────┐      │  ┌─────────────────────┐   ┌──────────────┐
          │ payment  │◄─────┘  │   Payment Service    │──►│ payment-dlq  │
          │ -retry   │◄────────│   Consumer + producer│   │card declined │
          └──────────┘         └──────────┬──────────┘   └──────┬───────┘
                                          │ success              │
                                          ▼                      ▼
                               ┌─────────────────────┐  ┌───────────────────┐
                               │  payment-processed   │  │ order-compensation│
                               └──────────┬──────────┘  │ restock signal    │
                                          │              └────────┬──────────┘
                                          ▼                       │ no guarantee ⚠️
                               ┌─────────────────────┐           │
                               │  Warehouse Service   │◄──────────┘
                               │    (coming next)     │
                               └──────────┬──────────┘
                                          │
                                          ▼
                               ┌─────────────────────┐
                               │ Notification Service │
                               │    (coming next)     │
                               └─────────────────────┘
```

### Topic Map

| Topic | Purpose | Producer | Consumer |
|---|---|---|---|
| `order-created` | New orders entering system | order_service | inventory_service |
| `inventory-checked` | Orders with confirmed stock | inventory_service | payment_service |
| `payment-processed` | Orders with confirmed payment | payment_service | warehouse_service |
| `inventory-retry` | Failed inventory — infra errors | inventory_service | inventory_service |
| `payment-retry` | Failed payments — infra errors | payment_service | payment_service |
| `inventory-dlq` | Out of stock — no retry | inventory_service | manual review |
| `payment-dlq` | Card declined — no retry | payment_service | manual review |
| `order-compensation` | Restock after payment failure | payment_service | inventory_service |

---

## The Gap vs Temporal

```
⚠️  Compensation has no guarantee — producing to order-compensation
    does not mean inventory_service will pick it up and restock.

⚠️  No single source of truth — to find where order ord-8821 is,
    you must check every topic, every consumer offset, and every
    service log across 5+ services.

⚠️  Every team hand-rolls the same patterns:
    - Retry logic with backoff
    - Dead letter queue routing
    - Idempotency dedup tables
    - Saga compensation events
```

### Kafka vs Temporal — Side by Side

| Concern | Kafka (this code) | Temporal |
|---|---|---|
| Retry logic | Hand-rolled per service | `RetryPolicy` built in |
| Dead letter queue | Manual topic + producer | Automatic |
| Idempotency | Custom `processed_orders` dict | Workflow ID |
| Saga compensation | Produce event, hope consumed | Native `except` block |
| Workflow state visibility | Piece together logs | Web UI single view |
| Step visibility | Check each topic separately | Single execution history |
| Timeout handling | Manual consumer timeout | `start_to_close_timeout` |

---

## Project Structure

```
kafka_order_system/
├── config.py              ← Confluent Cloud credentials + topic names
├── order_service.py       ← Producer — places orders onto Kafka
├── inventory_service.py   ← Consumer/producer — checks stock, retries, DLQ
├── payment_service.py     ← Consumer/producer — charges card, compensation
└── README_KAFKA.md        ← This file
```

---

## Setup

```bash
pip3 install confluent-kafka
```

Add your Confluent Cloud credentials to `config.py`:

```python
BOOTSTRAP_SERVERS = "your-cluster.gcp.confluent.cloud:9092"
API_KEY           = "your-api-key"
API_SECRET        = "your-api-secret"
```

---

## How to Run

**Terminal 1 — Inventory service:**
```bash
python3 inventory_service.py
```

**Terminal 2 — Payment service:**
```bash
python3 payment_service.py
```

**Terminal 3 — Place an order:**
```bash
# Happy path
python3 order_service.py success

# Payment failure — triggers compensation
python3 order_service.py payment_fail

# Out of stock — goes to DLQ
python3 order_service.py out_of_stock
```

---

## Key Patterns Demonstrated

**Manual idempotency** — every service maintains its own `processed_orders`
dict. In production this would be a Postgres table with
`INSERT ... ON CONFLICT DO NOTHING`. Temporal handles this automatically
via Workflow ID.

**Two error types handled differently:**
- Business errors (out of stock, card declined) → commit offset, send to DLQ, don't retry
- Infrastructure errors (network timeout, DB down) → don't commit offset, send to retry topic

**Saga compensation** — when payment fails after inventory was reserved,
payment_service produces to `order-compensation`. inventory_service must
be listening to restock. There is no guarantee this succeeds — this is
the exact fragility Temporal eliminates with its native compensation in
the `except ActivityError` block.

**Manual offset commit** — `enable.auto.commit: False` on every consumer.
Offset only commits after successful DB write. If the service crashes
between processing and committing, Kafka replays the event — caught by
the idempotency check.
