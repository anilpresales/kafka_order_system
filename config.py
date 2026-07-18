# config.py
# Shared Kafka configuration for all services

# ─────────────────────────────────────────────────────────────
# YOUR CONFLUENT CLOUD CREDENTIALS — fill these in
# ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "bootstrap:9092"
API_KEY           = "APIKEY"
API_SECRET        = "SECRET"

# ─────────────────────────────────────────────────────────────
# BASE CONFIGS
# ─────────────────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
}

CONSUMER_CONFIG = {
    **KAFKA_CONFIG,
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": False,
}

# ─────────────────────────────────────────────────────────────
# TOPICS
# ─────────────────────────────────────────────────────────────
TOPICS = {
    "order_created":      "order-created",
    "inventory_checked":  "inventory-checked",
    "payment_processed":  "payment-processed",
    "inventory_retry":    "inventory-retry",
    "payment_retry":      "payment-retry",
    "inventory_dlq":      "inventory-dlq",
    "payment_dlq":        "payment-dlq",
    "order_compensation": "order-compensation",
}

# ─────────────────────────────────────────────────────────────
# RETRY SETTINGS
# ─────────────────────────────────────────────────────────────
MAX_RETRY_ATTEMPTS    = 3
RETRY_BACKOFF_SECONDS = 2
