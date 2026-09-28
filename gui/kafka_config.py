"""
Shared Kafka configuration for the AI Essentials demo GUI.

Single place for broker + SASL + topic settings so the producer, consumer
and any future components all agree. This is the exact config validated
against the HPE Data Fabric KWP cluster.

CRITICAL: `enable_idempotence=False` is REQUIRED. The KWP broker does not
support InitProducerIdRequest (the idempotent producer handshake), so leaving
idempotence on causes silent produce failures.
"""

# --- broker / auth ---
BROKERS = (
    "ezdf-core2.ezmeral.demo.local:9092,"
    "ezdf-core3.ezmeral.demo.local:9092,"
    "ezdf-core1.ezmeral.demo.local:9092"
)
SASL = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": "mapr",
    "sasl_plain_password": "mapr123",
    "api_version": (1, 1, 0),  # pinned to a KWP-supported protocol level
}

# --- topic ---
TOPIC = "rundmc"

# --- event generation ---
# Base rate for the "event" stream; the demo fault boosts this to an "alert storm".
BASE_EVENTS_PER_SEC = 5
# Fault ("alert storm") rate. Kept down at 30/s so the demo log stays readable —
# that is ~450 alerts per 15s window, still well above the storm trigger
# (>100 in-window alerts), while being ~7x calmer than the original 200/s.
FAULT_EVENTS_PER_SEC = 30

# Healthy (event) p95 range — stays below the 500ms breach threshold.
NORMAL_P95_MIN_MS = 70
NORMAL_P95_MAX_MS = 140

# Fault (alert) p95 range — remains above the 500ms breach threshold.
FAULT_P95_MIN_MS = 510
FAULT_P95_MAX_MS = 780

THRESHOLD_MS = 500   # p95 breach threshold referenced by the runbook

# Alert-count "boost" per alert during the storm (varied for realism).
ALERT_BOOST_MIN = 4
ALERT_BOOST_MAX = 9

# Durations (seconds) for a fault cycle
FAULT_DURATION_S = 30
NORMAL_DURATION_S = 15


def base_kwargs(**overrides):
    """Return broker + SASL kwargs usable by both producer and consumer."""
    kwargs = dict(SASL)
    kwargs["bootstrap_servers"] = BROKERS
    kwargs.update(overrides)
    return kwargs


def producer_kwargs(**overrides):
    """Producer kwargs. Forces enable_idempotence=False (KWP broker requirement).

    The KWP broker does not support InitProducerIdRequest, so idempotent
    delivery must be off or produce calls fail silently.
    """
    kwargs = base_kwargs()
    kwargs.setdefault("enable_idempotence", False)
    kwargs.update(overrides)
    return kwargs


def consumer_kwargs(**overrides):
    """Consumer kwargs. enable_idempotence is producer-only, so not included."""
    return base_kwargs(**overrides)
