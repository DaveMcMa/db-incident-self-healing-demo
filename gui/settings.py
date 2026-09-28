"""
User-configurable overrides for the three GUI windows, layered on top of the
hard-coded defaults.

Three windows, each with a small set of override-able settings (defaults shown):

  * Live event stream  -> kafka_topic            (default: cfg.TOPIC = "rundmc")
  * Spark driver logs  -> spark_pod, spark_ns    (default: siem-prod-v1-driver /
                                                   project-user-dmcmahon)
  * Langflow agent log -> langflow_base, langflow_flow_id, langflow_api_key

Overrides are loaded from (and saved to) a small JSON file so they survive a
webapp restart. If no override is stored, the built-in defaults are used.
"""
from __future__ import annotations

import json
import os

from . import kafka_config as cfg
from . import spark_tail as st
from . import langflow_tail as lf

# ---- persistence --------------------------------------------------------
# Store overrides next to the package so they're easy to find & edit by hand.
SETTINGS_FILE = os.environ.get(
    "DEMO_SETTINGS_FILE",
    os.path.join(os.path.dirname(__file__), "settings.json"),
)

# ---- defaults (identical to the current hard-coded behaviour) -----------
DEFAULTS = {
    # Window 1: live event stream
    "kafka_topic": cfg.TOPIC,
    # Window 2: Spark driver logs
    "spark_pod": st.POD,
    "spark_ns": st.NAMESPACE,
    # Window 3: Langflow agent log
    "langflow_base": lf.LANGFOW_BASE,
    "langflow_flow_id": lf.FLOW_ID,
    "langflow_api_key": lf.API_KEY,
}

# Field -> short human label (used by the settings UI).
LABELS = {
    "kafka_topic": "Kafka topic",
    "spark_pod": "Spark driver pod",
    "spark_ns": "Kubernetes namespace",
    "langflow_base": "Langflow base URL",
    "langflow_flow_id": "Langflow flow ID",
    "langflow_api_key": "Langflow API key",
}


def _load() -> dict:
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as fh:
            stored = json.load(fh)
        if not isinstance(stored, dict):
            stored = {}
    except Exception:  # noqa: BLE001 - file missing/corrupt -> defaults
        stored = {}
    # layer stored overrides on top of defaults (unknown keys are dropped)
    result = {}
    for key, default in DEFAULTS.items():
        value = stored.get(key)
        if value is not None:
            result[key] = value
        else:
            result[key] = default
    return result


def get() -> dict:
    """Current effective settings: overrides if present, else defaults."""
    return dict(_load())


def defaults() -> dict:
    return dict(DEFAULTS)


def labels() -> dict:
    return dict(LABELS)


def update(overrides: dict) -> dict:
    """Merge the given overrides into the stored settings and persist.

    Only known keys are accepted. Blank/None values are ignored (keeps the
    current/default value). Returns the resulting effective settings.
    """
    current = _load()
    changed = False
    for key, value in (overrides or {}).items():
        if key not in DEFAULTS:
            continue
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        value = str(value).strip()
        if current.get(key) != value:
            current[key] = value
            changed = True
    if changed:
        _save(current)
    return _normalize(current)


def _save(current: dict) -> None:
    with open(SETTINGS_FILE, "w", encoding="utf-8") as fh:
        json.dump(current, fh, indent=2)


def _normalize(current: dict) -> dict:
    """Return a clean settings dict with defaults for anything missing."""
    return {k: current.get(k, DEFAULTS[k]) for k in DEFAULTS}


def reset() -> dict:
    """Clear stored overrides and return the factory defaults."""
    try:
        os.remove(SETTINGS_FILE)
    except OSError:
        pass
    return dict(DEFAULTS)


def topic() -> str:
    """Effective Kafka topic (override if set, else the DEFAULTS value)."""
    return _load().get("kafka_topic", cfg.TOPIC)


def apply_to_runtime() -> None:
    """Record current settings for the worker loops.

    The Spark and Langflow tails pick any new pod/URL/flow/key values up on
    their next reconnect/poll automatically because they read settings on each
    cycle. A Kafka topic change affects the producer/consumer, which are
    (re)created on start/stop via the existing API, so no extra work here.
    """
    return _load()
