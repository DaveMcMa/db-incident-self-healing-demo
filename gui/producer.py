"""
DB log producer for the AI Essentials demo.

Emits structured `db-prod-01` latency-alert events onto the Kafka `rundmc`
topic, cycling between a normal baseline and an injected fault (the "storm").

Run as a daemon thread so the Flask app can start/stop it via buttons.
Controllable via a threading.Event so the GUI can start and stop cleanly.
"""
from __future__ import annotations

import json
import threading
import time
import datetime as dt
import random

from kafka import KafkaProducer

from . import kafka_config as cfg
from . import settings as demo_settings


class LogProducer:
    """Produce synthetic db-prod-01 latency events in a background thread."""

    def __init__(self):
        self._producer = None
        self._thread = None
        self._running = threading.Event()
        # Fault is ONLY ever started/stopped by explicit user action (buttons).
        # There is NO automatic cycling between normal and fault phases.
        self._fault_active = False
        self._stats = {"sent": 0, "fault_active": False}

        # callbacks the GUI can attach to
        self.on_produce = None          # fn(payload_dict)
        self.on_cycle_change = None     # fn("fault"|"normal")

    # ------------------------------------------------------------------ #
    # control
    # ------------------------------------------------------------------ #
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._producer = KafkaProducer(
            **cfg.producer_kwargs(),
            max_block_ms=5000,
            request_timeout_ms=15000,
        )
        # start in the NORMAL phase
        self._fault_active = False
        self._running.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=3)
        if self._producer:
            try:
                self._producer.close(timeout=5)
            except Exception:
                pass
            self._producer = None

    def inject_fault(self):
        """Begin the fault storm now. ONLY triggered by user action."""
        self._fault_active = True
        if self.on_cycle_change:
            self.on_cycle_change("fault")

    def clear_fault(self):
        self._fault_active = False
        if self.on_cycle_change:
            self.on_cycle_change("normal")

    @property
    def running(self) -> bool:
        return self._running.is_set()

    @property
    def stats(self) -> dict:
        self._stats["fault_active"] = self._fault_active
        return self._stats

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #
    def _run(self):
        # Start in the NORMAL phase. There is NO automatic cycling:
        # the fault storm is emitted ONLY while _fault_active has been set
        # by the user's "Inject Fault" button, and stops when they hit
        # "Reset Phase" (clear_fault).
        self.clear_fault()

        while self._running.is_set():
            if self._fault_active:
                rate = cfg.FAULT_EVENTS_PER_SEC
            else:
                rate = cfg.BASE_EVENTS_PER_SEC

            self._emit(rate)
            time.sleep(0.1)  # tick loop; each tick emits ~ rate/10 events

    def _emit(self, rate: float):
        n = max(1, int(rate // 10))
        for _ in range(n):
            payload = self._make_event()
            try:
                future = self._producer.send(demo_settings.topic(), json.dumps(payload).encode("utf-8"))
                future.get(timeout=5)
                self._stats["sent"] += 1
                if self.on_produce:
                    self.on_produce(payload)
            except Exception as e:  # noqa: BLE001 - demo resilience
                if self.on_produce:
                    self.on_produce({**payload, "error": str(e)})
                time.sleep(0.2)

    def _make_event(self) -> dict:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        if self._fault_active:
            # STORM: these are ALERTS — value sustained above the breach
            # threshold, levels alternate, and each alert counts toward the
            # "100+ alerts" storm the classifier folds together.
            p95 = random.randint(cfg.FAULT_P95_MIN_MS, cfg.FAULT_P95_MAX_MS)
            is_alert = True
            source = "latency-alert"
            level = "CRITICAL" if random.random() < 0.45 else "WARNING"
            alert_count_boost = random.randint(cfg.ALERT_BOOST_MIN, cfg.ALERT_BOOST_MAX)
        else:
            # NORMAL: these are quiet EVENTS, not alerts — healthy latency with
            # realistic jitter, always below the breach threshold.
            p95 = random.randint(cfg.NORMAL_P95_MIN_MS, cfg.NORMAL_P95_MAX_MS)
            is_alert = False
            source = "latency-event"
            level = "INFO"
            alert_count_boost = 0

        return {
            "timestamp": now,
            "entity": "db-prod-01",
            "source": source,
            "metric": "query_latency_p95_ms",
            "value": p95,
            "threshold": cfg.THRESHOLD_MS,
            "host": "db-prod-01.hpepcai.local",
            "region": "emea",
            "production": 1,
            "business_criticality": 1,
            "alert_level": level,
            "alert_count_boost": alert_count_boost,
            "is_alert": is_alert,
            "fault": self._fault_active,
        }
