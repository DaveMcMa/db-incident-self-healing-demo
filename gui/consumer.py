"""
Kafka consumer for the AI Essentials demo GUI.

Reads events from the `rundmc` topic and forwards each to a handler callback
(which the Flask layer connects to a WebSocket to render live on screen).

Consumes from the latest offset so the GUI shows live traffic (and ignores
historical test messages already in the topic).
"""
from __future__ import annotations

import json
import threading

from kafka import KafkaConsumer

from . import kafka_config as cfg
from . import settings as demo_settings


class LogConsumer:
    """Consume db log events and push them live to the provided handler."""

    def __init__(self, on_message=None):
        self._consumer = None
        self._thread = None
        self._running = threading.Event()
        self._stats = {"received": 0}
        self.on_message = on_message   # fn(record_dict)

    # ------------------------------------------------------------------ #
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        # Consume from latest -> live view only. We use a FRESH, unique group id
        # on every start so the GUI NEVER replays historical messages that are
        # still sitting in the topic (including old ALERT/fault data from
        # earlier runs). A fixed group id + auto_offset_reset="latest" is only a
        # *fallback* and can resume from old committed offsets, which replays
        # old fault storms as "random failures". A unique group avoids that.
        import time as _t
        self._consumer = KafkaConsumer(
            demo_settings.topic(),
            **cfg.consumer_kwargs(),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=f"demo-gui-live-{int(_t.time() * 1000)}",  # unique per start
            consumer_timeout_ms=2000,
        )
        self._running.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=3)
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None

    @property
    def running(self) -> bool:
        return self._running.is_set()

    @property
    def stats(self) -> dict:
        return self._stats

    # ------------------------------------------------------------------ #
    def _run(self):
        while self._running.is_set():
            try:
                for record in self._consumer:
                    if not self._running.is_set():
                        break
                    self._stats["received"] += 1
                    try:
                        payload = json.loads(record.value.decode("utf-8"))
                    except Exception:
                        payload = {"raw": record.value.decode("utf-8", "replace")}
                    payload["__kafka"] = {
                        "partition": record.partition,
                        "offset": record.offset,
                        "ts": record.timestamp,
                    }
                    if self.on_message:
                        self.on_message(payload)
            except Exception:  # noqa: BLE001 - transient consumer errors
                if self._running.is_set():
                    threading.Event().wait(0.5)
