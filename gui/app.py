"""
Flask app that drives the AI Essentials demo GUI.

Endpoints:
    GET  /                -> browser UI
    GET  /stream          -> Server-Sent-Events stream (live logs + phase)
    POST /api/start|stop|fault|clear-fault
    GET  /api/status|health

Live data flows one-way from the server to the browser via SSE (Server-Sent
Events). This is deliberately simpler and more robust than WebSockets for the
live log/clock/agent feeds this demo GUI needs, and it grows cleanly into a
multi-panel dashboard.

The producer & consumer run in background threads; any emitted event is pushed
onto a small pub/sub bus shared with the SSE streams.
"""
from __future__ import annotations

import os
import json
import queue
import threading
import time

from flask import Flask, render_template, jsonify, Response, request, stream_with_context

from . import kafka_config as cfg
from .producer import LogProducer
from .consumer import LogConsumer
from .topic_admin import recreate_topic
from .spark_tail import SparkLogTail
from .langflow_tail import LangflowTail
from . import settings as demo_settings

# --- paths ---
BASE_DIR = os.path.dirname(__file__)
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=STATIC_DIR)

# --- global engine state ---
g_lock = threading.Lock()
producer = LogProducer()
consumer = LogConsumer()

# --- SSE pub/sub bus ---
# Every live event is broadcast to every connected SSE subscriber.
_bus = []                 # list of queue.Queue (one per browser stream)
_bus_lock = threading.Lock()


def _broadcast(event_type: str, data: dict):
    """Push one event to every live SSE subscriber."""
    message = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with _bus_lock:
        for q in list(_bus):
            try:
                q.put_nowait(message)
            except queue.Full:
                pass  # drop if subscriber is slow


def _subscribe():
    """Create a new per-client queue for the SSE stream."""
    q = queue.Queue(maxsize=2000)
    with _bus_lock:
        _bus.append(q)
    return q


def _unsubscribe(q):
    with _bus_lock:
        if q in _bus:
            _bus.remove(q)


# wire the producer/consumer callbacks into the bus
def _on_emit(payload: dict):
    _broadcast("log", payload)


def _on_phase(phase: str):
    _broadcast("phase", {"phase": phase})


producer.on_produce = _on_emit
producer.on_cycle_change = _on_phase
consumer.on_message = _on_emit

# Spark driver log tail — push each line to the GUI as a "sparklog" event.
spark_tail = SparkLogTail(
    on_line=lambda line: _broadcast("sparklog", {"line": line})
)
spark_tail.start()

# Langflow agent log tail — poll the Langflow transaction monitor and push each
# new output (timestamp + message) to the GUI as a "langflowlog" event.
langflow_tail = LangflowTail(
    on_message=lambda ts, text: _broadcast("langflowlog", {"timestamp": ts, "text": text})
)
langflow_tail.start()


# ---------------------------------------------------------------------- #
# UI
# ---------------------------------------------------------------------- #
@app.route("/")
def index():
    return render_template("index.html")


@app.get("/stream")
def stream():
    """Server-Sent Events: live logs + phase changes."""
    q = _subscribe()

    def gen():
        try:
            # keepalive comment so the connection stays open
            yield ": connected\n\n"
            while True:
                try:
                    msg = q.get(timeout=15)
                    yield msg
                except queue.Empty:
                    yield ": ping\n\n"
        finally:
            _unsubscribe(q)

    return Response(
        stream_with_context(gen()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ---------------------------------------------------------------------- #
# API
# ---------------------------------------------------------------------- #
@app.post("/api/start")
def api_start():
    with g_lock:
        producer.start()
        consumer.start()
    return jsonify({"ok": True, "producer": producer.running,
                    "consumer": consumer.running})


@app.post("/api/stop")
def api_stop():
    with g_lock:
        producer.stop()
        consumer.stop()
    return jsonify({"ok": True, "producer": producer.running,
                    "consumer": consumer.running})


@app.post("/api/fault")
def api_fault():
    with g_lock:
        producer.inject_fault()
    return jsonify({"ok": True, "fault_active": producer.stats["fault_active"]})


@app.post("/api/clear-fault")
def api_clear_fault():
    with g_lock:
        producer.clear_fault()
    return jsonify({"ok": True, "fault_active": producer.stats["fault_active"]})


@app.post("/api/resolve")
def api_resolve():
    """Resolve the current storm: clear the fault + log it for the demo UI.

    This is the endpoint Langflow calls back to once it has finished its
    resolution work (RCA / runbook), closing the detect -> classify -> resolve
    loop.
    """
    with g_lock:
        producer.clear_fault()
    _broadcast("resolved", {"message": "Incident resolved via resolution agent"})
    return jsonify({
        "ok": True,
        "resolved": True,
        "fault_active": producer.stats["fault_active"],
    })


@app.post("/api/reset-topic")
def api_reset_topic():
    """Recreate the `rundmc` topic at the MapR level so there's no stale data."""
    result = recreate_topic()
    return jsonify(result)


@app.get("/api/status")
def api_status():
    return jsonify({
        "producer_running": producer.running,
        "consumer_running": consumer.running,
        "producer": producer.stats,
        "consumer": consumer.stats,
        "topic": cfg.TOPIC,
        "brokers": cfg.BROKERS,
    })


@app.get("/api/health")
def api_health():
    return jsonify({"ok": True})


# ---------------------------------------------------------------------- #
# Settings (user-configurable overrides for the three windows)
# ---------------------------------------------------------------------- #
@app.get("/api/settings")
def api_settings_get():
    return jsonify({
        "settings": demo_settings.get(),
        "defaults": demo_settings.defaults(),
        "labels": demo_settings.labels(),
    })


@app.post("/api/settings")
def api_settings_post():
    """Apply user-supplied overrides for the three windows, keeping defaults.

    Accepts a JSON body / partial {"field": "value", ...}. Blanks are ignored.
    After saving, the Spark and Langflow tails are restarted so the new
    pod/namespace/URL/flow/key take effect immediately. If the Kafka topic
    changed and the producer/consumer are running, they are restarted too.
    """
    body = request.get_json(silent=True) or {}
    overrides = body.get("overrides") if isinstance(body.get("overrides"), dict) else body
    eff = demo_settings.update(overrides)

    # Restart the tails so new pod/URL/flow/key are picked up immediately.
    spark_tail.stop()
    spark_tail.start()
    langflow_tail.stop()
    langflow_tail.start()

    # Kafka topic applies to producer/consumer -> restart if they're running.
    with g_lock:
        was_producer = producer.running
        was_consumer = consumer.running
        if was_producer:
            producer.stop()
        if was_consumer:
            consumer.stop()
        if was_producer:
            producer.start()
        if was_consumer:
            consumer.start()

    _broadcast("settings", {"settings": eff})
    return jsonify({
        "ok": True,
        "settings": eff,
        "defaults": demo_settings.defaults(),
        "producer": producer.running,
        "consumer": consumer.running,
    })


@app.post("/api/settings/reset")
def api_settings_reset():
    """Clear all overrides back to the built-in defaults."""
    eff = demo_settings.reset()
    spark_tail.stop()
    spark_tail.start()
    langflow_tail.stop()
    langflow_tail.start()
    with g_lock:
        was_producer = producer.running
        was_consumer = consumer.running
        if was_producer:
            producer.stop()
        if was_consumer:
            consumer.stop()
        if was_producer:
            producer.start()
        if was_consumer:
            consumer.start()
    _broadcast("settings", {"settings": eff})
    return jsonify({"ok": True, "settings": eff, "defaults": demo_settings.defaults()})
