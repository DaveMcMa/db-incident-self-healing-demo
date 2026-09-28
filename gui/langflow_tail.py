"""
Poll the Langflow RunDMC agent's transaction monitor and push each new
transaction's timestamp + output message to a callback.

The Langflow monitor endpoint returns the most recent transactions for a flow
(including each vertex's inputs/outputs). We care about the chat output vertex,
so we emit `timestamp` + `outputs.message.message` for every new transaction we
haven't seen yet, and call `on_message(ts: str, text: str)` for each.

Uses only the stdlib (urllib) so no new dependencies are needed, and reads
config from environment variables so the endpoint/credentials can be pointed
at the right Langflow instance without editing code.
"""
from __future__ import annotations

import json
import os
import ssl
import threading
import urllib.request

# --- config (override via env) ---
LANGFOW_BASE = os.environ.get(
    "LANGFOW_BASE", "https://langflow-rundmc.hpepcai4.demo.local"
)
FLOW_ID = os.environ.get(
    "LANGFOW_FLOW_ID", "365484e7-cf26-41aa-b8c0-343451f56ec1"
)
API_KEY = os.environ.get("LANGFOW_API_KEY", "sk-oKOsn8Lo5z2u2ugUWAM4V8tRLKy9iXB257-LdpncFFA")

POLL_INTERVAL = float(os.environ.get("LANGFOW_POLL_INTERVAL", "2"))
MAX_TRANSACTIONS = int(os.environ.get("LANGFOW_MAX_TRANSACTIONS", "50"))
TOTAL_TIMEOUT = int(os.environ.get("LANGFOW_TIMEOUT", "10"))

# The Langflow endpoint presents an internal/self-signed certificate (the same
# reason the original curl command used `-sk`), so skip verification here.
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


class LangflowTail:
    def __init__(self, on_message=None):
        self.on_message = on_message   # fn(ts: str, text: str)
        self._running = threading.Event()
        self._thread = None
        self._seen = set()             # transaction ids already emitted
        self.error = None              # last poll error (for diagnostics)

    def start(self):
        if self._running.is_set():
            return
        self._running.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running.clear()

    def _effective(self):
        """Resolve (base, flow_id, api_key) from settings, else the defaults.

        Imported lazily to avoid a circular import (settings imports
        langflow_tail to read the defaults).
        """
        try:
            from . import settings as _s
            eff = _s.get()
            return (
                eff.get("langflow_base", LANGFOW_BASE),
                eff.get("langflow_flow_id", FLOW_ID),
                eff.get("langflow_api_key", API_KEY),
            )
        except Exception:  # noqa: BLE001 - fall back to module defaults
            return LANGFOW_BASE, FLOW_ID, API_KEY

    def _fetch(self):
        base, flow_id, api_key = self._effective()
        url = (
            f"{base}/api/v1/monitor/transactions"
            f"?flow_id={flow_id}"
            f"&limit={MAX_TRANSACTIONS}"
        )
        req = urllib.request.Request(url, headers={"x-api-key": api_key})
        with urllib.request.urlopen(req, timeout=TOTAL_TIMEOUT, context=_SSL_CTX) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _run(self):
        while self._running.is_set():
            try:
                data = self._fetch()
                items = data.get("items") or []
                for item in items:
                    tx_id = item.get("id")
                    if tx_id in self._seen:
                        continue
                    self._seen.add(tx_id)
                    self._seen = set(list(self._seen)[-200:])  # keep set bounded
                    # Only surface completed chat outputs (skip intermediate work).
                    if item.get("status") != "success":
                        continue
                    outputs = item.get("outputs") or {}
                    message = (outputs.get("message") or {}).get("message")
                    if message is None:
                        continue
                    ts = item.get("timestamp")
                    if self.on_message:
                        self.on_message(ts, message)
            except Exception as e:  # noqa: BLE001
                self.error = str(e)
            self._running.wait(POLL_INTERVAL)
