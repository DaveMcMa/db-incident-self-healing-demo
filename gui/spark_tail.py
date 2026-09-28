"""
Tail the Spark driver logs and push lines to a callback.

Runs `kubectl logs -f` on the Spark driver pod in the demo namespace and calls
`on_line(line)` for every line of stdout/stderr. Uses the dedicated HPE cluster
kubeconfig saved at ~/.kube/config-ctc so it doesn't depend on the default
(minikube) context.
"""
from __future__ import annotations

import os
import subprocess
import threading

KUBECONFIG = "/Users/davemcmahon/.kube/config-ctc"
NAMESPACE = "project-user-dmcmahon"
POD = "siem-prod-v1-driver"


def _effective():
    """Return (pod, namespace) from the settings store, else the defaults.

    Imported lazily to avoid a circular import (settings imports spark_tail to
    read the defaults).
    """
    try:
        from . import settings as _s
        eff = _s.get()
        return eff.get("spark_pod", POD), eff.get("spark_ns", NAMESPACE)
    except Exception:  # noqa: BLE001 - fall back to hard-coded defaults
        return POD, NAMESPACE


class SparkLogTail:
    def __init__(self, on_line=None):
        self.on_line = on_line          # fn(line: str)
        self._proc = None
        self._thread = None
        self._running = threading.Event()

    def start(self):
        if self._running.is_set():
            return
        self._running.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running.clear()
        if self._proc:
            try:
                self._proc.terminate()
            except Exception:  # noqa: BLE001
                pass
            self._proc = None

    def _run(self):
        env = {"KUBECONFIG": KUBECONFIG}
        full_env = {**os.environ, **env}

        # Reconnect loop so the tail survives pod recreations.
        while self._running.is_set():
            pod, namespace = _effective()
            cmd = [
                "kubectl", "logs", "-f",
                pod, "-n", namespace,
            ]
            try:
                self._proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, env=full_env,
                )
                for line in self._proc.stdout:
                    if not self._running.is_set():
                        break
                    if self.on_line and line:
                        self.on_line(line.rstrip("\n"))
            except Exception as e:  # noqa: BLE001
                if self.on_line:
                    self.on_line(f"[spark-tail] error: {e}")
            finally:
                if self._proc:
                    try:
                        self._proc.wait(timeout=5)
                    except Exception:  # noqa: BLE001
                        pass
            # brief pause before reconnecting
            self._running.wait(2)
