"""
MapR-level topic admin for the AI Essentials demo GUI.

Recreates the `rundmc` Kafka topic (delete + create) directly against the HPE
Ezmeral Data Fabric cluster, via SSH -> `maprcli kafkatopic`. Used by the "Reset
Topic" button so the demo starts from a clean, empty topic with no stale data.

Validated against ezdf-core1.ezmeral.demo.local:
    maprcli kafkatopic create -topic <t> [-partitions N] [-ttl S]
    maprcli kafkatopic delete -topic <t>
"""
from __future__ import annotations

import subprocess

# --- MapR node + creds (embedded for the demo; see app-level config) ---
MAPR_HOST = "ezdf-core1.ezmeral.demo.local"
MAPR_USER = "mapr"
MAPR_PASS = "mapr123"

TOPIC = "rundmc"
PARTITIONS = 1
TTL_SECONDS = 604800   # 7 days, matches the original topic's ttl


def _ssh_run(remote_cmd: str) -> tuple[int, str, str]:
    """Run a command on the MapR node via sshpass+ssh. Returns (rc, stdout, stderr)."""
    cmd = [
        "sshpass", "-p", MAPR_PASS, "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=15",
        "-o", "PreferredAuthentications=password",
        "-o", "PubkeyAuthentication=no",
        "-o", "NumberOfPasswordPrompts=1",
        f"{MAPR_USER}@{MAPR_HOST}",
        remote_cmd,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return proc.returncode, proc.stdout, proc.stderr


def recreate_topic(topic: str = TOPIC) -> dict:
    """Delete (if it exists) and recreate the topic. Returns a status dict."""
    # 1) delete existing topic (ignore "does not exist" errors)
    rc, out, err = _ssh_run(f"maprcli kafkatopic delete -topic {topic}")
    # 2) recreate it clean
    rc2, out2, err2 = _ssh_run(
        f"maprcli kafkatopic create -topic {topic} "
        f"-partitions {PARTITIONS} -ttl {TTL_SECONDS}"
    )

    ok = rc2 == 0 and "does not exist" not in (out2 + err2)
    return {
        "ok": ok,
        "topic": topic,
        "partitions": PARTITIONS,
        "ttl": TTL_SECONDS,
        "delete_rc": rc,
        "create_rc": rc2,
        "message": ("Topic recreated" if ok else "Reset failed")
                   + f" (delete_rc={rc}, create_rc={rc2})",
        "stdout": (out + out2).strip(),
        "stderr": (err + err2).strip(),
    }
