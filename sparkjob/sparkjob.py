"""
AI Essentials demo — Spark streaming job.

Two modes:
  NORMAL  -> Spark prints every raw log message received on `rundmc`
             (live connection feedback) as `[RUNDMC] <json>`.
  INCIDENT -> The moment an alert storm is detected (per-entity 15s window with
             > N alerts and p95 > threshold), raw log streaming pauses and the
             job plays a fixed self-heal sequence:
               1. Incident Detected: Batching
               2. Incident Detected: Classifying        (MLIS severity classifier)
               3. Result from classifier: <SEV>
               4. Logging ticket [Writing to MariaDB]  (lowercase `siem`)
               5. incident logged to resolution agent

The incident flag auto-reverts: Spark keeps monitoring the stream the whole
time, and once traffic returns to normal (a full cooldown passes with no new
storm output) raw log streaming resumes.

PERFORMANCE NOTES (behaviour is unchanged; see "PERF:" comments inline)
  * spark.sql.shuffle.partitions 200 -> SHUFFLE_PARTITIONS. The windowed
    aggregation previously ran ~200 tasks (each with a state-store commit) per
    micro-batch on a single executor core.
  * FAIR scheduler with one pool per query, so the cheap 5s raw-log batches are
    no longer queued behind the stateful storm batches.
  * Raw sink collects only the `value` column, decodes each message once and
    stops scanning for alerts at the first hit (only "any alert?" was used).
  * Storm sink runs the stateful plan once (collect) instead of twice
    (count + collect).
  * MariaDB writes go straight through the already-loaded MySQL JDBC driver
    instead of launching two Spark jobs per incident; falls back to the
    original Spark JDBC writer if a direct connection can't be opened.
  * SSL context built once instead of on every HTTP call.
"""
import json
import ssl
import time
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, max as _max
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType,
)

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
KAFKA_BROKER = (
    "ezdf-core2.ezmeral.demo.local:9092,"
    "ezdf-core3.ezmeral.demo.local:9092,"
    "ezdf-core1.ezmeral.demo.local:9092"
)
SASL_JAAS = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="mapr" password="mapr123";'
)
KAFKA_OPTS = {
    "kafka.bootstrap.servers": KAFKA_BROKER,
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config":  SASL_JAAS,
    "kafka.enable.idempotence": "false",
}

INPUT_TOPIC = "rundmc"
# Must match the recent messages the Spark job's raw-log sink prints; the
# swallow flag is module-level so it never collides with the storm detector's.
CHECKPOINT  = "/tmp/db_incident_checkpoint"
CHECKPOINT_RAW = "/tmp/db_incident_rawlog"

# Storm detection (15s window, per entity)
WINDOW_SECONDS    = 15
STORM_ALERT_COUNT = 100   # alerts > this in the window = storm
STORM_P95_MS      = 500   # p95 > this = breach

# PERF: number of shuffle / state-store partitions for the windowed
# aggregation. Default Spark value is 200, which is massive overkill for a
# handful of entities and was the main cause of the "falling behind" warnings.
# NOTE: a stateful query keeps the partition count stored in its checkpoint.
# If CHECKPOINT already exists from a previous run, delete it (or change the
# path) for this to take effect.
SHUFFLE_PARTITIONS = 2

# MLIS severity classifier (BentoML service, authenticated via Bearer JWT).
# Tested with curl old: POST /predict with body {"incident": {6 features}}.
MLIS_URL   = ("https://db-log-classification.project-data-engineers"
              ".serving.hpepcai4.demo.local/predict")
MLIS_TOKEN = ("eyJhbGciOiJSUzI1NiIsImtpZCI6Ikh2V3o0V1BoU3dkNFY4Tk9MSk9ObHM3Wk9UU1hxWjJLR0xyb1NwMldfSjQifQ."
              "eyJhdWQiOlsiYXBpIiwiaXN0aW8tY2EiXSwiZXhwIjoxNzkzMTk5OTM1LCJpYXQiOjE3OTA2MDc5MzUsImlzcyI6"
              "Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwianRpIjoiYTM4MzQwNDQt"
              "MDI4NC00OWY1LWE0OTAtNGU2Y2RiNjIyMDFkIiwia3ViZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJ1aSIs"
              "InNlcnZpY2VhY2NvdW50Ijp7Im5hbWUiOiJpc3ZjLWVwLTE3OTA2MDc5MzU5MDEiLCJ1aWQiOiI3ZWQyMDM5My0wMDdl"
              "LTRlNjYtYjQwZi04NGM3Yzk5Y2VjYjYifX0sIm5iZiI6MTc5MDYwNzkzNSwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2Nv"
              "dW50OnVpOmlzdmMtZXAtMTc5MDYwNzkzNTkwMSJ9."
              "ZWS60WcXPrVoqJZ4N4xK0jh0ehvtRmx6F8LryDjDMdrrRBOLZivtkK9MjcZTMd4DsFEP2pUptPPs3HAi1O5K9yfakUA9EK"
              "27x4QgmoZV0zn-UNVJEO9w8NpFdkOiyeEW_ph1gNiXxw3urIVr4UjBDyv8A81cV1plnMbJezKWGA739NvOaFgt0dYEqqMd"
              "VLNagcmZY9H10CHHx0pfc4JqElTBkfVQMcI6GnH226ChSEa-botUbdElxUGfhF0y_nrbsydAlOaXCK0_321qDtE81ej01uy"
              "47AXNJAF5A8qIG-YrIRAbX4IojstYbFrVD9KTg8e4dZtxWQEr6kmu7RZpDg")

# MariaDB (lowercase `siem`) — source of truth for incidents + tickets.
MYSQL = {
    "host": "10.0.31.73",
    "user": "root",
    "password": "Compaq1!",
    "database": "siem",      # lowercase, as recreated
}
TICKET_PREFIX = "INC"
JDBC_URL = ("jdbc:mysql://{host}:3306/{db}"
            "?useSSL=false&serverTimezone=UTC").format(
                host=MYSQL["host"], db=MYSQL["database"])
JDBC_PROPERTIES = {
    "user": MYSQL["user"],
    "password": MYSQL["password"],
    "driver": "com.mysql.cj.jdbc.Driver",
}

APP_NAME = "DBIncidentStormDetection"

# Langflow "resolution agent" — fire-and-forget hand-off after a ticket is
# logged. NOTE: this endpoint authenticates via the `x-api-key` header, NOT
# `Authorization: Bearer` (Bearer returns 403 on this setup).
LANGFLOW_URL   = ("https://langflow-rundmc.hpepcai4.demo.local/"
                  "api/v1/run/365484e7-cf26-41aa-b8c0-343451f56ec1")
LANGFLOW_TOKEN = "sk-oKOsn8Lo5z2u2ugUWAM4V8tRLKy9iXB257-LdpncFFA"

# --- driver-side state ---
_incident_in_progress = False   # suppresses raw-log printing while true
_incident_started_at = 0.0      # epoch time the incident (storm) began
_last_storm_at = 0.0            # epoch time of last alert-storm activity seen
_last_handled_at = 0.0          # epoch time we last logged an incident (cooldown)
STORM_RESET_SECS = 45           # (no longer used for dedup; kept for reference)
# One incident per storm episode: the raw sink starts a new episode when the
# first alert arrives; the storm sink logs at most one incident per episode.
_storm_episode = 0              # incremented at each "alert storm starting"
_logged_episode = 0             # episode for which an incident was last logged

# Demo timing (see main()): 1s watermark + 5s storm trigger -> ~16-26s from the
# first alert to classification.
STORM_ETA_SECS = 20

# Ticket fields derived from the classifier severity. Adjust to match the
# `priority` values your ticket table / webapp expects.
PRIORITY_BY_SEVERITY = {"SEV-1": "P1", "SEV-2": "P2", "SEV-3": "P3",
                        "SEV-4": "P4", "SEV-5": "P5"}
RESOLUTION_CODE = "Auto-resolved"

# Ticket opened for the current storm episode, so the raw sink can mark it
# resolved when the stream returns to normal.
_SPARK = None
_open_ticket = None              # {"ticket": ..., "entity": ..., "episode": ...}
_pending_resolution = None       # set if the storm ends before its ticket is written

# PERF: build the (unverified) SSL context once and reuse it for every call.
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# --------------------------------------------------------------------------- #
# Event schema (matches the demo producer's JSON)
# --------------------------------------------------------------------------- #
EVENT_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("entity", StringType(), True),
    StructField("source", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("threshold", IntegerType(), True),
    StructField("host", StringType(), True),
    StructField("region", StringType(), True),
    StructField("production", IntegerType(), True),
    StructField("business_criticality", IntegerType(), True),
    StructField("alert_level", StringType(), True),
    StructField("alert_count_boost", IntegerType(), True),
    StructField("is_alert", BooleanType(), True),
    StructField("fault", BooleanType(), True),
])

# Column layouts for the two MariaDB tables. `id` is auto-increment and is
# never written. Columns whose value is None are left out of the INSERT so the
# table's own defaults apply.
INCIDENT_COLUMNS = ["entity", "source", "alert_count", "severity", "category",
                    "assignment_group", "title", "p95_latency_ms",
                    "event_timestamp", "created_at"]
TICKET_COLUMNS = ["ticket_number", "incident_id", "short_description",
                  "category", "severity", "assignment_group", "state",
                  "priority", "assigned_to", "resolution_code",
                  "resolution_notes", "mttr_seconds", "opened_at",
                  "updated_at", "resolved_at", "closed_at"]


# --------------------------------------------------------------------------- #
# Raw log sink (NORMAL mode) — prints every message unless an incident is active
# --------------------------------------------------------------------------- #
def _is_alert(raw: str) -> bool:
    """Same test as before: truthy top-level `is_alert`; bad JSON -> False."""
    try:
        return bool(json.loads(raw).get("is_alert"))
    except Exception:  # noqa: BLE001
        return False


def log_raw_batch(batch_df, batch_id):
    global _incident_in_progress, _incident_started_at, _storm_episode
    # Collect once and inspect the whole batch. While muted we STILL monitor the
    # incoming stream so we can detect when it returns to normal (no timer:
    # resume only when a batch arrives containing zero alerts).
    # PERF: the stream is already projected to `value` only (see main()).
    rows = batch_df.collect()
    if not rows:
        return

    if _incident_in_progress:
        # Muted during the storm, but keep monitoring: if this batch is back to
        # normal (no alerts at all), the storm has ended -> resume streaming.
        # PERF: `any` stops at the first alert instead of parsing every row.
        if not any(_is_alert(r.value.decode("utf-8", errors="replace"))
                   for r in rows):
            _incident_in_progress = False
            dur = time.time() - _incident_started_at
            if dur >= 60:
                m, s = divmod(int(dur), 60)
                print(f"[spark] incident resolved in {m} min {s} sec — raw log streaming resumed")
            else:
                print(f"[spark] incident resolved in {int(dur)} seconds — raw log streaming resumed")
            record_resolution(_storm_episode, int(dur))
        return

    # PERF: decode each message once, reuse it for printing and alert check,
    # and emit the whole batch with a single write (identical output).
    decoded = [r.value.decode("utf-8", errors="replace") for r in rows]
    print("\n".join(f"[RUNDMC] {raw}" for raw in decoded))

    # The moment a real alert arrives, the storm has started -> stop raw logs
    # IMMEDIATELY (no waiting for the windowed storm aggregation to emit, which
    # caused the ~2min delay). The storm sink then does the classify + DB work.
    if any(_is_alert(raw) for raw in decoded):
        _incident_in_progress = True
        _incident_started_at = time.time()
        _storm_episode += 1
        print("[spark] alert storm starting — raw log streaming paused")
        print("Incident Detected: Batching")
        print(f"[spark] batching new alerts for inference — ETA ~{STORM_ETA_SECS} seconds")


# --------------------------------------------------------------------------- #
# MariaDB writer
# --------------------------------------------------------------------------- #
def _jdbc_write(spark, df, table):
    """Original Spark JDBC writer — kept as the fallback path."""
    df.write \
      .format("jdbc") \
      .option("url", JDBC_URL) \
      .option("dbtable", table) \
      .options(**JDBC_PROPERTIES) \
      .mode("append") \
      .save()


_jdbc_driver_registered = False


def _open_direct_jdbc(spark):
    """PERF: open a plain JDBC connection on the driver JVM.

    Uses the MySQL driver that is already on the classpath (--packages) and
    Spark's own DriverRegistry (the same mechanism Spark's JDBC writer uses to
    get around classloader isolation). Returns None if this isn't possible, in
    which case the caller falls back to the original Spark writer.
    """
    global _jdbc_driver_registered
    try:
        jvm = spark._jvm
        if not _jdbc_driver_registered:
            jvm.org.apache.spark.sql.execution.datasources.jdbc \
               .DriverRegistry.register(JDBC_PROPERTIES["driver"])
            _jdbc_driver_registered = True
        return jvm.java.sql.DriverManager.getConnection(
            JDBC_URL, JDBC_PROPERTIES["user"], JDBC_PROPERTIES["password"])
    except Exception as e:  # noqa: BLE001
        print(f"[spark] direct JDBC unavailable ({e}); using Spark JDBC writer")
        return None


def _insert_row(spark, conn, table, columns, values):
    """Single-row INSERT; returns the auto-increment id (or None)."""
    sql = "INSERT INTO `{}` ({}) VALUES ({})".format(
        table,
        ", ".join(f"`{c}`" for c in columns),
        ", ".join("?" * len(columns)),
    )
    ps = conn.prepareStatement(
        sql, spark._jvm.java.sql.Statement.RETURN_GENERATED_KEYS)
    try:
        for i, v in enumerate(values, start=1):
            ps.setObject(i, v)
        ps.executeUpdate()   # autocommit is on for a fresh connection
        rs = ps.getGeneratedKeys()
        try:
            return int(rs.getLong(1)) if rs.next() else None
        finally:
            rs.close()
    finally:
        ps.close()


def _write_row(spark, conn, table, columns, values):
    """Write one row, skipping None columns so table defaults apply.

    Uses the direct connection when available (returns the new id), otherwise
    the Spark JDBC writer (id unknown -> None).
    """
    pairs = [(c, v) for c, v in zip(columns, values) if v is not None]
    cols = [c for c, _ in pairs]
    vals = tuple(v for _, v in pairs)
    if conn is not None:
        return _insert_row(spark, conn, table, cols, vals)
    _jdbc_write(spark, spark.createDataFrame([vals], cols), table)
    return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def resolve_ticket(ticket: str, entity: str, mttr_seconds: int, resolved_at: str):
    """Mark the episode's ticket resolved with what the job knows."""
    conn = _open_direct_jdbc(_SPARK) if _SPARK is not None else None
    if conn is None:
        print(f"[spark] could not update ticket {ticket}: no direct DB connection")
        return
    notes = (f"Alert storm on {entity} subsided: stream returned to normal "
             f"(no alerts) {mttr_seconds}s after the storm began. Incident was "
             f"classified, logged and handed to the resolution agent.")
    try:
        ps = conn.prepareStatement(
            "UPDATE `tickets` SET `state` = ?, `resolution_code` = ?, "
            "`resolution_notes` = ?, `mttr_seconds` = ?, `updated_at` = ?, "
            "`resolved_at` = ? WHERE `ticket_number` = ?")
        try:
            for i, v in enumerate(("Resolved", RESOLUTION_CODE, notes,
                                   int(mttr_seconds), resolved_at, resolved_at,
                                   ticket), start=1):
                ps.setObject(i, v)
            n = ps.executeUpdate()
        finally:
            ps.close()
        print(f"[spark] ticket {ticket} marked Resolved in MariaDB "
              f"(mttr={mttr_seconds}s, rows={n})")
    except Exception as e:  # noqa: BLE001
        print(f"[spark] ticket update error for {ticket}: {e}")
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


def record_resolution(episode: int, mttr_seconds: int):
    """Called by the raw sink when a storm ends."""
    global _pending_resolution
    resolved_at = _utc_now()
    t = _open_ticket
    if t is not None and t["episode"] == episode:
        resolve_ticket(t["ticket"], t["entity"], mttr_seconds, resolved_at)
    else:
        # Storm ended before its window was classified; apply when logged.
        _pending_resolution = {"episode": episode, "mttr": mttr_seconds,
                               "resolved_at": resolved_at}


def write_incident(spark, s: dict, severity: str) -> str:
    """Write one incident + open a ticket in lowercase `siem`. Returns ticket no."""
    # Ticket number derived from the incident's unique window-start epoch, so it
    # can NEVER collide with existing/seed rows (the old monotonic counter
    # started at 2000 every restart and hit "Duplicate entry 'INC-2001'").
    win_start = s["window"]["start"]
    epoch = int(win_start.timestamp()) if hasattr(win_start, "timestamp") else int(time.time())
    ticket_number = f"{TICKET_PREFIX}-{epoch}"
    event_ts = _utc_now()
    title = f"DB latency storm on {s['entity']}"

    incident_row = (
        s["entity"], "latency-alert", int(s["alert_count"]), severity,
        "Database", "Database Ops", title,
        int(round(float(s["p95_latency_ms"]))), event_ts, event_ts,
    )

    conn = _open_direct_jdbc(spark)
    try:
        incident_id = _write_row(spark, conn, "incidents",
                                 INCIDENT_COLUMNS, incident_row)
        print(f"[spark] incident written: {s['entity']} severity={severity}"
              + (f" id={incident_id}" if incident_id is not None else ""))

        ticket_row = (
            ticket_number, incident_id, title, "Database", severity,
            "Database Ops", "New", PRIORITY_BY_SEVERITY.get(severity),
            None, None, None, None,           # assigned_to .. mttr_seconds
            event_ts, event_ts, None, None,   # opened/updated/resolved/closed
        )
        _write_row(spark, conn, "tickets", TICKET_COLUMNS, ticket_row)
        print(f"[spark] ticket opened: {ticket_number} in MariaDB (siem)")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
    return ticket_number


# --------------------------------------------------------------------------- #
# MLIS classifier
# --------------------------------------------------------------------------- #
def classify(incident: dict) -> dict:
    """Call the MLIS severity classifier. Returns {'severity':..., 'probabilities':...}."""
    body = json.dumps({"incident": incident}).encode("utf-8")
    req = urllib.request.Request(
        MLIS_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {MLIS_TOKEN}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, context=_SSL_CTX, timeout=15) as resp:
        return json.loads(resp.read())


def handoff_to_langflow(incident: dict, ticket_number: str):
    """Fire-and-forget the incident to the Langflow resolution agent.

    Authenticates via `x-api-key` (NOT Authorization Bearer). We don't read the
    response — Langflow owns the rest of the resolution lifecycle. Best-effort.
    """
    payload = {**incident, "ticket_number": ticket_number}
    body = json.dumps({
        "output_type": "chat",
        "input_value": json.dumps(payload),
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            LANGFLOW_URL,
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": LANGFLOW_TOKEN,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, context=_SSL_CTX, timeout=30) as resp:
            print(f"[spark] langflow handoff HTTP {resp.status}")
    except Exception as e:  # noqa: BLE001
        print(f"[spark] langflow handoff error: {e}")


# --------------------------------------------------------------------------- #
# Storm sink (INCIDENT mode)
# --------------------------------------------------------------------------- #
def process_storm_batch(spark, batch_df, batch_id):
    global _last_storm_at, _last_handled_at, _logged_episode
    global _open_ticket, _pending_resolution
    # PERF: a single collect() instead of count() + collect(); each action on
    # the micro-batch re-executed the whole stateful aggregation.
    rows = batch_df.collect()
    if not rows:
        return

    _last_storm_at = time.time()

    storms = [row.asDict() for row in rows]
    for s in storms:
        entity = s["entity"]
        # One incident per storm episode. The episode is opened by the raw sink
        # at the first alert and only a new storm (after "incident resolved")
        # opens another, so a long storm no longer re-logs every ~60s. Windows
        # that close late, after the storm has resolved, still belong to the
        # same episode and are skipped too.
        if _storm_episode != 0 and _logged_episode == _storm_episode:
            print(f"[spark] storm still active for {entity} — incident already logged")
            continue
        _logged_episode = _storm_episode
        _last_handled_at = time.time()

        incident = {
            "alert_count": int(s["alert_count"]),
            "p95_latency_ms": float(s["p95_latency_ms"]),
            "production": int(s.get("production") or 0),
            "business_criticality": int(s.get("business_criticality") or 0),
            # exploitability / data_exposure not in the event feed -> defaults,
            # which the classifier takes as 0 (no +1 severity bumps).
            "exploitability": 0,
            "data_exposure": 0,
        }

        print(f"[spark] batched storm for {entity}: "
              f"alerts={incident['alert_count']}, p95={incident['p95_latency_ms']}ms")
        print("Incident Detected: Classifying")
        try:
            result = classify(incident)
            severity = result.get("severity", "SEV-3")
            probs = result.get("probabilities", {})
            print(f"Result from classifier: {severity}")
            print(f"[spark] probabilities: {json.dumps(probs)}")
        except Exception as e:  # noqa: BLE001
            severity = "SEV-3"
            print(f"Result from classifier: {severity} (classifier error: {e})")

        print("Logging ticket [Writing to MariaDB]")
        ticket = None
        try:
            ticket = write_incident(spark, s, severity)
            print(f"[spark] ticket {ticket} written to siem")
        except Exception as e:  # noqa: BLE001
            print(f"[spark] DB write error: {e}")

        if ticket is not None:
            _open_ticket = {"ticket": ticket, "entity": entity,
                            "episode": _logged_episode}
            p = _pending_resolution
            if p is not None and p["episode"] == _logged_episode:
                _pending_resolution = None
                resolve_ticket(ticket, entity, p["mttr"], p["resolved_at"])

        print("incident logged to resolution agent")
        # hand the incident off to the Langflow resolution agent (fire-and-forget)
        handoff_to_langflow(incident, ticket)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    # PERF: fewer shuffle partitions + FAIR scheduling between the two queries.
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)) \
        .config("spark.scheduler.mode", "FAIR") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    spark.sparkContext.setLogLevel("WARN")
    global _SPARK
    _SPARK = spark
    sc = spark.sparkContext

    raw = spark.readStream \
        .format("kafka") \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .options(**KAFKA_OPTS) \
        .load()

    parsed = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("d")
    ).select("d.*") \
     .withColumn("event_ts", col("timestamp").cast("timestamp"))

    # --- sink 1: raw log feedback (NORMAL mode) ---
    # Reads the RAW Kafka frame (value = full JSON bytes) so each line printed
    # is the complete original log message. NOTE: must be `raw`, not `parsed` —
    # `parsed` reuses `value` for the latency metric (a float), not the JSON.
    # PERF: project to `value` only, so key/topic/offset/etc. are never shipped
    # to the driver. Own FAIR pool so it isn't stuck behind storm batches.
    sc.setLocalProperty("spark.scheduler.pool", "rawlog")
    raw_query = raw.select("value").writeStream \
        .foreachBatch(log_raw_batch) \
        .option("checkpointLocation", CHECKPOINT_RAW) \
        .trigger(processingTime="5 seconds") \
        .start()

    # --- sink 2: storm detection (INCIDENT mode) ---
    # PERF: keep only the columns the aggregation needs.
    alerts = parsed.filter(col("is_alert")) \
        .select("entity", "event_ts", "value", "production",
                "business_criticality")
    storms = alerts \
        .withWatermark("event_ts", "1 second") \
        .groupBy(col("entity"), window(col("event_ts"), f"{WINDOW_SECONDS} seconds")) \
        .agg(
            count("*").alias("alert_count"),
            _max("value").alias("p95_latency_ms"),
            _max("production").alias("production"),
            _max("business_criticality").alias("business_criticality"),
        ) \
        .filter(
            (col("alert_count") > STORM_ALERT_COUNT) &
            (col("p95_latency_ms") > STORM_P95_MS)
        )

    sc.setLocalProperty("spark.scheduler.pool", "storm")
    storm_query = storms.writeStream \
        .foreachBatch(lambda df, bid: process_storm_batch(spark, df, bid)) \
        .option("checkpointLocation", CHECKPOINT) \
        .trigger(processingTime="5 seconds") \
        .start()
    sc.setLocalProperty("spark.scheduler.pool", None)

    print(f"Streaming started. input={INPUT_TOPIC} "
          f"window={WINDOW_SECONDS}s storm> {STORM_ALERT_COUNT} alerts "
          f"& p95>{STORM_P95_MS}ms")
    raw_query.awaitTermination()
    storm_query.awaitTermination()


if __name__ == "__main__":
    main()
