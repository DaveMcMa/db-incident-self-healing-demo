# DB Incident Self-Healing Demo

HPE Private Cloud AI (AI Essentials) demo: a `db-prod-01` database latency
storm is detected and resolved automatically, with a live GUI showing all the
moving parts.

**Auto-heal flow:** Kafka log storm → Spark detects + stops raw logs → MLIS
classifies severity → MariaDB incident/ticket logged → Langflow resolves (RCA
runbook) → webapp GUI shows live event, Spark driver, and Langflow agent logs.

## Structure

```
gui/          Flask demo webapp (live event / Spark / Langflow log Windows)
sparkjob/     Spark Structured Streaming job that detects & classifies the storm
classifier/   BentoML service (MLIS classification endpoint) + built artifact
training/     Training notebook + labelled incident dataset
langflow/     IncidentBot.json (Langflow resolution flow)
assets/       Training / evaluation charts (PNG)
```

## Components

- **gui** — `demo_gui` Flask app. Live SSE feed of Kafka events, Spark driver
  log tail (`kubectl logs -f siem-prod-v1-driver`), and Langflow agent log tail
  (transaction monitor). Configurable overrides via the ⚙ Settings button.
- **sparkjob** — Spark job reads the `rundmc` Kafka topic, detects a latency
  storm, stops raw logs, classifies severity via BentoML/MLIS, writes an
  incident + ticket to MariaDB (`siem` schema), and hands off to Langflow.
- **classifier** — BentoML service exposing the MLIS prediction endpoint
  (`/predict`), auth via Bearer JWT; `bento` built artifact included.
- **langflow** — the resolution flow (IncidentBot) that calls back into the GUI
  `/api/resolve` when an incident is resolved.

## Run the GUI locally

```bash
cd gui
pip install -r requirements.txt   # needs kafka-python, Flask
python -m demo_gui.run            # serves http://localhost:5001
```

Requires access to the HPE lab (Kafka/MapR, the Spark driver pod via kubectl
with the cluster kubeconfig, and the Langflow instance).

## Endpoints (gui)

| Method | Path               | Purpose                            |
|--------|--------------------|------------------------------------|
| GET    | `/`                | Browser UI                         |
| GET    | `/stream`          | SSE live log feed                  |
| GET    | `/api/health`      | Health check                       |
| GET    | `/api/status`      | Producer/consumer/topic status     |
| POST   | `/api/start|stop`  | Start/stop producer + consumer     |
| POST   | `/api/fault`       | Inject the latency storm           |
| POST   | `/api/clear-fault` | Stop the storm                     |
| POST   | `/api/resolve`     | Langflow callback; marks resolved  |
| GET/POST| `/api/settings`  | View / apply window overrides      |

## Notes

- Kafka SASL/auth, MLIS classifier URL, Langflow flow ID/URL, and MariaDB
  connection are lab-specific and configured in the source / env vars.
- `enable_idempotence=False` is required when producing to the KWP broker.
