# AIE Knowledge Base — Load Guide & Recommended Settings

This folder contains pre-authored, demo-grade runbook content for the
"One Incident, One Storyline" AI Essentials demo. Load these Markdown files
into an AIE Knowledge Base so the Langflow agent can retrieve grounded
recommended fixes.

## Why the content is structured this way (reliability)

AIE Knowledge Base (2026.07.1) pipelines files as: ingest -> chunk -> embed
(default `nvidia/nv-embedqa-e5-v5`) -> vector store (managed local-Weaviate)
-> retrieve top-K -> generate.

Chunk Size and Overlap Size are **user-configurable** and the docs do not
publish numeric defaults (they follow the selected model's profile/endpoint).
So every runbook in this folder is authored as a **self-contained unit**:

- One procedure per file, with entity + symptom + fix + action co-located
  (a complete answer survives any chunk boundary).
- Each runbook stays compact (< ~300 tokens) so it fits one chunk and the
  retrieval window without mid-procedure truncation.
- Keywords mirror exactly what our Kafka producer + Spark job emit
  (entity names, source strings), so semantic retrieval matches reliably.

## Recommended Knowledge Base settings

When you deploy the Knowledge Base in AIE, set these Advanced Settings:

| Parameter            | Recommended value | Why |
|----------------------|-------------------|-----|
| Embedding model      | `nvidia/nv-embedqa-e5-v5` | Default, validated by HPE |
| Chunk Size           | ~512 tokens (model profile default ok) | Self-contained sections tolerate a range |
| Overlap Size         | ~50-100 tokens | Preserves context across chunk boundaries |
| Number of Documents (K) | 5-8 | Enough context for a grounded fix |
| Relevance Threshold  | 0.5-0.6 | Includes good matches, drops noise |
| Vector DB            | Managed (local-Weaviate) | Default, zero setup |
| Maximum tokens (response) | 512 | Runbook-length answers |
| Temperature          | 0.1-0.3 | Deterministic, factual answers |

## Loading steps (AIE UI)

1. Gen AI > Knowledge Base > Create New Knowledge Base.
2. Select a project and an LLM (e.g. llama-3.1-8b or a nemotron model via MLIS).
3. Data Source: upload this `kb-content/` folder (or a directory), or point
   at a Data Volume / Object Store containing these files.
4. Set Embedding Model and the recommended Advanced Settings above.
5. Verify ingestion in the Knowledge Base File Logs (each file logs success).
6. Enable tracing (private project) and use Citations to prove provenance
   during the demo.

## File index (demo scope)

- `severity-matrix.md` — incident classification, priority, routing (SEV map)
- `db-prod-01-latency-spike.md` — the star DB latency runbook
- `virtual-patch-emergency.md` — virtual patching self-heal play
- `soc-outage-routing.md` — SOC/NOC unified routing & auto-close governance

These four content files support the single `db-prod-01` incident storyline
plus the virtual-patch / unified-visibility beats. Load the whole
`kb-content/` folder; the four are the retrieval targets, the load guide is
meta only.
