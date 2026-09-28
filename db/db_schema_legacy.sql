-- ============================================================================
-- AI Essentials demo — MariaDB schema
-- "One DB incident, four acts" — Database performance + self-heal scenario
--
-- Two tables on the SAME MariaDB instance:
--   1. incidents      -> the enriched incident from the Spark pipeline
--                         (replaces Delta Lake; standard MariaDB)
--   2. tickets        -> FAKE ServiceNow — end-to-end ticket lifecycle managed
--                         locally (no real ServiceNow instance required)
--   3. ticket_activity -> audit trail of every state change (activity feed)
--
-- Run once against your MariaDB (reuses the existing 'telco' DB + root creds
-- pattern from telco-reuse-review/00-tests/00-DB-create.ipynb).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. INCIDENTS — the enriched, de-duplicated incident record
--    Written by the Spark streaming job in Act 1 (Detect).
--    Columns mirror the KB runbook entity/keywords (db-prod-01, p95, severity).
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS telco;
USE telco;

CREATE TABLE IF NOT EXISTS incidents (
    id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity             VARCHAR(64)   NOT NULL,          -- e.g. 'db-prod-01'
    source             VARCHAR(64)   NOT NULL,          -- e.g. 'latency-alert'
    alert_count        INT           NOT NULL DEFAULT 0, -- noisy-alert tally folded in
    severity           VARCHAR(8)    NOT NULL DEFAULT 'SEV-3', -- from severity matrix
    category           VARCHAR(32)   NOT NULL DEFAULT 'Database',
    assignment_group   VARCHAR(64)   NOT NULL DEFAULT 'Database Ops',
    title              VARCHAR(255),
    p95_latency_ms     INT,
    event_timestamp    DATETIME      NOT NULL,
    created_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_entity   (entity),
    INDEX idx_severity (severity),
    INDEX idx_created  (event_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 2. TICKETS — FAKE ServiceNow
--    Drop-in replacement for a real ServiceNow instance. The Langflow/webhook
--    "agent" opens -> updates -> auto-resolves a row here instead of calling
--    the ServiceNow REST API. Columns mirror the core ServiceNow incident
--    (Incident) table fields your runbooks reference.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tickets (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticket_number     VARCHAR(32)   NOT NULL UNIQUE,   -- e.g. INC0012345
    incident_id       BIGINT,                          -- FK -> incidents.id (nullable)
    short_description VARCHAR(255)  NOT NULL,
    category          VARCHAR(64),
    severity          VARCHAR(8)     NOT NULL DEFAULT 'SEV-3',
    assignment_group  VARCHAR(64),
    state             VARCHAR(24)    NOT NULL DEFAULT 'New',
        -- lifecycle: New -> In Progress -> Resolved -> Closed
        -- (mimics ServiceNow state model; auto-close only after verification)
    priority          VARCHAR(16),
    assigned_to       VARCHAR(128),
    resolution_code   VARCHAR(64),                     -- e.g. 'Index Rebuild' / 'Lock Kill'
    resolution_notes  TEXT,
    mttr_seconds      INT,                             -- MTTR clock stopped on screen
    opened_at         DATETIME      NOT NULL,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    resolved_at       DATETIME,
    closed_at         DATETIME,
    CONSTRAINT fk_ticket_incident FOREIGN KEY (incident_id)
        REFERENCES incidents (id) ON DELETE SET NULL,
    INDEX idx_state    (state),
    INDEX idx_severity (severity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 3. TICKET_ACTIVITY — audit trail backing the "activity feed"
--    Every state change the agent makes is logged here for the demo's
--    "Agent activity feed" / provenance story.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticket_activity (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticket_id      BIGINT        NOT NULL,
    actor          VARCHAR(128),                       -- e.g. 'langflow-agent' / 'runbook'
    action         VARCHAR(64)   NOT NULL,             -- opened / updated / resolved / closed
    from_state     VARCHAR(24),
    to_state       VARCHAR(24),
    note           TEXT,
    created_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_activity_ticket FOREIGN KEY (ticket_id)
        REFERENCES tickets (id) ON DELETE CASCADE,
    INDEX idx_ticket (ticket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- Confidence/verification gate for auto-close (from soc-outage-routing.md):
--   A ticket may only auto-close when the verification gate confirms low
--   residual risk (resolution_code + resolved_at set) AND the runbook
--   verification step passed (see ticket_activity 'verified' action).
-- ============================================================================
