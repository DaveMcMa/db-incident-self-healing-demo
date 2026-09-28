-- ============================================================================
-- AI Essentials demo — MariaDB database + schema setup
--
-- Creates the databases and tables that the demo components write to:
--   * sparkjob  -> incidents, tickets        (detect/classify stage)
--   * langflow  -> tickets, ticket_activity  (resolve stage)
--
-- THE DATABASE USED BY THE RUNNING DEMO IS `siem` (lowercase).
--   host: 10.0.31.73:3306
--   user: root (see sparkjob/sparkjob.py MYSQL config)
--
-- A legacy `telco`-based variant also existed; `siem` is the recreated,
-- canonical database this demo actually uses. Both are idempotent.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. THE `siem` DATABASE (lowercase) — used by the running demo
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS siem
    CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE siem;

-- INCIDENTS -- the enriched, de-duplicated incident record written by the
-- Spark streaming job (the "Detect" stage).
CREATE TABLE IF NOT EXISTS incidents (
    id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity             VARCHAR(64)   NOT NULL,          -- e.g. 'db-prod-01'
    source             VARCHAR(64)   NOT NULL,          -- e.g. 'latency-alert'
    alert_count        INT           NOT NULL DEFAULT 0, -- noisy-alert tally
    severity           VARCHAR(8)    NOT NULL DEFAULT 'SEV-3', -- severity matrix
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

-- TICKETS -- FAKE ServiceNow. The Langflow/resolution "agent" opens -> updates
-- -> auto-resolves a row here instead of a real ServiceNow API.
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
    priority          VARCHAR(16),
    assigned_to       VARCHAR(128),
    resolution_code   VARCHAR(64),                     -- e.g. 'Index Rebuild'
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

-- TICKET_ACTIVITY -- audit trail backing the "activity feed"; every state
-- change the agent makes is logged here.
CREATE TABLE IF NOT EXISTS ticket_activity (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticket_id      BIGINT        NOT NULL,
    actor          VARCHAR(128),                       -- e.g. 'langflow-agent'
    action         VARCHAR(64)   NOT NULL,             -- opened/updated/resolved/closed
    from_state     VARCHAR(24),
    to_state       VARCHAR(24),
    note           TEXT,
    created_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_activity_ticket FOREIGN KEY (ticket_id)
        REFERENCES tickets (id) ON DELETE CASCADE,
    INDEX idx_ticket (ticket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 2. (Optional) legacy `telco` DATABASE — the original schema location.
--    Kept only for backward compatibility with the earlier demo design.
--    The current code does NOT use this; use `siem` above.
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS telco
    CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE telco;

CREATE TABLE IF NOT EXISTS incidents (
    id                 BIGINT AUTO_INCREMENT PRIMARY KEY,
    entity             VARCHAR(64)   NOT NULL,
    source             VARCHAR(64)   NOT NULL,
    alert_count        INT           NOT NULL DEFAULT 0,
    severity           VARCHAR(8)    NOT NULL DEFAULT 'SEV-3',
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

CREATE TABLE IF NOT EXISTS tickets (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticket_number     VARCHAR(32)   NOT NULL UNIQUE,
    incident_id       BIGINT,
    short_description VARCHAR(255)  NOT NULL,
    category          VARCHAR(64),
    severity          VARCHAR(8)     NOT NULL DEFAULT 'SEV-3',
    assignment_group  VARCHAR(64),
    state             VARCHAR(24)    NOT NULL DEFAULT 'New',
    priority          VARCHAR(16),
    assigned_to       VARCHAR(128),
    resolution_code   VARCHAR(64),
    resolution_notes  TEXT,
    mttr_seconds      INT,
    opened_at         DATETIME      NOT NULL,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    resolved_at       DATETIME,
    closed_at         DATETIME,
    CONSTRAINT fk_ticket_incident FOREIGN KEY (incident_id)
        REFERENCES incidents (id) ON DELETE SET NULL,
    INDEX idx_state    (state),
    INDEX idx_severity (severity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ticket_activity (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticket_id      BIGINT        NOT NULL,
    actor          VARCHAR(128),
    action         VARCHAR(64)   NOT NULL,
    from_state     VARCHAR(24),
    to_state       VARCHAR(24),
    note           TEXT,
    created_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_activity_ticket FOREIGN KEY (ticket_id)
        REFERENCES tickets (id) ON DELETE CASCADE,
    INDEX idx_ticket (ticket_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- Sample one-liners (run after the statements above):
--   SHOW DATABASES;                      -- expect siem + telco
--   USE siem; SHOW TABLES;               -- incidents, tickets, ticket_activity
--   SELECT * FROM siem.incidents ORDER BY id DESC LIMIT 5;
-- ============================================================================
