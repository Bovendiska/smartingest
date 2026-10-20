-- SmartIngest metadata + quarantine schema. Silver tables are created per source at runtime.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- Anonymous demo visitors. Only a hash of each token is stored.
CREATE TABLE IF NOT EXISTS meta.demo_sessions (
    session_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash      char(64)    NOT NULL UNIQUE,
    ip_hash         char(64)    NOT NULL,           -- hashed, never the raw IP
    uploads         integer     NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS demo_sessions_expiry_idx ON meta.demo_sessions (expires_at);

CREATE TABLE IF NOT EXISTS meta.batches (
    batch_id        uuid PRIMARY KEY,               -- uuid5(owner, source, file sha256): re-upload = same id
    owner_session   uuid REFERENCES meta.demo_sessions(session_id) ON DELETE CASCADE,  -- NULL = admin
    source_name     text        NOT NULL,
    silver_table    text        NOT NULL,           -- demo data lives in its own per-visitor table
    filename        text        NOT NULL,
    object_key      text        NOT NULL,           -- bronze file key (local volume or S3)
    file_sha256     char(64)    NOT NULL,
    status          text        NOT NULL,           -- loaded | needs_review
    -- rows_quarantined and quality_score are kept current as rows leave quarantine
    rows_total      integer     NOT NULL,
    rows_clean      integer     NOT NULL,
    rows_auto_fixed integer     NOT NULL,
    rows_quarantined integer    NOT NULL,
    quality_score   numeric(5,2) NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS batches_owner_idx ON meta.batches (owner_session);

CREATE TABLE IF NOT EXISTS meta.column_profiles (
    batch_id        uuid    NOT NULL REFERENCES meta.batches(batch_id) ON DELETE CASCADE,
    column_name     text    NOT NULL,
    original_name   text    NOT NULL,
    semantic_type   text    NOT NULL,
    type_confidence numeric(4,3) NOT NULL,
    type_source     text    NOT NULL,
    null_ratio      numeric(5,4) NOT NULL,
    match_ratio     numeric(4,3) NOT NULL,
    required        boolean NOT NULL,
    distinct_count  integer NOT NULL,
    PRIMARY KEY (batch_id, column_name)
);

CREATE TABLE IF NOT EXISTS meta.issues (
    issue_id        bigserial PRIMARY KEY,
    batch_id        uuid    NOT NULL REFERENCES meta.batches(batch_id) ON DELETE CASCADE,
    row_index       integer NOT NULL,
    column_name     text    NOT NULL,                -- '_row' for row-level issues (duplicates)
    rule            text    NOT NULL,
    original_value  text,
    suggestion      text,
    reason          text    NOT NULL DEFAULT '',
    confidence      text    NOT NULL,                -- high | medium | low | none
    source          text    NOT NULL,                -- rule | llm
    status          text    NOT NULL,                -- open | auto_fixed | accepted | edited | value_dropped | row_dropped
    final_value     text,
    resolved_at     timestamptz,
    UNIQUE (batch_id, row_index, column_name)
);
CREATE INDEX IF NOT EXISTS issues_open_idx ON meta.issues (batch_id) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS meta.schema_registry (
    source_name     text NOT NULL,
    column_name     text NOT NULL,
    semantic_type   text NOT NULL,
    first_batch_id  uuid NOT NULL,
    PRIMARY KEY (source_name, column_name)
);

CREATE TABLE IF NOT EXISTS meta.llm_calls (
    call_id         bigserial PRIMARY KEY,
    batch_id        uuid,
    purpose         text    NOT NULL,
    model           text    NOT NULL,
    input_tokens    integer NOT NULL,
    output_tokens   integer NOT NULL,
    latency_ms      integer NOT NULL,
    ok              boolean NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quarantine.rows (
    batch_id        uuid    NOT NULL REFERENCES meta.batches(batch_id) ON DELETE CASCADE,
    row_index       integer NOT NULL,
    data            jsonb   NOT NULL,                -- values after high-confidence auto-fixes
    PRIMARY KEY (batch_id, row_index)
);
