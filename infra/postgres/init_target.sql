-- CDC event store: structured change events from the WAL watcher
CREATE TABLE IF NOT EXISTS cdc_events (
    id              BIGSERIAL    PRIMARY KEY,
    event_id        UUID         NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    source_table    VARCHAR(100) NOT NULL,
    action          VARCHAR(10)  NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    entity_id       UUID         NOT NULL,
    aggregate_id    UUID,        
    lsn             PG_LSN       NOT NULL,
    changed_columns JSONB        NOT NULL DEFAULT '{}',
    full_row        JSONB        NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_cdc_events_entity     ON cdc_events (entity_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_cdc_events_table_time ON cdc_events (source_table, received_at DESC);

CREATE TABLE IF NOT EXISTS batch_aggregates (
    id             BIGSERIAL    PRIMARY KEY,
    entity_type    VARCHAR(50)  NOT NULL,
    entity_id      UUID         NOT NULL,
    snapshotted_at TIMESTAMPTZ  NOT NULL,
    batch_run_id   UUID         NOT NULL,
    payload        JSONB        NOT NULL,
    UNIQUE (entity_type, entity_id, batch_run_id)
);

CREATE INDEX IF NOT EXISTS idx_batch_aggregates_type_time
    ON batch_aggregates (entity_type, snapshotted_at DESC);

CREATE TABLE IF NOT EXISTS integration_state (
    integration_name     VARCHAR(100) PRIMARY KEY,
    last_confirmed_lsn   TEXT,
    last_batch_run_at    TIMESTAMPTZ,
    last_updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    consecutive_failures INT          NOT NULL DEFAULT 0,
    last_error           TEXT
);

INSERT INTO integration_state (integration_name) VALUES
    ('cdc_watcher'), ('batch_aggregator')
ON CONFLICT DO NOTHING;