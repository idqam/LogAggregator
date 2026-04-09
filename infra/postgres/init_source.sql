-- Log store — write-heavy, read-occasionally 
CREATE TABLE IF NOT EXISTS logs (
    id              BIGSERIAL    PRIMARY KEY,
    log_id          UUID         NOT NULL UNIQUE,
    service_name    VARCHAR(100) NOT NULL,
    level           VARCHAR(10)  NOT NULL CHECK (level IN ('DEBUG','INFO','WARN','ERROR','FATAL')),
    message         TEXT         NOT NULL,
    timestamp       TIMESTAMPTZ  NOT NULL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    kafka_topic     VARCHAR(200) NOT NULL,
    kafka_partition INT          NOT NULL,
    kafka_offset    BIGINT       NOT NULL,
    metadata        JSONB        NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_logs_service_time   ON logs (service_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level_time     ON logs (level, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_time           ON logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_kafka_position ON logs (kafka_topic, kafka_partition, kafka_offset);
CREATE INDEX IF NOT EXISTS idx_logs_metadata_gin   ON logs USING GIN (metadata);

CREATE TABLE IF NOT EXISTS anomalies (
    id             BIGSERIAL    PRIMARY KEY,
    anomaly_id     UUID         NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    detected_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    anomaly_type   VARCHAR(50)  NOT NULL,
    service_name   VARCHAR(100),
    severity       VARCHAR(20)  NOT NULL CHECK (severity IN ('low','medium','high','critical')),
    window_start   TIMESTAMPTZ  NOT NULL,
    window_end     TIMESTAMPTZ  NOT NULL,
    affected_count INT          NOT NULL,
    zscore         NUMERIC(8,4),
    description    TEXT         NOT NULL,
    log_ids        UUID[]       NOT NULL DEFAULT '{}',
    resolved       BOOLEAN      NOT NULL DEFAULT false,
    resolved_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected     ON anomalies (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_service_type ON anomalies (service_name, anomaly_type, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity     ON anomalies (severity, detected_at DESC)
    WHERE resolved = false;

-- ── Detection job queue — Postgres-native task queue via SKIP LOCKED
CREATE TABLE IF NOT EXISTS detection_jobs (
    id            BIGSERIAL    PRIMARY KEY,
    job_id        UUID         NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    scheduled_for TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    job_type      VARCHAR(50)  NOT NULL,
    payload       JSONB        NOT NULL DEFAULT '{}',
    attempts      INT          NOT NULL DEFAULT 0,
    last_error    TEXT,
    completed_at  TIMESTAMPTZ,
    worker_id     VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON detection_jobs (scheduled_for ASC)
    WHERE status = 'pending';

-- ── Kafka offset tracking — application-level idempotency
CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic      VARCHAR(200) NOT NULL,
    partition  INT          NOT NULL,
    offset     BIGINT       NOT NULL,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

------ fallback for Batch Change Aggregator when CDC is down 
CREATE TABLE IF NOT EXISTS change_log (
    id          BIGSERIAL    PRIMARY KEY,
    entity_type VARCHAR(50)  NOT NULL,
    entity_id   UUID         NOT NULL,
    changed_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    action      VARCHAR(10)  NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE'))
);

CREATE INDEX IF NOT EXISTS idx_change_log_type_time
    ON change_log (entity_type, changed_at DESC);

CREATE OR REPLACE FUNCTION record_log_change() RETURNS trigger AS $$
BEGIN
    INSERT INTO change_log (entity_type, entity_id, action)
    VALUES ('log', COALESCE(NEW.log_id, OLD.log_id), TG_OP);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER logs_change_trigger
    AFTER INSERT OR UPDATE OR DELETE ON logs
    FOR EACH ROW EXECUTE FUNCTION record_log_change();

CREATE OR REPLACE FUNCTION record_anomaly_change() RETURNS trigger AS $$
BEGIN
    INSERT INTO change_log (entity_type, entity_id, action)
    VALUES ('anomaly', COALESCE(NEW.anomaly_id, OLD.anomaly_id), TG_OP);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER anomalies_change_trigger
    AFTER INSERT OR UPDATE OR DELETE ON anomalies
    FOR EACH ROW EXECUTE FUNCTION record_anomaly_change();