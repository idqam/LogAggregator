

-- create the initial logs table 
CREATE TABLE IF NOT EXISTS logs (
    id BIGSERIAL PRIMARY KEY, -- automatically increasing id val
    log_id UUID NOT NULL UNIQUE, 
    service_name VARCHAR(100) NOT NULL,
    level VARCHAR(10) NOT NULL CHECK (level IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')),
    message TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_topic VARCHAR(200) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'
);
--these are composite indices where we sort first by x as in the pages of a book and then
-- on that book we sort the words by y
-- this in postgres handled by a B-tree
CREATE INDEX IF NOT EXISTS idx_logs_service_time ON logs (service_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_level_time ON logs (level, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_time ON logs (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_kafka_position ON logs (kafka_topic, kafka_partition, kafka_offset);

CREATE INDEX IF NOT EXISTS idx_logs_metadata_gin ON logs USING GIN (metadata);