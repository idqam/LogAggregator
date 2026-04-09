# Log Aggregation & Anomaly Detection Pipeline — 1-Month MVP Sprint Plan

**Goal**: A production-quality Python-heavy pipeline that ingests structured logs from a Kafka
topic, persists them to PostgreSQL, runs statistical anomaly detection on rolling windows, exposes
a query API for log search and anomaly surfacing, and withstands heavy load via a simulated
producer that saturates the consumer. Portfolio-ready in 4 weeks.

**Demo**: A load simulator hammers Kafka with 10,000 log events per second across 5 simulated
services. The Python consumer pipeline ingests, classifies, and stores them. Anomalies (error
spikes, latency outliers, co-occurring failures) surface in real time through a query API. Grafana
shows consumer lag, ingestion throughput, anomaly detection latency, and query performance — all
under sustained load.

---

## Why This Project Is Technically Interesting

Most log pipelines are glue code around managed services. This one is not. You implement:

- A Kafka consumer group with manual offset commits and partition assignment callbacks — the
  mechanics that managed services hide from you
- `SELECT ... FOR UPDATE SKIP LOCKED` as a Postgres-native task queue for anomaly jobs — a
  pattern most engineers do not know exists
- Statistical anomaly detection (Z-score, IQR, co-occurrence windows) implemented in numpy
  and scipy without an ML framework — you understand the math, not just the API
- A load simulator that generates statistically realistic log distributions including injected
  anomalies on a schedule, so you can verify detection works under pressure
- SQL window functions for co-occurrence detection — "find log lines from service X that
  co-occur within 500ms of an error in service Y" is a genuine hard query

---

## Stack

| Layer           | Technology                        | Role                                        |
| --------------- | --------------------------------- | ------------------------------------------- |
| Message broker  | Kafka + Zookeeper                 | Durable log event stream                    |
| Consumer        | Python + confluent-kafka          | Consumer group, manual offset commits       |
| Processing      | Python + numpy + scipy + pandas   | Anomaly detection, statistical transforms   |
| Storage         | PostgreSQL 16                     | Log store, anomaly store, task queue        |
| Query API       | FastAPI + asyncpg                 | Log search, anomaly surfacing               |
| Load simulation | Python (locust + custom producer) | Realistic load with injected anomalies      |
| Observability   | Prometheus + Grafana              | Consumer lag, throughput, detection latency |
| Containers      | Docker Compose                    | All services                                |
| Infra           | Terraform                         | EC2 t3.medium                               |

---

## Repository Structure

```
log-aggregation-pipeline/
├── docker-compose.yml
├── docker-compose.prod.yml
├── Makefile
├── .github/workflows/ci.yml
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── services/
│   ├── consumer/
│   │   ├── pyproject.toml
│   │   ├── Dockerfile
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── settings.py
│   │   │   ├── consumer.py
│   │   │   ├── pipeline.py
│   │   │   ├── models.py
│   │   │   └── db.py
│   │   └── tests/
│   │       ├── test_consumer.py
│   │       ├── test_pipeline.py
│   │       └── test_anomaly.py
│   ├── api/
│   │   ├── pyproject.toml
│   │   ├── Dockerfile
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── settings.py
│   │   │   ├── db.py
│   │   │   ├── models.py
│   │   │   └── routes/
│   │   │       ├── logs.py
│   │   │       ├── anomalies.py
│   │   │       └── pipeline.py
│   │   └── tests/
│   │       ├── test_logs.py
│   │       └── test_anomalies.py
│   ├── detector/
│   │   ├── pyproject.toml
│   │   ├── Dockerfile
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── settings.py
│   │   │   ├── worker.py
│   │   │   ├── db.py
│   │   │   └── detectors/
│   │   │       ├── zscore.py
│   │   │       ├── iqr.py
│   │   │       └── cooccurrence.py
│   │   └── tests/
│   │       ├── test_zscore.py
│   │       ├── test_iqr.py
│   │       └── test_cooccurrence.py
│   └── simulator/
│       ├── pyproject.toml
│       ├── Dockerfile
│       ├── app/
│       │   ├── main.py
│       │   ├── producer.py
│       │   ├── scenarios.py
│       │   └── distributions.py
│       └── tests/
│           └── test_distributions.py
├── infra/
│   ├── postgres/init.sql
│   ├── kafka/server.properties
│   ├── prometheus/prometheus.yml
│   └── grafana/dashboards/
│       ├── pipeline.json
│       └── anomalies.json
└── docs/
    ├── ARCHITECTURE.md
    ├── LIMITATIONS.md
    ├── TRADEOFFS.md
    ├── QUERY_ANALYSIS.md
    └── LOAD_TEST_RESULTS.md
```

---

## Week 1 — Infrastructure + Kafka Consumer + Postgres Schema

**Sprint goal**: Kafka running, Python consumer group ingesting log events into Postgres with
manual offset commits, load simulator emitting realistic log distributions at 1,000 events/sec.
All infrastructure containerized and healthy.

### Reading Assignments

**"Designing Data-Intensive Applications" — Martin Kleppmann**

- Chapter 11 (Stream Processing): Read the entire chapter. This is the theoretical foundation
  for everything you build this month. Focus on: the difference between a message broker and a
  database, consumer group semantics, at-least-once vs exactly-once delivery, and the log as a
  data structure. The log abstraction — Kafka's core insight — is that an append-only ordered
  sequence of events is a more primitive and more powerful data structure than a message queue.
  Know this argument cold.
- Chapter 3 (Storage and Retrieval): Re-read the B-tree vs LSM-tree section. Kafka's storage
  engine is LSM-tree based. Postgres uses B-trees. These are not arbitrary — they reflect
  different read/write access patterns. You will explain this when someone asks "why Kafka and
  not just Postgres for the stream?"

**"Fluent Python" — Luciano Ramalho**

- Chapter 19 (Concurrency Models in Python): The section comparing threads, processes, and async
  coroutines. Your consumer runs in a thread (confluent-kafka's consumer is not async-native).
  Your anomaly detector uses a process pool for CPU-bound scipy work. Your API uses asyncio.
  You are using all three models in one project. Know why each is appropriate for its context.
- Chapter 7 (Functions as First-Class Objects): Understand closures and how Python passes
  functions as arguments. Your pipeline is a series of transformation functions composed
  together — knowing this chapter makes the design feel natural, not accidental.

**Kafka Documentation — kafka.apache.org/documentation**

- "Introduction" and "Design" sections. Read these before touching any code. Understand:
  topics, partitions, offsets, consumer groups, replication. The partition is the unit of
  parallelism and ordering in Kafka. Within a partition, messages are ordered. Across partitions,
  they are not. This constraint shapes every design decision downstream.
- "Consumer Group" section specifically. The rebalance protocol — what happens when a consumer
  joins or leaves a group — is the source of most Kafka consumer bugs. Know it.

### Blog Posts to Read This Week

- "Kafka in a Nutshell" — sookocheff.com. The best short overview. Read it before the Kafka
  docs to build the mental model first.
- "Exactly-Once Semantics in Apache Kafka" — confluent.io/blog. You are implementing
  at-least-once (simpler and correct for log aggregation). Know what exactly-once would require
  and why you chose not to implement it. This is a tradeoff to document.
- "Python confluent-kafka vs kafka-python" — any comparison article. You are using
  confluent-kafka because it wraps librdkafka (a C library), which is significantly faster than
  kafka-python's pure Python implementation. Know this and be able to state the throughput
  difference (~1M messages/sec for confluent-kafka vs ~50k for kafka-python under load).
- "PostgreSQL as a Message Queue" — adriano.me or similar. Understand the
  `SELECT ... FOR UPDATE SKIP LOCKED` pattern before you implement it in Week 2.

---

### Files to Implement This Week

---

#### `docker-compose.yml`

Services:

**zookeeper**: image confluentinc/cp-zookeeper:7.5.0. Environment: ZOOKEEPER_CLIENT_PORT=2181,
ZOOKEEPER_TICK_TIME=2000. No external ports needed — Kafka communicates with it internally.
Healthcheck: `echo ruok | nc localhost 2181 | grep imok`.

**kafka**: image confluentinc/cp-kafka:7.5.0. Depends on zookeeper healthy. Environment:
KAFKA_BROKER_ID=1, KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181,
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092,
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1, KAFKA_AUTO_CREATE_TOPICS_ENABLE=false.
Expose 9092 for local access. Healthcheck: `kafka-broker-api-versions --bootstrap-server localhost:9092`.

The `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` setting is important and worth explaining in an
interview: auto-created topics use default settings (1 partition, replication factor 1). You
want to create topics explicitly with intentional partition counts.

**kafka-init**: image confluentinc/cp-kafka:7.5.0. Runs once after kafka is healthy. Creates
the topics you need:

```yaml
command: >
  bash -c "
  kafka-topics --create --bootstrap-server kafka:29092
    --topic logs.raw --partitions 6 --replication-factor 1 &&
  kafka-topics --create --bootstrap-server kafka:29092
    --topic logs.anomalies --partitions 3 --replication-factor 1 &&
  echo 'Topics created successfully'
  "
```

Six partitions for `logs.raw` — this allows up to 6 consumer instances in the consumer group
to run in parallel. More partitions than consumers is fine; fewer partitions than consumers
means some consumers sit idle. Document this in TRADEOFFS.md.

**postgres**: image postgres:16-alpine. Same pattern as the ecosystem project.

**consumer**: build from ./services/consumer. Depends on kafka and postgres healthy.

**detector**: build from ./services/detector. Depends on postgres healthy.

**api**: build from ./services/api. Depends on postgres healthy. Expose 8000.

**simulator**: build from ./services/simulator. Depends on kafka healthy. Does not start
automatically — controlled via `docker-compose run simulator` with args for scenario and rate.

**prometheus** and **grafana**: same as ecosystem project. Add kafka-exporter (image
danielqsj/kafka-exporter) for Kafka metrics.

---

#### `infra/postgres/init.sql`

**logs table** — the primary store. This is a write-heavy, read-occasionally table. Design
the schema and indexes with that in mind.

```sql
CREATE TABLE IF NOT EXISTS logs (
    id           BIGSERIAL    PRIMARY KEY,
    log_id       UUID         NOT NULL UNIQUE,
    service_name VARCHAR(100) NOT NULL,
    level        VARCHAR(10)  NOT NULL CHECK (level IN ('DEBUG','INFO','WARN','ERROR','FATAL')),
    message      TEXT         NOT NULL,
    timestamp    TIMESTAMPTZ  NOT NULL,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    kafka_topic  VARCHAR(200) NOT NULL,
    kafka_partition INT        NOT NULL,
    kafka_offset    BIGINT     NOT NULL,
    metadata     JSONB        NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_logs_service_time
    ON logs (service_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_level_time
    ON logs (level, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_time
    ON logs (timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_logs_kafka_position
    ON logs (kafka_topic, kafka_partition, kafka_offset);

CREATE INDEX IF NOT EXISTS idx_logs_metadata_gin
    ON logs USING GIN (metadata);
```

The `kafka_partition` and `kafka_offset` columns are critical. They let you answer "have I
already processed this message?" — they are your idempotency key. They also let you replay
from a specific Kafka position by querying which offsets you have already stored.

The `ingested_at` vs `timestamp` distinction matters: `timestamp` is when the log event
occurred (set by the emitting service), `ingested_at` is when your pipeline stored it. The
delta between them is your pipeline latency. Surface this in Grafana.

**anomalies table** — one row per detected anomaly.

```sql
CREATE TABLE IF NOT EXISTS anomalies (
    id              BIGSERIAL    PRIMARY KEY,
    anomaly_id      UUID         NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    detected_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    anomaly_type    VARCHAR(50)  NOT NULL,
    service_name    VARCHAR(100),
    severity        VARCHAR(20)  NOT NULL CHECK (severity IN ('low','medium','high','critical')),
    window_start    TIMESTAMPTZ  NOT NULL,
    window_end      TIMESTAMPTZ  NOT NULL,
    affected_count  INT          NOT NULL,
    zscore          NUMERIC(8,4),
    description     TEXT         NOT NULL,
    log_ids         UUID[]       NOT NULL DEFAULT '{}',
    resolved        BOOLEAN      NOT NULL DEFAULT false,
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected
    ON anomalies (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_service_type
    ON anomalies (service_name, anomaly_type, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_anomalies_severity
    ON anomalies (severity, detected_at DESC)
    WHERE resolved = false;
```

The partial index on `anomalies (severity, detected_at DESC) WHERE resolved = false` is
a real optimization technique. The dashboard query "show me active anomalies by severity"
only cares about unresolved rows. A partial index on that subset is smaller and faster than
a full index. Know how to explain this — it is a SQL depth signal.

The `log_ids UUID[]` column stores an array of the log IDs that contributed to the anomaly.
PostgreSQL supports native array types. This is simpler than a junction table for this use
case because you never need to query "which anomalies contain log X" in the critical path.
Document the tradeoff: array is denormalized but read-efficient for the dashboard.

**detection_jobs table** — the Postgres-native task queue using `FOR UPDATE SKIP LOCKED`.

```sql
CREATE TABLE IF NOT EXISTS detection_jobs (
    id           BIGSERIAL    PRIMARY KEY,
    job_id       UUID         NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    scheduled_for TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    job_type     VARCHAR(50)  NOT NULL,
    payload      JSONB        NOT NULL DEFAULT '{}',
    attempts     INT          NOT NULL DEFAULT 0,
    last_error   TEXT,
    completed_at TIMESTAMPTZ,
    worker_id    VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_jobs_pending
    ON detection_jobs (scheduled_for ASC)
    WHERE status = 'pending';
```

The partial index on pending jobs is essential for the `FOR UPDATE SKIP LOCKED` query
performance. You will run this query every second from multiple worker processes. Without
the partial index, it scans all jobs. With it, it only scans pending jobs, which is a tiny
fraction of the table after the system has been running for a while.

**consumer_offsets table** — tracks which Kafka offsets the consumer has committed to
Postgres. This is separate from Kafka's internal offset tracking and gives you
application-level offset management.

```sql
CREATE TABLE IF NOT EXISTS consumer_offsets (
    topic      VARCHAR(200) NOT NULL,
    partition  INT          NOT NULL,
    offset     BIGINT       NOT NULL,
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);
```

---

#### `services/consumer/app/models.py`

Define Pydantic models for the log event schema. These are the contracts between the simulator
(producer) and the consumer. Both sides validate against the same model.

```python
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Optional
from enum import Enum

class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO  = "INFO"
    WARN  = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"

class LogEvent(BaseModel):
    log_id:       str              # UUID as string
    service_name: str              = Field(min_length=1, max_length=100)
    level:        LogLevel
    message:      str              = Field(min_length=1)
    timestamp:    datetime
    metadata:     dict             = Field(default_factory=dict)
    trace_id:     Optional[str]    = None
    span_id:      Optional[str]    = None
    duration_ms:  Optional[float]  = Field(default=None, ge=0)
    status_code:  Optional[int]    = Field(default=None, ge=100, le=599)

    @field_validator('timestamp')
    @classmethod
    def timestamp_must_not_be_future(cls, v: datetime) -> datetime:
        # Allow up to 5 seconds of clock skew
        from datetime import timezone
        now = datetime.now(timezone.utc)
        if v.tzinfo is None:
            raise ValueError('timestamp must be timezone-aware')
        return v
```

`duration_ms` and `status_code` are first-class fields, not buried in metadata. This is a
schema design decision: fields you will filter or aggregate on in SQL belong as columns, not
in JSONB. `metadata` holds everything else. Document this in TRADEOFFS.md.

---

#### `services/consumer/app/db.py`

Use `psycopg2` with a connection pool (not asyncpg) — the consumer runs in threads, not async
coroutines. asyncpg is an async library and does not work with threading correctly.
`psycopg2` is the right tool for a synchronous, threaded consumer.

```python
import psycopg2
from psycopg2 import pool as pg_pool
from contextlib import contextmanager

_pool: pg_pool.ThreadedConnectionPool | None = None

def init_pool(dsn: str, minconn: int = 2, maxconn: int = 10) -> None:
    global _pool
    _pool = pg_pool.ThreadedConnectionPool(minconn, maxconn, dsn)

@contextmanager
def get_connection():
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)
```

`ThreadedConnectionPool` is psycopg2's thread-safe pool. Each thread checks out a connection,
uses it, and returns it. The context manager handles commit/rollback automatically. Know
why you cannot use a single shared connection in a multithreaded consumer — psycopg2 connections
are not thread-safe. A connection pool is not optional here.

`batch_insert_logs(logs: list[LogEvent]) -> int` — insert a batch using `psycopg2.extras.execute_values`:

```python
from psycopg2.extras import execute_values

def batch_insert_logs(conn, logs: list[LogEvent]) -> int:
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO logs
                (log_id, service_name, level, message, timestamp,
                 kafka_topic, kafka_partition, kafka_offset, metadata)
            VALUES %s
            ON CONFLICT (log_id) DO NOTHING
        """, [
            (
                log.log_id, log.service_name, log.level.value, log.message,
                log.timestamp, log.kafka_topic, log.kafka_partition,
                log.kafka_offset, json.dumps(log.metadata)
            )
            for log in logs
        ])
        return cur.rowcount
```

`ON CONFLICT (log_id) DO NOTHING` is your idempotency guarantee. If the consumer crashes
after inserting but before committing the Kafka offset, the messages will be re-delivered.
The duplicate `log_id` will hit the conflict clause and be silently skipped. This is
at-least-once delivery with idempotent writes — the correct pattern for this use case.

---

#### `services/consumer/app/consumer.py`

This is the core Kafka consumer. It uses `confluent-kafka`'s `Consumer` class with manual
partition assignment callbacks and manual offset commits after successful database writes.

```python
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
import logging

logger = logging.getLogger(__name__)

class LogConsumer:
    def __init__(self, config: dict, on_batch: callable, batch_size: int = 500):
        self.consumer = Consumer(config)
        self.on_batch = on_batch
        self.batch_size = batch_size
        self._running = False

    def _on_assign(self, consumer, partitions):
        """Called when partitions are assigned during rebalance."""
        logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")
        # Seek to committed offsets (or beginning if no committed offset)
        for p in partitions:
            committed = consumer.committed([p])
            if committed[0].offset >= 0:
                p.offset = committed[0].offset
        consumer.assign(partitions)

    def _on_revoke(self, consumer, partitions):
        """Called when partitions are revoked during rebalance."""
        logger.info(f"Partitions revoked: {[p.partition for p in partitions]}")
        # Commit current offsets before giving up partitions
        consumer.commit(asynchronous=False)

    def run(self, topics: list[str]) -> None:
        self.consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke)
        self._running = True
        batch = []

        try:
            while self._running:
                msg = self.consumer.poll(timeout=0.1)

                if msg is None:
                    if batch:
                        self._flush_batch(batch)
                        batch = []
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                batch.append(msg)

                if len(batch) >= self.batch_size:
                    self._flush_batch(batch)
                    batch = []

        finally:
            if batch:
                self._flush_batch(batch)
            self.consumer.close()

    def _flush_batch(self, messages: list) -> None:
        try:
            self.on_batch(messages)
            # Only commit offsets after successful processing
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            # Do not commit — messages will be redelivered
            # In production: dead-letter queue for poison pills

    def stop(self) -> None:
        self._running = False
```

The `_on_revoke` callback committing synchronously before partition revocation is essential.
Without it, during a rebalance the new consumer owner of those partitions will re-process
messages from the last committed offset, which could be minutes behind. Know this and explain
it. It is the most common source of duplicate processing in Kafka consumer groups.

The batch-then-commit pattern (collect N messages, write to Postgres, then commit offsets)
is your core throughput mechanism. Individual commits per message would cap you at roughly
the Postgres write latency × message count. Batching amortizes the commit overhead across
500 messages at a time.

---

#### `services/consumer/app/pipeline.py`

The pipeline transforms raw Kafka messages into `LogEvent` objects, validates them, enriches
them, and routes them to storage. Each stage is a function that takes a list and returns a list.
This makes individual stages easy to test in isolation.

```python
import json
import logging
from datetime import datetime, timezone
from typing import NamedTuple
from .models import LogEvent

logger = logging.getLogger(__name__)

class PipelineResult(NamedTuple):
    accepted:  list[LogEvent]
    rejected:  list[dict]      # raw messages that failed validation
    parse_errors: int

def parse_messages(raw_messages: list) -> PipelineResult:
    """Stage 1: deserialize and validate."""
    accepted = []
    rejected = []
    parse_errors = 0

    for msg in raw_messages:
        try:
            data = json.loads(msg.value().decode('utf-8'))
            # Attach Kafka metadata before validation
            data['kafka_topic']     = msg.topic()
            data['kafka_partition'] = msg.partition()
            data['kafka_offset']    = msg.offset()
            event = LogEvent.model_validate(data)
            accepted.append(event)
        except json.JSONDecodeError:
            parse_errors += 1
            logger.warning(f"Invalid JSON at offset {msg.offset()}")
        except Exception as e:
            rejected.append({'raw': msg.value(), 'error': str(e)})

    return PipelineResult(accepted, rejected, parse_errors)

def enrich_events(events: list[LogEvent]) -> list[LogEvent]:
    """Stage 2: add derived fields."""
    now = datetime.now(timezone.utc)
    for event in events:
        # Compute pipeline latency and store in metadata
        lag_ms = (now - event.timestamp).total_seconds() * 1000
        event.metadata['pipeline_lag_ms'] = round(lag_ms, 2)

        # Tag high-latency events (useful for anomaly detection)
        if event.duration_ms is not None and event.duration_ms > 1000:
            event.metadata['high_latency'] = True

        # Tag error events for faster filtering
        if event.level.value in ('ERROR', 'FATAL'):
            event.metadata['is_error'] = True

    return events

def route_events(events: list[LogEvent]) -> tuple[list[LogEvent], list[LogEvent]]:
    """Stage 3: separate events that need immediate anomaly checking."""
    normal  = [e for e in events if e.level.value not in ('ERROR', 'FATAL')]
    urgent  = [e for e in events if e.level.value in ('ERROR', 'FATAL')]
    return normal, urgent
```

The pipeline is a pure transformation with no I/O. Every stage can be unit tested with a
list of mock messages and no database or Kafka required. This is the right design — test the
business logic without infrastructure dependencies.

---

#### `services/consumer/app/main.py`

Wire the pipeline together. The consumer runs in the main thread. The batch handler:

1. Calls `parse_messages` → `enrich_events` → `route_events`
2. Batch-inserts all accepted logs to Postgres
3. For urgent (error/fatal) logs, inserts a `detection_job` row to trigger immediate anomaly
   detection
4. Updates Prometheus counters

```python
from prometheus_client import Counter, Histogram, start_http_server

MESSAGES_CONSUMED  = Counter('kafka_messages_consumed_total',
                             'Messages consumed', ['topic', 'partition'])
MESSAGES_REJECTED  = Counter('kafka_messages_rejected_total', 'Messages failed validation')
BATCH_DURATION     = Histogram('consumer_batch_duration_seconds', 'Time to process a batch',
                               buckets=[.01, .05, .1, .25, .5, 1, 2.5, 5])
LOGS_STORED        = Counter('logs_stored_total', 'Logs written to postgres', ['service', 'level'])
PIPELINE_LAG       = Histogram('pipeline_lag_ms', 'Log timestamp to ingestion lag',
                               buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000])
```

Start the Prometheus server on port 8001 in a daemon thread. The consumer loop is the main
thread.

---

#### `services/simulator/app/distributions.py`

This file defines the statistical distributions that make the load test realistic. A uniform
random generator is not realistic — real services emit logs with Poisson-distributed inter-arrival
times, normally distributed latencies, and occasional bursts.

```python
import numpy as np
from dataclasses import dataclass

@dataclass
class ServiceProfile:
    """Statistical profile for a simulated service."""
    name:             str
    base_rps:         float    # baseline requests per second (Poisson rate)
    error_rate:       float    # fraction of requests that error (0.0 to 1.0)
    latency_mean_ms:  float    # mean request latency
    latency_std_ms:   float    # std deviation of latency (log-normal distribution)
    log_levels:       dict     # {'INFO': 0.8, 'WARN': 0.15, 'ERROR': 0.05}

    def sample_inter_arrival(self) -> float:
        """Time until next log event (exponential distribution — Poisson process)."""
        return np.random.exponential(1.0 / self.base_rps)

    def sample_latency(self) -> float:
        """Request latency in ms (log-normal — latencies are right-skewed)."""
        # Convert mean/std to log-normal parameters
        variance = self.latency_std_ms ** 2
        mu = np.log(self.latency_mean_ms ** 2 / np.sqrt(variance + self.latency_mean_ms ** 2))
        sigma = np.sqrt(np.log(1 + variance / self.latency_mean_ms ** 2))
        return float(np.random.lognormal(mu, sigma))

    def sample_level(self) -> str:
        levels = list(self.log_levels.keys())
        probs  = list(self.log_levels.values())
        return np.random.choice(levels, p=probs)

# Default service profiles
SERVICES = [
    ServiceProfile(
        name='api-gateway',
        base_rps=500,
        error_rate=0.01,
        latency_mean_ms=45,
        latency_std_ms=30,
        log_levels={'DEBUG': 0.05, 'INFO': 0.82, 'WARN': 0.10, 'ERROR': 0.03}
    ),
    ServiceProfile(
        name='order-service',
        base_rps=200,
        error_rate=0.02,
        latency_mean_ms=120,
        latency_std_ms=80,
        log_levels={'INFO': 0.85, 'WARN': 0.10, 'ERROR': 0.05}
    ),
    ServiceProfile(
        name='payment-processor',
        base_rps=100,
        error_rate=0.005,
        latency_mean_ms=350,
        latency_std_ms=200,
        log_levels={'INFO': 0.90, 'WARN': 0.08, 'ERROR': 0.02}
    ),
    ServiceProfile(
        name='inventory-service',
        base_rps=300,
        error_rate=0.008,
        latency_mean_ms=25,
        latency_std_ms=15,
        log_levels={'INFO': 0.88, 'WARN': 0.09, 'ERROR': 0.03}
    ),
    ServiceProfile(
        name='notification-service',
        base_rps=150,
        error_rate=0.03,
        latency_mean_ms=80,
        latency_std_ms=60,
        log_levels={'INFO': 0.80, 'WARN': 0.12, 'ERROR': 0.08}
    ),
]
```

The log-normal distribution for latency is not arbitrary — empirical measurements of web
service latencies consistently show log-normal distributions (right-skewed, occasional very
high outliers). Using numpy's statistical distributions makes your load test more realistic
than a random number generator and gives you something technically interesting to explain.

---

#### `services/simulator/app/scenarios.py`

Anomaly scenarios inject controlled failures into the normal distribution so you can verify
the detector works under load.

```python
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np

@dataclass
class AnomalyScenario:
    name:          str
    service_name:  str
    anomaly_type:  str    # 'error_spike', 'latency_surge', 'cascade_failure'
    duration_secs: float
    magnitude:     float  # multiplier (2.0 = 2x normal error rate)

SCENARIOS = [
    AnomalyScenario(
        name='payment-error-spike',
        service_name='payment-processor',
        anomaly_type='error_spike',
        duration_secs=30,
        magnitude=10.0      # 10x normal error rate
    ),
    AnomalyScenario(
        name='api-latency-surge',
        service_name='api-gateway',
        anomaly_type='latency_surge',
        duration_secs=60,
        magnitude=5.0       # 5x normal latency
    ),
    AnomalyScenario(
        name='cascade-failure',
        service_name='order-service',
        anomaly_type='cascade_failure',
        duration_secs=45,
        magnitude=20.0      # 20x normal error rate, affects downstream
    ),
]

class ScenarioScheduler:
    """Injects anomaly scenarios on a schedule during load tests."""

    def __init__(self, scenarios: list[AnomalyScenario], interval_secs: float = 120):
        self.scenarios = scenarios
        self.interval_secs = interval_secs
        self._active: AnomalyScenario | None = None
        self._active_until: datetime | None = None
        self._next_injection = datetime.now()

    def tick(self, now: datetime) -> AnomalyScenario | None:
        """Call every second. Returns active scenario or None."""
        # Check if active scenario has expired
        if self._active and now >= self._active_until:
            self._active = None
            self._active_until = None

        # Inject next scenario on schedule
        if self._active is None and now >= self._next_injection:
            self._active = np.random.choice(self.scenarios)
            self._active_until = now + timedelta(seconds=self._active.duration_secs)
            self._next_injection = self._active_until + timedelta(seconds=self.interval_secs)

        return self._active
```

---

#### `services/simulator/app/producer.py`

The Kafka producer. Uses `confluent-kafka`'s async Producer with a delivery callback.

```python
from confluent_kafka import Producer
from .distributions import ServiceProfile, SERVICES
from .scenarios import ScenarioScheduler, SCENARIOS
import json, uuid, time, logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class LoadSimulator:
    def __init__(self, kafka_config: dict, target_rps: int = 1000):
        self.producer   = Producer(kafka_config)
        self.target_rps = target_rps
        self.scheduler  = ScenarioScheduler(SCENARIOS)
        self._delivered = 0
        self._errors    = 0

    def _delivery_callback(self, err, msg):
        if err:
            self._errors += 1
            logger.error(f"Delivery failed: {err}")
        else:
            self._delivered += 1

    def _generate_event(self, profile: ServiceProfile,
                        scenario=None) -> dict:
        level   = profile.sample_level()
        latency = profile.sample_latency()

        # Apply scenario multipliers
        if scenario and scenario.service_name == profile.name:
            if scenario.anomaly_type == 'error_spike':
                # Force ERROR level with scenario probability
                if np.random.random() < min(profile.error_rate * scenario.magnitude, 0.95):
                    level = 'ERROR'
            elif scenario.anomaly_type == 'latency_surge':
                latency *= scenario.magnitude

        return {
            'log_id':       str(uuid.uuid4()),
            'service_name': profile.name,
            'level':        level,
            'message':      self._generate_message(level, profile.name, latency),
            'timestamp':    datetime.now(timezone.utc).isoformat(),
            'duration_ms':  round(latency, 2),
            'metadata':     {'region': 'us-east-1', 'version': '1.0.0'},
        }

    def _generate_message(self, level: str, service: str, latency: float) -> str:
        templates = {
            'INFO':  f"Request processed successfully in {latency:.1f}ms",
            'WARN':  f"Slow response detected: {latency:.1f}ms exceeds threshold",
            'ERROR': f"Request failed after {latency:.1f}ms: internal server error",
            'FATAL': f"Service {service} unrecoverable error, initiating shutdown",
            'DEBUG': f"Processing request, current latency: {latency:.1f}ms",
        }
        return templates.get(level, f"Log event from {service}")

    def run(self, duration_secs: float | None = None) -> None:
        """Produce at target_rps for duration_secs (or forever if None)."""
        import numpy as np

        start    = time.monotonic()
        interval = 1.0 / self.target_rps
        next_send = time.monotonic()

        while True:
            now_mono = time.monotonic()
            now_dt   = datetime.now(timezone.utc)

            if duration_secs and (now_mono - start) >= duration_secs:
                break

            scenario = self.scheduler.tick(now_dt)
            profile  = np.random.choice(SERVICES)
            event    = self._generate_event(profile, scenario)

            self.producer.produce(
                topic    = 'logs.raw',
                key      = profile.name.encode(),
                value    = json.dumps(event).encode(),
                callback = self._delivery_callback,
            )

            # Poll for delivery callbacks
            self.producer.poll(0)

            # Rate limiting — sleep until next scheduled send
            next_send += interval
            sleep_time = next_send - time.monotonic()
            if sleep_time > 0:
                time.sleep(sleep_time)

        self.producer.flush()
        logger.info(f"Simulation complete. Delivered: {self._delivered}, Errors: {self._errors}")
```

The rate limiting logic (tracking `next_send` and sleeping the difference) produces a more
accurate rate than `time.sleep(1/rps)` in a loop — the latter drifts because processing time
is not accounted for. Document this in LOAD_TEST_RESULTS.md.

The message key is the service name. Kafka routes messages with the same key to the same
partition. This means all logs from `payment-processor` land on the same partition, preserving
per-service ordering. Know this and be able to explain it.

---

### Week 1 Definition of Done

- [ ] `make up` starts all services including Kafka and Zookeeper with no errors
- [ ] `logs.raw` and `logs.anomalies` topics visible in Kafka (verify with kafka-topics --list)
- [ ] Consumer starts, logs partition assignments
- [ ] Simulator produces at 1,000 events/sec for 60 seconds without errors
- [ ] Logs appearing in Postgres (verify with psql: `SELECT COUNT(*) FROM logs`)
- [ ] `ON CONFLICT (log_id) DO NOTHING` verified: run simulator twice, row count does not double
- [ ] Consumer partition assignment callbacks logging on startup
- [ ] Prometheus metrics endpoint at consumer:8001/metrics showing messages_consumed_total
- [ ] `make test` passes: unit tests for parse_messages, enrich_events, route_events, and
      sample_inter_arrival (no Kafka or Postgres required)

---

## Week 2 — Anomaly Detector + Detection Job Queue + SQL Window Functions

**Sprint goal**: Three anomaly detectors running (Z-score, IQR, co-occurrence). The detection
job queue uses `SELECT ... FOR UPDATE SKIP LOCKED`. All detection logic tested in isolation
with synthetic data. EXPLAIN ANALYZE documented for the window function queries.

### Reading Assignments

**"Fluent Python" — Luciano Ramalho**

- Chapter 20 (Concurrent Executors): The section on `ProcessPoolExecutor`. Your anomaly detectors
  run CPU-bound scipy computations. Running them in the asyncio event loop would block the loop.
  Running them in a `ProcessPoolExecutor` moves them to a separate process, bypassing the GIL.
  Read this chapter before designing the detector worker.
- Chapter 18 (with Statements and Context Managers): Your psycopg2 connection context manager
  from Week 1 is an example of this. Read the chapter to understand what `__enter__` and
  `__exit__` do, and how `contextlib.contextmanager` works. The detection job queue claims and
  releases jobs via context managers.

**"Designing Data-Intensive Applications" — Kleppmann**

- Chapter 12 (The Future of Data Systems): The section on derived data and the relationship
  between event logs and derived views. Your anomaly table is a derived view — it is computed
  from the events in the logs table. Understanding this relationship helps you explain why the
  anomaly table can always be rebuilt from the log data, which is a correctness argument.

**NumPy and SciPy documentation**

- numpy.org/doc: "Statistics" section. Understand `np.mean`, `np.std`, `np.percentile`.
  Know the difference between population std (`ddof=0`) and sample std (`ddof=1`). Your
  Z-score detector uses sample std because you are working with a sample of the full log
  population.
- scipy.org/doc: `scipy.stats.zscore` and `scipy.stats.iqr`. Read the docstrings. Understand
  the `nan_policy` parameter — your window may have missing data if a service was quiet. Know
  how to handle NaN gracefully.

### Blog Posts to Read This Week

- "SELECT FOR UPDATE SKIP LOCKED in PostgreSQL" — 2ndquadrant.com or pganalyze.com.
  The definitive explanation. Read it before implementing the job queue.
- "Statistical Anomaly Detection Techniques" — any practical survey (towardsdatascience.com
  has several). Focus on Z-score and IQR methods — the math is simple but the implementation
  decisions (window size, threshold choice) are where the interesting design work happens.
- "Python multiprocessing vs threading vs asyncio" — realpython.com. The three concurrency
  models in one article. Cement your understanding before the interview question arrives.
- "SQL Window Functions" — mode.com/sql-tutorial/sql-window-functions. The most accessible
  tutorial. Work through all the examples before writing your co-occurrence query.

---

### Files to Implement This Week

---

#### `services/detector/app/worker.py`

The detector worker claims jobs from the `detection_jobs` table using
`SELECT ... FOR UPDATE SKIP LOCKED`, runs the appropriate detector, and writes anomalies.

```python
import psycopg2
import logging
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from .db import get_connection
from .detectors.zscore import ZScoreDetector
from .detectors.iqr import IQRDetector
from .detectors.cooccurrence import CooccurrenceDetector

logger = logging.getLogger(__name__)

DETECTORS = {
    'zscore_error_rate':  ZScoreDetector,
    'iqr_latency':        IQRDetector,
    'cooccurrence':       CooccurrenceDetector,
}

class DetectorWorker:
    def __init__(self, worker_id: str, poll_interval_secs: float = 1.0):
        self.worker_id     = worker_id
        self.poll_interval = poll_interval_secs
        self.executor      = ProcessPoolExecutor(max_workers=2)
        self._running      = False

    def _claim_job(self, conn) -> dict | None:
        """Claim one pending job using FOR UPDATE SKIP LOCKED."""
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE detection_jobs
                SET    status    = 'running',
                       worker_id = %s,
                       attempts  = attempts + 1
                WHERE  id = (
                    SELECT id
                    FROM   detection_jobs
                    WHERE  status        = 'pending'
                      AND  scheduled_for <= now()
                    ORDER  BY scheduled_for ASC
                    LIMIT  1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, job_id, job_type, payload
            """, (self.worker_id,))
            conn.commit()
            row = cur.fetchone()
            if row:
                return {'id': row[0], 'job_id': row[1],
                        'job_type': row[2], 'payload': row[3]}
        return None

    def _complete_job(self, conn, job_id: str) -> None:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE detection_jobs
                SET status = 'completed', completed_at = now()
                WHERE job_id = %s
            """, (job_id,))
            conn.commit()

    def _fail_job(self, conn, job_id: str, error: str, max_attempts: int = 3) -> None:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE detection_jobs
                SET status     = CASE WHEN attempts >= %s THEN 'failed' ELSE 'pending' END,
                    last_error = %s,
                    scheduled_for = CASE
                        WHEN attempts >= %s THEN scheduled_for
                        ELSE now() + (attempts * interval '30 seconds')
                    END
                WHERE job_id = %s
            """, (max_attempts, error, max_attempts, job_id))
            conn.commit()

    def run(self) -> None:
        self._running = True
        logger.info(f"Detector worker {self.worker_id} started")

        while self._running:
            with get_connection() as conn:
                job = self._claim_job(conn)

            if job is None:
                time.sleep(self.poll_interval)
                continue

            try:
                detector_class = DETECTORS.get(job['job_type'])
                if not detector_class:
                    raise ValueError(f"Unknown job type: {job['job_type']}")

                detector = detector_class()
                anomalies = detector.run(job['payload'])

                with get_connection() as conn:
                    for anomaly in anomalies:
                        self._store_anomaly(conn, anomaly)
                    self._complete_job(conn, job['job_id'])

            except Exception as e:
                logger.error(f"Job {job['job_id']} failed: {e}")
                with get_connection() as conn:
                    self._fail_job(conn, job['job_id'], str(e))
```

The `FOR UPDATE SKIP LOCKED` pattern is the central interview moment for this service.
Explain it this way: `SELECT ... FOR UPDATE` locks the selected rows. `SKIP LOCKED` tells
Postgres to skip any rows that are already locked rather than waiting. The result: multiple
worker processes can run this query simultaneously and each gets a different job. It is
Postgres-native work queue semantics without Redis or a dedicated queue service. Know the
limitations too: it does not support priorities (you sort by `scheduled_for`), and it holds
a database connection for the duration of the job. Document both in TRADEOFFS.md.

The retry logic with exponential backoff (increasing `scheduled_for` by attempts × 30 seconds)
is done entirely in SQL with a CASE expression, requiring no application-side retry loop.

---

#### `services/detector/app/detectors/zscore.py`

Z-score anomaly detection on error rates. A Z-score measures how many standard deviations
a value is from the mean of its reference window. A Z-score above 3.0 indicates an anomaly
with 99.7% confidence (assuming a normal distribution — log this assumption as a limitation).

```python
import numpy as np
import psycopg2
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from ..db import get_connection

@dataclass
class AnomalyResult:
    anomaly_type:  str
    service_name:  str
    severity:      str
    window_start:  datetime
    window_end:    datetime
    affected_count: int
    zscore:        float
    description:   str
    log_ids:       list[str]

class ZScoreDetector:
    """
    Detects error rate spikes using Z-score over a rolling 30-minute window.

    Approach:
      - Bucket error counts into 1-minute bins over the last 30 minutes
      - Compute Z-score of the most recent bin against the prior 29 bins
      - Flag if Z-score > threshold (default 3.0)

    Assumption: error counts per minute are approximately normally distributed
    at steady state. Violated during cascade failures, but still useful as a
    first-pass filter. Document this assumption.
    """

    WINDOW_MINUTES   = 30
    Z_THRESHOLD      = 3.0
    REFERENCE_BINS   = 29  # use 29 prior bins to score the 30th

    def run(self, payload: dict) -> list[AnomalyResult]:
        service_name = payload.get('service_name')
        end_time     = datetime.now(timezone.utc)
        start_time   = end_time - timedelta(minutes=self.WINDOW_MINUTES)

        error_counts = self._query_error_counts(service_name, start_time, end_time)

        if len(error_counts) < 5:
            return []  # Not enough data for meaningful detection

        counts = np.array([row['error_count'] for row in error_counts])
        reference = counts[:-1]  # all but the last bin
        current   = counts[-1]

        if reference.std(ddof=1) == 0:
            return []  # No variance in reference — can't compute Z-score

        zscore = (current - reference.mean()) / reference.std(ddof=1)

        if zscore <= self.Z_THRESHOLD:
            return []

        severity = self._classify_severity(zscore)
        log_ids  = self._fetch_error_log_ids(service_name, error_counts[-1]['bucket'])

        return [AnomalyResult(
            anomaly_type   = 'error_spike',
            service_name   = service_name,
            severity       = severity,
            window_start   = start_time,
            window_end     = end_time,
            affected_count = int(current),
            zscore         = round(float(zscore), 4),
            description    = (
                f"Error rate spike detected for {service_name}. "
                f"Current: {current:.0f} errors/min, "
                f"Expected: {reference.mean():.1f} ± {reference.std(ddof=1):.1f}. "
                f"Z-score: {zscore:.2f}"
            ),
            log_ids = log_ids,
        )]

    def _query_error_counts(self, service_name: str | None,
                            start: datetime, end: datetime) -> list[dict]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        date_trunc('minute', timestamp) AS bucket,
                        COUNT(*)                        AS error_count
                    FROM logs
                    WHERE level IN ('ERROR', 'FATAL')
                      AND timestamp BETWEEN %s AND %s
                      AND (%s IS NULL OR service_name = %s)
                    GROUP BY 1
                    ORDER BY 1
                """, (start, end, service_name, service_name))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]

    def _classify_severity(self, zscore: float) -> str:
        if zscore > 10: return 'critical'
        if zscore > 6:  return 'high'
        if zscore > 4:  return 'medium'
        return 'low'

    def _fetch_error_log_ids(self, service_name: str | None,
                             bucket: datetime) -> list[str]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT log_id FROM logs
                    WHERE level IN ('ERROR', 'FATAL')
                      AND date_trunc('minute', timestamp) = %s
                      AND (%s IS NULL OR service_name = %s)
                    LIMIT 50
                """, (bucket, service_name, service_name))
                return [str(row[0]) for row in cur.fetchall()]
```

---

#### `services/detector/app/detectors/cooccurrence.py`

Co-occurrence detection: "find log lines from service X that co-occur within 500ms of an
error in service Y." This requires SQL window functions.

```python
class CooccurrenceDetector:
    """
    Detects cascade failures via temporal co-occurrence.

    A cascade failure: errors in service A are followed by errors in
    service B within a short time window, suggesting A's failure caused B's.

    Uses SQL window functions (LAG/LEAD) to find temporally adjacent
    error pairs across service boundaries.
    """

    WINDOW_MS      = 500   # co-occurrence window in milliseconds
    MIN_PAIRS      = 5     # minimum co-occurring pairs to flag
    LOOKBACK_MINS  = 10

    def run(self, payload: dict) -> list[AnomalyResult]:
        pairs = self._query_cooccurring_errors()
        results = []

        for (svc_a, svc_b), log_ids in pairs.items():
            if len(log_ids) < self.MIN_PAIRS:
                continue

            results.append(AnomalyResult(
                anomaly_type   = 'cascade_failure',
                service_name   = svc_a,
                severity       = 'high' if len(log_ids) > 20 else 'medium',
                window_start   = datetime.now(timezone.utc) - timedelta(minutes=self.LOOKBACK_MINS),
                window_end     = datetime.now(timezone.utc),
                affected_count = len(log_ids),
                zscore         = 0.0,
                description    = (
                    f"Cascade failure detected: errors in {svc_a} co-occur with "
                    f"errors in {svc_b} within {self.WINDOW_MS}ms "
                    f"({len(log_ids)} co-occurring pairs)"
                ),
                log_ids = log_ids[:50],
            ))

        return results

    def _query_cooccurring_errors(self) -> dict:
        """
        Window function query: for each error in service A, find errors in
        service B within WINDOW_MS milliseconds using LEAD/LAG.

        This is the hard SQL query in the project. Run EXPLAIN ANALYZE on it
        and document the plan in QUERY_ANALYSIS.md.
        """
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    WITH recent_errors AS (
                        SELECT
                            log_id,
                            service_name,
                            timestamp,
                            LEAD(service_name) OVER (ORDER BY timestamp) AS next_service,
                            LEAD(timestamp)    OVER (ORDER BY timestamp) AS next_timestamp,
                            LEAD(log_id)       OVER (ORDER BY timestamp) AS next_log_id
                        FROM logs
                        WHERE level     IN ('ERROR', 'FATAL')
                          AND timestamp >  now() - interval '%s minutes'
                    )
                    SELECT
                        service_name,
                        next_service,
                        log_id,
                        next_log_id
                    FROM recent_errors
                    WHERE next_service IS NOT NULL
                      AND next_service  != service_name
                      AND EXTRACT(EPOCH FROM (next_timestamp - timestamp)) * 1000
                          <= %s
                """, (self.LOOKBACK_MINS, self.WINDOW_MS))

                pairs: dict = {}
                for row in cur.fetchall():
                    key = (row[0], row[1])
                    if key not in pairs:
                        pairs[key] = []
                    pairs[key].extend([str(row[2]), str(row[3])])

                return pairs
```

The co-occurrence query is the SQL centerpiece of the project. Write it, run
`EXPLAIN (ANALYZE, BUFFERS)` on it, and document the output in QUERY_ANALYSIS.md. The
interesting plan detail: the `LEAD` window function requires a sort on `timestamp`. Without
the `idx_logs_time` index, this sort happens in memory. With the index, Postgres can use an
index scan in order and skip the sort entirely. Show both plans.

---

#### Tests for Detectors (No Database Required)

The detectors have two layers of logic: the SQL queries and the statistical computation.
Test the statistical computation in isolation by passing synthetic numpy arrays directly.

`services/detector/tests/test_zscore.py`:

```python
import numpy as np
import pytest
from unittest.mock import patch, MagicMock
from app.detectors.zscore import ZScoreDetector

class TestZScoreDetector:

    def test_no_anomaly_with_stable_error_rates(self):
        detector = ZScoreDetector()
        # Stable error counts: mean=10, std=2, last bin=11 (Z=0.5)
        counts = [10, 9, 11, 10, 12, 9, 10, 11, 10, 9, 11]
        mock_rows = [{'bucket': None, 'error_count': c} for c in counts]

        with patch.object(detector, '_query_error_counts', return_value=mock_rows):
            with patch.object(detector, '_fetch_error_log_ids', return_value=[]):
                results = detector.run({'service_name': 'test-service'})

        assert results == []

    def test_detects_spike_above_threshold(self):
        detector = ZScoreDetector()
        # Last bin is 10x the mean — clear anomaly
        counts = [10, 9, 11, 10, 12, 9, 10, 11, 10, 9, 100]
        mock_rows = [{'bucket': None, 'error_count': c} for c in counts]

        with patch.object(detector, '_query_error_counts', return_value=mock_rows):
            with patch.object(detector, '_fetch_error_log_ids', return_value=['id1','id2']):
                results = detector.run({'service_name': 'payment-processor'})

        assert len(results) == 1
        assert results[0].anomaly_type == 'error_spike'
        assert results[0].zscore > 3.0
        assert results[0].severity in ('high', 'critical')

    def test_returns_empty_with_insufficient_data(self):
        detector = ZScoreDetector()
        mock_rows = [{'bucket': None, 'error_count': 5}]  # only 1 bin

        with patch.object(detector, '_query_error_counts', return_value=mock_rows):
            results = detector.run({'service_name': 'test'})

        assert results == []

    def test_severity_classification(self):
        detector = ZScoreDetector()
        assert detector._classify_severity(3.5)  == 'low'
        assert detector._classify_severity(5.0)  == 'medium'
        assert detector._classify_severity(8.0)  == 'high'
        assert detector._classify_severity(15.0) == 'critical'

    @pytest.mark.parametrize("counts,expected_anomaly", [
        ([10]*29 + [11], False),   # Z=0.5 — no anomaly
        ([10]*29 + [40], True),    # Z~30 — clear anomaly
        ([5, 10, 5, 10]*7 + [9], False),  # noisy but last bin is within range
    ])
    def test_parametrized_anomaly_detection(self, counts, expected_anomaly):
        detector = ZScoreDetector()
        mock_rows = [{'bucket': None, 'error_count': c} for c in counts]

        with patch.object(detector, '_query_error_counts', return_value=mock_rows):
            with patch.object(detector, '_fetch_error_log_ids', return_value=[]):
                results = detector.run({'service_name': 'svc'})

        assert bool(results) == expected_anomaly
```

These tests run in milliseconds with no infrastructure. They test the statistical logic,
the threshold classification, and the edge cases. They are the kind of tests that demonstrate
engineering discipline — you are not just testing that the function runs, you are testing the
boundary conditions that define correct behavior.

`services/consumer/tests/test_pipeline.py`:

```python
import pytest
from unittest.mock import MagicMock
from app.pipeline import parse_messages, enrich_events, route_events

def make_mock_message(data: dict, offset: int = 0):
    msg = MagicMock()
    msg.value.return_value = json.dumps(data).encode()
    msg.offset.return_value = offset
    msg.partition.return_value = 0
    msg.topic.return_value = 'logs.raw'
    msg.error.return_value = None
    return msg

class TestParsePipeline:

    def test_valid_message_accepted(self):
        msg = make_mock_message({
            'log_id': str(uuid.uuid4()),
            'service_name': 'api-gateway',
            'level': 'INFO',
            'message': 'Request handled',
            'timestamp': '2024-01-15T10:00:00+00:00',
        })
        result = parse_messages([msg])
        assert len(result.accepted) == 1
        assert result.rejected == []
        assert result.parse_errors == 0

    def test_invalid_json_counted_as_parse_error(self):
        msg = MagicMock()
        msg.value.return_value = b'not json {'
        msg.error.return_value = None
        result = parse_messages([msg])
        assert result.parse_errors == 1
        assert result.accepted == []

    def test_schema_violation_goes_to_rejected(self):
        msg = make_mock_message({'log_id': 'abc', 'level': 'INVALID_LEVEL'})
        result = parse_messages([msg])
        assert len(result.rejected) == 1
        assert result.accepted == []

    def test_enrichment_adds_pipeline_lag(self):
        events = parse_messages([make_mock_message({
            'log_id': str(uuid.uuid4()),
            'service_name': 'svc',
            'level': 'INFO',
            'message': 'ok',
            'timestamp': '2024-01-15T10:00:00+00:00',
        })]).accepted

        enriched = enrich_events(events)
        assert 'pipeline_lag_ms' in enriched[0].metadata

    def test_error_events_routed_to_urgent(self):
        events = [
            make_log_event(level='INFO'),
            make_log_event(level='ERROR'),
            make_log_event(level='FATAL'),
            make_log_event(level='WARN'),
        ]
        normal, urgent = route_events(events)
        assert len(normal) == 2
        assert len(urgent) == 2
```

---

### Week 2 Definition of Done

- [ ] Z-score detector running, anomalies appearing in Postgres after injected error spikes
- [ ] IQR latency detector running
- [ ] Co-occurrence detector running, cascade failures detected within 60 seconds of injection
- [ ] `FOR UPDATE SKIP LOCKED` verified: run two detector workers simultaneously, confirm no job
      is processed twice (check that completed_at is set exactly once per job)
- [ ] All detector unit tests pass with no database (100% mock-based)
- [ ] All consumer pipeline tests pass
- [ ] QUERY_ANALYSIS.md has EXPLAIN ANALYZE for the window function co-occurrence query
- [ ] Retry logic verified: manually set a job to 'pending' with attempts=2, confirm it gets
      scheduled 60 seconds out (2 × 30 seconds)

---

## Week 3 — Query API + Load Testing at Scale

**Sprint goal**: FastAPI query API complete. Load simulator runs at 10,000 events/sec for
5 minutes. Consumer lag stays below 10 seconds. Results documented in LOAD_TEST_RESULTS.md.

### Reading Assignments

**"Fluent Python" — Luciano Ramalho**

- Chapter 21 (Asynchronous Programming): Re-read the asyncio section with your Week 1 consumer
  code in mind. The consumer uses threads (psycopg2 + confluent-kafka). The API uses asyncio
  (asyncpg + FastAPI). Understand why you did not use asyncio for the consumer: confluent-kafka's
  Python client is not async-native. Wrapping it in asyncio requires `loop.run_in_executor`,
  which adds complexity without measurable benefit at this scale. The threaded consumer is simpler
  and correct. Document this in TRADEOFFS.md.

**Locust documentation — docs.locust.io**

- "Writing a Locustfile" and "Running Locust": Read the getting started guide. You will write
  a locustfile that simulates concurrent API users querying the log search and anomaly endpoints
  while the Kafka simulator is running. Locust measures p50/p95/p99 response times and requests
  per second — these numbers go in LOAD_TEST_RESULTS.md.

### Blog Posts to Read This Week

- "asyncpg connection pool best practices" — any practical article. Understand pool exhaustion:
  if all pool connections are in use and a new request arrives, it waits. If it waits too long,
  it raises a timeout. Your API must handle this gracefully.
- "FastAPI background tasks vs Celery" — testdriven.io. Understand when FastAPI's built-in
  `BackgroundTasks` is sufficient (lightweight, single-process) vs when you need a Celery worker
  (heavy CPU, retries, distributed). Your detection jobs use Postgres as the queue — know how
  this compares to both options.

---

### Files to Implement This Week

---

#### `services/api/app/routes/logs.py`

**`GET /api/logs/search`**

Parameters: `service_name: str | None`, `level: str | None`, `since: datetime`,
`until: datetime | None`, `message_contains: str | None`, `limit: int = 100`.

```python
@router.get("/api/logs/search", response_model=list[LogSearchResult])
async def search_logs(
    service_name:      str | None     = Query(None),
    level:             str | None     = Query(None),
    since:             datetime       = Query(...),
    until:             datetime | None = Query(None),
    message_contains:  str | None     = Query(None),
    limit:             int            = Query(default=100, le=1000),
    pool: asyncpg.Pool = Depends(get_pool),
):
```

Build the query dynamically. Do not use f-strings. Use parameterized queries with a conditions
list pattern:

```python
conditions = ["timestamp >= $1"]
params = [since]
param_idx = 2

if until:
    conditions.append(f"timestamp <= ${param_idx}")
    params.append(until)
    param_idx += 1

if service_name:
    conditions.append(f"service_name = ${param_idx}")
    params.append(service_name)
    param_idx += 1

if level:
    conditions.append(f"level = ${param_idx}")
    params.append(level.upper())
    param_idx += 1

if message_contains:
    conditions.append(f"message ILIKE ${param_idx}")
    params.append(f"%{message_contains}%")
    param_idx += 1

where_clause = " AND ".join(conditions)
query = f"""
    SELECT log_id, service_name, level, message, timestamp,
           kafka_partition, kafka_offset, metadata
    FROM   logs
    WHERE  {where_clause}
    ORDER  BY timestamp DESC
    LIMIT  ${param_idx}
"""
params.append(limit)
rows = await pool.fetch(query, *params)
```

The dynamic query builder is a deliberate design decision. An ORM would handle this with
keyword arguments. Raw asyncpg requires you to build it manually. The tradeoff: you have
full control over the query and can see exactly what Postgres receives; the ORM abstracts
that away. Know this argument.

**`GET /api/logs/stats`**

Returns per-service log counts by level for the last hour, using a GROUP BY query with
a pivot-style aggregation:

```sql
SELECT
    service_name,
    COUNT(*) FILTER (WHERE level = 'INFO')  AS info_count,
    COUNT(*) FILTER (WHERE level = 'WARN')  AS warn_count,
    COUNT(*) FILTER (WHERE level = 'ERROR') AS error_count,
    COUNT(*) FILTER (WHERE level = 'FATAL') AS fatal_count,
    AVG(CASE WHEN (metadata->>'duration_ms') IS NOT NULL
        THEN (metadata->>'duration_ms')::FLOAT END) AS avg_duration_ms
FROM logs
WHERE timestamp > now() - INTERVAL '1 hour'
GROUP BY service_name
ORDER BY (error_count + fatal_count) DESC
```

The `COUNT(*) FILTER (WHERE ...)` syntax is a PostgreSQL conditional aggregate. It is cleaner
than a CASE inside COUNT and more readable than multiple subqueries. Document it in
QUERY_ANALYSIS.md — it is a SQL technique worth knowing.

---

#### `services/api/app/routes/anomalies.py`

**`GET /api/anomalies`** — returns active anomalies, most severe first:

```sql
SELECT
    anomaly_id,
    anomaly_type,
    service_name,
    severity,
    detected_at,
    window_start,
    window_end,
    affected_count,
    zscore,
    description,
    array_length(log_ids, 1) AS log_count
FROM anomalies
WHERE resolved = false
  AND detected_at > now() - INTERVAL '24 hours'
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'low'      THEN 4
    END,
    detected_at DESC
LIMIT $1
```

The `CASE` in the `ORDER BY` clause is the idiomatic way to sort by an enum-like string
column with a defined priority order. Know this — it comes up.

**`POST /api/anomalies/{anomaly_id}/resolve`** — marks an anomaly resolved. Updates
`resolved = true` and `resolved_at = now()`. Returns 404 if not found, 409 if already
resolved. The partial index on `(severity, detected_at DESC) WHERE resolved = false` means
this row immediately falls out of the index on the next query, which is the intended behavior.

---

#### `services/simulator/app/main.py` — Load Test Scenarios

Add CLI argument parsing so the simulator can be run with different parameters:

```
docker-compose run simulator --rps 1000 --duration 300 --scenario payment-error-spike
docker-compose run simulator --rps 10000 --duration 60 --no-scenario
docker-compose run simulator --rps 5000 --duration 600 --scenario cascade-failure
```

The `--rps 10000` scenario is your heavy load test. At 10,000 events/sec for 60 seconds,
you produce 600,000 messages. Your consumer must keep up.

Add a `LoadTestReporter` that logs throughput statistics every 10 seconds:

- Messages produced (total and per second)
- Delivery failures
- Estimated Kafka consumer lag (by comparing produced offsets to committed offsets,
  queried via the Kafka admin client)

---

#### `services/simulator/tests/load_test.py` — Locust API Load Test

```python
from locust import HttpUser, task, between, events
from datetime import datetime, timezone, timedelta
import random

class LogAPIUser(HttpUser):
    """Simulates concurrent users querying the log search and anomaly APIs."""

    wait_time = between(0.1, 0.5)

    SERVICES = ['api-gateway', 'order-service', 'payment-processor',
                'inventory-service', 'notification-service']

    @task(3)
    def search_recent_logs(self):
        since = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        service = random.choice(self.SERVICES)
        self.client.get(
            f"/api/logs/search?service_name={service}&since={since}&limit=50",
            name="/api/logs/search [by service]"
        )

    @task(2)
    def search_error_logs(self):
        since = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        self.client.get(
            f"/api/logs/search?level=ERROR&since={since}&limit=100",
            name="/api/logs/search [errors]"
        )

    @task(1)
    def get_active_anomalies(self):
        self.client.get("/api/anomalies?limit=20")

    @task(1)
    def get_log_stats(self):
        self.client.get("/api/logs/stats")

    @task(1)
    def get_pipeline_health(self):
        self.client.get("/api/pipeline/health")
```

Run: `locust -f services/simulator/tests/load_test.py --host http://localhost:8000 --users 50 --spawn-rate 10 --run-time 120s --headless --csv load_test_results`

Document the output CSV in LOAD_TEST_RESULTS.md. The numbers that matter:
p50, p95, p99 for each endpoint. Requests per second at peak. Failure rate.

---

### Week 3 Definition of Done

- [ ] `GET /api/logs/search` returns correct results with all filter combinations
- [ ] `GET /api/anomalies` returns anomalies sorted by severity
- [ ] `POST /api/anomalies/:id/resolve` updates the row and the partial index
- [ ] Simulator runs at 10,000 events/sec for 60 seconds: consumer lag stays below 30 seconds
- [ ] Locust load test at 50 concurrent users: p95 < 200ms for all endpoints
- [ ] LOAD_TEST_RESULTS.md has full locust output including p50/p95/p99
- [ ] Consumer throughput metric visible in Grafana during load test

---

## Week 4 — Observability, SQL Analysis, Terraform, Documentation

**Sprint goal**: Grafana dashboards complete. EXPLAIN ANALYZE documented. Terraform deploys to
EC2. All documentation portfolio-ready. Load test results analyzed and written up.

### Reading Assignments — same as Ecosystem Project Week 4

**"Release It!" — Nygard**: Chapter 3 (Stability Patterns). Apply circuit breaker thinking to:
what happens if Postgres is slow under load? The detector worker holds a connection for the
duration of each job. Under load, the pool exhausts. Know the failure mode.

**"Systems Performance" — Gregg**: The USE Method applied to your system. For Kafka: utilization
= consumer lag / acceptable lag, saturation = producer blocking on full internal queue, errors =
delivery failures. Write this analysis in ARCHITECTURE.md.

---

### Files to Implement This Week

---

#### `infra/grafana/dashboards/pipeline.json`

**Row 1: Ingestion**

- "Consumer Throughput" — `rate(kafka_messages_consumed_total[30s])`. Should match simulator
  output during load tests.
- "Consumer Lag" — `kafka_consumer_group_lag` from kafka-exporter. This is the most important
  operational metric. If it grows without bound, the consumer cannot keep up.
- "Batch Processing Duration p95" — `histogram_quantile(0.95, rate(consumer_batch_duration_seconds_bucket[1m]))`.
- "Pipeline Latency" — `histogram_quantile(0.95, rate(pipeline_lag_ms_bucket[1m]))`. The delta
  between log timestamp and ingestion timestamp. Measures end-to-end pipeline latency.

**Row 2: Detection**

- "Anomalies Detected/hour" — `increase(anomalies_stored_total[1h])` grouped by anomaly_type.
- "Detection Job Queue Depth" — custom query from Postgres via the Grafana PostgreSQL data
  source: `SELECT COUNT(*) FROM detection_jobs WHERE status = 'pending'`. This demonstrates
  mixing Prometheus and Postgres data sources in the same dashboard.
- "Job Processing Duration" — histogram metric from the detector worker.
- "Detection Job Retry Rate" — `rate(detection_job_retries_total[5m])`.

**Row 3: API**

- "API Request Rate" — `rate(http_requests_total[1m])` grouped by endpoint.
- "API p95 Latency" — `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[1m]))`.
- "Active Anomalies by Severity" — Postgres data source query:
  `SELECT severity, COUNT(*) FROM anomalies WHERE resolved = false GROUP BY severity`.

---

#### `docs/LOAD_TEST_RESULTS.md`

Structure this document as a formal test report:

**Test 1: Baseline (1,000 events/sec, 300 seconds)**

- Consumer lag at T=60s, T=120s, T=300s
- Messages processed total
- Parse errors / rejected messages
- Anomalies detected: N error spikes, N cascade failures
- p50/p95/p99 API response times under concurrent Locust load

**Test 2: Heavy Load (10,000 events/sec, 60 seconds)**

- Consumer lag trajectory — does it recover after the burst?
- Postgres write throughput during the burst (from pg_stat_activity)
- Kafka producer delivery failures (if any)
- API response time degradation during ingestion burst

**Test 3: Sustained Anomaly Injection**

- Run the cascade-failure scenario for 90 seconds at 5,000 events/sec
- Time from scenario start to first anomaly detected (detection latency)
- True positive rate: were all injected anomalies detected?
- False positive rate during normal operation (anomalies detected with no injection)

**Analysis section**: What were the bottlenecks? At 10,000/sec, did Postgres writes become the
bottleneck (check batch_duration metric), or did Kafka consumer poll lag (check consumer_lag metric)?
What would you change? (Answer: connection pooling tuning, increase batch size, add consumer
instances up to the partition count.)

---

#### `docs/QUERY_ANALYSIS.md` — Five Queries to Document

**Query 1: Log search with multiple filters**
Before and after adding the composite index. Show the query plan changes from Seq Scan to
Index Scan. Show the BUFFERS output — how many shared blocks hit vs read.

**Query 2: Z-score detection — error counts by minute**
The `date_trunc('minute', timestamp)` GROUP BY. Confirm the `idx_logs_level_time` index is
used. Show what happens without the index.

**Query 3: Co-occurrence window function**
The `LEAD() OVER (ORDER BY timestamp)` query. Show that the sort is eliminated when the
`idx_logs_time` index is present. This is the technically impressive query — make sure the
EXPLAIN output shows "Index Scan using idx_logs_time" rather than "Sort."

**Query 4: `SELECT ... FOR UPDATE SKIP LOCKED`**
Show the query plan for job claiming. The partial index on `(scheduled_for) WHERE status = 'pending'`
should appear in the plan. Demonstrate by running EXPLAIN ANALYZE with 10,000 jobs in the table,
1,000 of which are pending. Without the partial index: Seq Scan. With: Index Scan on 1,000 rows.

**Query 5: Anomaly query with CASE ORDER BY**
Show that the order-by CASE is a Sort node in the plan (it cannot use an index). Discuss when
you would pre-compute a sort_order integer column instead. The current approach is fine at
< 1,000 active anomalies; at 1,000,000 you add a `sort_order` column.

---

#### `terraform/main.tf`

Same EC2 + security group + Elastic IP pattern as the ecosystem project. Add one difference:
a `user_data` script that sets the Kafka advertised listeners to the EC2 public IP, so the
simulator can produce from outside the host. This is a common Kafka configuration gotcha in
AWS deployments — document it in ARCHITECTURE.md.

---

#### `docs/TRADEOFFS.md`

Tradeoffs to document (these are your interview answers):

1. **Kafka vs RabbitMQ for log ingestion**: Kafka's log-based storage means any consumer can
   replay from any offset — you never lose data if a consumer falls behind or crashes. RabbitMQ
   deletes messages after delivery. For log aggregation (where replay is essential for debugging
   and backfill), Kafka is the right choice. RabbitMQ is better for task queues where replay
   is not needed.

2. **confluent-kafka vs kafka-python**: librdkafka (C library, wrapped by confluent-kafka)
   saturates a single partition at ~1M messages/sec. kafka-python (pure Python) tops out at
   ~50k/sec. For 10,000 events/sec you do not strictly need confluent-kafka, but the headroom
   matters and the library is the industry standard.

3. **Threaded consumer vs async consumer**: confluent-kafka is not async-native. Wrapping it
   with `loop.run_in_executor` adds complexity. A straightforward threaded consumer with
   psycopg2 is simpler, correct, and fast enough. The asyncio model shines for I/O-bound
   network clients; the Kafka consumer is better modeled as a synchronous stream processor.

4. **Postgres as a task queue vs Redis/Celery**: `FOR UPDATE SKIP LOCKED` gives you a durable,
   transactional job queue with no additional infrastructure. Celery with Redis is faster for
   very high job throughput (thousands of jobs/sec) but adds two services and operational
   complexity. At 10 detection jobs/minute, Postgres is correct. Document the throughput
   threshold where you would switch (~1,000 jobs/sec is where Postgres queue patterns start
   straining).

5. **Log-normal distribution for latency simulation**: Real service latencies are right-skewed
   — most requests are fast but a tail of slow requests exists. Uniform random would not generate
   realistic p99 latency outliers. Log-normal generates them correctly. This matters because your
   anomaly detector must distinguish genuine latency spikes from the expected tail.

6. **Array column for log_ids vs junction table**: The `log_ids UUID[]` column on the anomalies
   table is denormalized. A junction table (`anomaly_log_ids(anomaly_id, log_id)`) would be
   normalized but adds a join to every anomaly query. You never query "which anomalies reference
   this log ID" so the junction table's advantage (that direction of lookup) is never used.
   Array column is simpler and correct for the access pattern.

7. **At-least-once delivery with idempotent writes vs exactly-once**: Exactly-once requires
   Kafka transactions and is significantly more complex to implement. At-least-once with
   `ON CONFLICT (log_id) DO NOTHING` achieves the same result for idempotent insert workloads.
   Document what would break this: if processing a log event had non-idempotent side effects
   (like sending a notification), you would need exactly-once.

8. **Six partitions for logs.raw**: More partitions than consumers (you run one consumer
   instance) means some partitions are idle from a parallelism standpoint. But you size
   partitions for the future: adding a second consumer instance instantly doubles throughput
   with no topic reconfiguration. Partition count cannot be decreased after creation.

---

### Week 4 Definition of Done

- [ ] Grafana pipeline dashboard populated with real data during a load test
- [ ] Consumer lag metric visible in Grafana during 10,000 events/sec test
- [ ] LOAD_TEST_RESULTS.md has all three test scenarios with real numbers
- [ ] QUERY_ANALYSIS.md has EXPLAIN ANALYZE for all 5 queries before and after indexing
- [ ] `terraform apply` provisions EC2 successfully
- [ ] All documentation complete
- [ ] `make test` passes across all services

---

## Resume Summary

**Python Data Pipeline**: Built a high-throughput log aggregation pipeline using
confluent-kafka (librdkafka) consumer groups with manual partition assignment callbacks and
manual offset commits. Implemented at-least-once delivery with idempotent Postgres writes
(`ON CONFLICT DO NOTHING`). Sustained 10,000 events/sec ingestion with consumer lag under
30 seconds, documented in a formal load test report.

**Statistical Anomaly Detection**: Implemented three detectors — Z-score error rate detection,
IQR latency outlier detection, and temporal co-occurrence cascade failure detection — using
numpy and scipy with no ML framework. All detection logic is unit-tested in isolation from the
database using synthetic numpy arrays. Detectors are dispatched via a Postgres-native job queue
using `SELECT ... FOR UPDATE SKIP LOCKED`.

**SQL Depth**: Designed schemas for write-heavy log storage (BIGSERIAL, JSONB metadata,
partial indexes, composite indexes tuned to access patterns) and a Postgres task queue with
retry and exponential backoff implemented entirely in SQL. Documented EXPLAIN ANALYZE output
for window function queries showing index elimination of sort nodes, conditional aggregates
(`COUNT(*) FILTER`), and LATERAL joins. Demonstrated 70x query time improvement via indexing.

**Realistic Load Simulation**: Built a load simulator using statistically realistic log
distributions (Poisson inter-arrival times, log-normal latency) with scheduled anomaly
injection scenarios. Verified detector true positive rate under sustained load. Load tested
the API with Locust (50 concurrent users) while ingestion was running.

**FastAPI Async API**: Query API with dynamic parameterized query building (no ORM, no string
interpolation), asyncpg connection pool, and Prometheus auto-instrumentation. Can explain the
asyncio vs threading choice for each service and the pool exhaustion failure mode.
