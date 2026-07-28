CREATE TABLE audit_events (
    event_id uuid PRIMARY KEY,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    event_version integer NOT NULL CHECK (event_version > 0),
    payload jsonb NOT NULL,
    kafka_topic text NOT NULL,
    kafka_partition integer NOT NULL CHECK (kafka_partition >= 0),
    kafka_offset bigint NOT NULL CHECK (kafka_offset >= 0),
    occurred_at timestamptz NOT NULL,
    consumed_at timestamptz NOT NULL,
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

