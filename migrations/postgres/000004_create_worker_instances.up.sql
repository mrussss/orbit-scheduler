CREATE TABLE worker_instances (
    id uuid PRIMARY KEY,
    worker_name text NOT NULL CHECK (char_length(worker_name) BETWEEN 1 AND 200),
    hostname text NOT NULL,
    capacity integer NOT NULL CHECK (capacity > 0),
    supported_task_types text[] NOT NULL,
    running_tasks integer NOT NULL DEFAULT 0 CHECK (running_tasks >= 0),
    draining boolean NOT NULL DEFAULT false,
    last_heartbeat_at timestamptz NOT NULL,
    started_at timestamptz NOT NULL,
    process_version text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

