CREATE TABLE agent_steps (
    task_id uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    step_no integer NOT NULL CHECK (step_no > 0),
    worker_instance_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('MODEL', 'TOOL', 'FINAL', 'ERROR')),
    tool_name text CHECK (tool_name IS NULL OR char_length(tool_name) BETWEEN 1 AND 100),
    input_summary jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(input_summary) = 'object' AND octet_length(input_summary::text) <= 8192),
    output_summary jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(output_summary) = 'object' AND octet_length(output_summary::text) <= 8192),
    status text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    PRIMARY KEY (task_id, attempt_no, step_no),
    FOREIGN KEY (task_id, attempt_no) REFERENCES task_attempts(task_id, attempt_no) ON DELETE CASCADE,
    CHECK ((status = 'RUNNING') = (finished_at IS NULL)),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK ((kind = 'TOOL') = (tool_name IS NOT NULL))
);

CREATE INDEX agent_steps_replay_idx ON agent_steps(task_id, attempt_no, step_no);
