CREATE TABLE projects (
    id uuid PRIMARY KEY,
    name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    status text NOT NULL CHECK (status IN ('ACTIVE', 'DISABLED')),
    task_quota bigint NOT NULL CHECK (task_quota >= 0),
    max_concurrent_tasks integer NOT NULL CHECK (max_concurrent_tasks > 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE api_tokens (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    token_prefix text NOT NULL CHECK (char_length(token_prefix) BETWEEN 8 AND 32),
    token_hash bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    scopes text[] NOT NULL,
    disabled boolean NOT NULL DEFAULT false,
    expires_at timestamptz,
    last_used_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (token_prefix, token_hash)
);

