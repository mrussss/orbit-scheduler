CREATE TABLE lab_tasks (
    id BINARY(16) NOT NULL,
    project_id BINARY(16) NOT NULL,
    idempotency_key VARCHAR(128) NULL,
    request_hash BINARY(32) NULL,
    status VARCHAR(32) NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    available_at DATETIME(6) NOT NULL,
    attempt_no INT NOT NULL DEFAULT 0,
    lease_owner BINARY(16) NULL,
    lease_expires_at DATETIME(6) NULL,
    payload JSON NOT NULL,
    result JSON NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_lab_task_project FOREIGN KEY (project_id) REFERENCES lab_projects(id),
    CONSTRAINT chk_lab_task_status CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED')),
    CONSTRAINT chk_lab_task_attempt CHECK (attempt_no >= 0),
    CONSTRAINT chk_lab_task_idempotency CHECK (
        (idempotency_key IS NULL AND request_hash IS NULL)
        OR (idempotency_key IS NOT NULL AND request_hash IS NOT NULL)
    ),
    UNIQUE KEY uk_lab_task_idempotency (project_id, idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE lab_task_attempts (
    task_id BINARY(16) NOT NULL,
    attempt_no INT NOT NULL,
    worker_id BINARY(16) NOT NULL,
    started_at DATETIME(6) NOT NULL,
    finished_at DATETIME(6) NULL,
    outcome VARCHAR(32) NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (task_id, attempt_no),
    CONSTRAINT fk_lab_attempt_task FOREIGN KEY (task_id) REFERENCES lab_tasks(id),
    CONSTRAINT chk_lab_attempt_number CHECK (attempt_no > 0),
    CONSTRAINT chk_lab_attempt_outcome CHECK (
        outcome IS NULL OR outcome IN ('SUCCEEDED', 'RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'TIMEOUT', 'CANCELED')
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
