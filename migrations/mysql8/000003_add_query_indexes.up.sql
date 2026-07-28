CREATE INDEX idx_lab_task_page
    ON lab_tasks(project_id, status, created_at DESC, id DESC);

CREATE INDEX idx_lab_task_fetch
    ON lab_tasks(status, available_at, priority DESC, id);

CREATE INDEX idx_lab_task_lease
    ON lab_tasks(status, lease_expires_at);
