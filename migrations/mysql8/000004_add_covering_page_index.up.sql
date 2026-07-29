CREATE INDEX idx_lab_task_page_cover
    ON lab_tasks(project_id, status, created_at DESC, id DESC, priority);
