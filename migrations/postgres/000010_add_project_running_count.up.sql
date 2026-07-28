ALTER TABLE projects
    ADD COLUMN running_tasks integer NOT NULL DEFAULT 0
        CHECK (running_tasks >= 0);

UPDATE projects project
SET running_tasks = counts.running_tasks
FROM (
    SELECT project_id, count(*)::integer AS running_tasks
    FROM tasks
    WHERE status = 'RUNNING'
    GROUP BY project_id
) counts
WHERE counts.project_id = project.id;

COMMENT ON COLUMN projects.running_tasks IS
    'Authoritative project concurrency count maintained by scheduler transactions';
