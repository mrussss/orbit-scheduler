ALTER TABLE worker_instances
    ADD COLUMN reported_running_tasks integer NOT NULL DEFAULT 0
        CHECK (reported_running_tasks >= 0);

COMMENT ON COLUMN worker_instances.running_tasks IS
    'Authoritative scheduler count, changed only by Fetch, ReportResult, and Reaper transactions';
COMMENT ON COLUMN worker_instances.reported_running_tasks IS
    'Worker-observed count from heartbeat; telemetry only and never used for capacity allocation';
