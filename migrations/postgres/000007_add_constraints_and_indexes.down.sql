DROP INDEX IF EXISTS audit_aggregate_time_idx;
DROP INDEX IF EXISTS outbox_claim_expiry_idx;
DROP INDEX IF EXISTS outbox_unpublished_idx;
DROP INDEX IF EXISTS worker_instances_heartbeat_idx;
DROP INDEX IF EXISTS task_attempts_history_idx;
DROP INDEX IF EXISTS tasks_expired_lease_idx;
DROP INDEX IF EXISTS tasks_type_status_available_idx;
DROP INDEX IF EXISTS tasks_job_page_idx;
DROP INDEX IF EXISTS tasks_project_status_page_idx;
DROP INDEX IF EXISTS tasks_scheduler_candidates_idx;
DROP INDEX IF EXISTS api_tokens_prefix_idx;
DROP INDEX IF EXISTS jobs_project_idempotency_idx;

