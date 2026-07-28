CREATE INDEX tasks_completed_housekeeping_idx ON tasks(completed_at, id) WHERE completed_at IS NOT NULL;
CREATE INDEX audit_consumed_housekeeping_idx ON audit_events(consumed_at, event_id);

