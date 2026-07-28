# Business API and querying

The platform administrator uses `ADMIN_TOKEN` on `/api/v1/projects/**` to
bootstrap projects and rotate tenant tokens. Tenant routes accept only stored
`orb_` bearer tokens and derive the project ID from the authenticated token;
request bodies cannot select another tenant.

Task and job lists use a signed `(created_at, id)` cursor and descending stable
ordering. Task list responses omit payload and result data; detail/result routes
serve those fields explicitly. Page size is capped at 100, job batches at 500,
and request bodies at `HTTP_MAX_BODY_BYTES`.

The scheduler candidate query is supported by
`tasks(status, available_at, priority DESC, id)`. Tenant paging uses
`tasks(project_id, status, created_at DESC, id DESC)`, while job paging uses
`tasks(job_id, created_at DESC, id DESC)`. Run the integration fixture with a
representative data set before recording `EXPLAIN (ANALYZE, BUFFERS)` output;
performance claims must include row counts and machine details.

Disabling a project rejects new tasks and excludes its pending tasks from fetch.
Already-running attempts retain their lease and may finish normally.
