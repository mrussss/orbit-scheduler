# Read-only Repository Agent v0.3

## Contract

An `agent` task diagnoses one issue against a server-allowlisted, fixed source snapshot:

```json
{
  "repository_root": "gateway",
  "issue": "AUTH requests stall while ordinary requests continue",
  "error_log": "auth_queue push FULL"
}
```

`repository_root` is an alias from `AGENT_REPOSITORIES_JSON`, not a client path. The Worker chooses `AGENT_MODEL`; clients cannot supply a Provider, URL, credential, model, prompt, tool schema, or tool implementation. The successful result contains `problem_type`, `likely_causes`, `code_evidence`, `recommended_checks`, `confidence`, and `limits`, plus model, Token, cost, call-count, and latency measurements.

## Tool and safety boundary

Exactly three server-owned tools exist:

- `search_code`: bounded literal search over allowlisted text source files;
- `read_file`: bounded line-range read of one allowlisted text file;
- `read_docs`: bounded literal search restricted to documentation.

There is no Shell, write, Git mutation, database, arbitrary HTTP, live Gateway, or automatic-fix tool. Startup canonicalizes every allowlisted root. Each requested path must be relative; `..`, absolute paths, missing paths, NULs, symlink escape, secret filenames, excluded dependency/VCS directories, binary/non-UTF-8 files, oversized files, excessive matches, excessive line ranges, and oversized tool results are rejected. Tool and payload JSON reject unknown fields and trailing values.

The model loop is bounded to 3–6 model rounds and a separately bounded number of tool calls. Context cancellation is checked before provider and tool work and during repository traversal. A 429/5xx/transport failure becomes a retryable Orbit outcome; the scheduler performs backoff as a new Attempt. No unbounded internal retry exists.

## Authoritative Trace and SSE

`agent_steps` stores `(task_id, attempt_no, step_no)`, Worker identity, `MODEL/TOOL/FINAL/ERROR`, optional tool name, bounded structured input/output summaries, status, and timestamps. Writes use the same `task + worker_instance + attempt + live lease` fencing identity as result reporting. Step numbers must be contiguous. A stale or expired Attempt cannot insert or finish authoritative Trace rows.

`GET /api/v1/tasks/:task_id/events` is a tenant-scoped SSE replay over PostgreSQL Trace. Events are `task_status`, `agent_step_started`, `agent_step_finished`, `tool_call`, `tool_result`, `final_result`, and `error`. `Last-Event-ID` uses `attempt:step:phase`. Reconnecting rebuilds step history from DB, while `final_result` is read from the terminal Task row; the stream is never a state authority. Disconnecting the HTTP client only ends that stream. Task cancellation still flows through lease renewal into the Worker Context and stops the Agent.

Trace summaries contain counts, byte sizes, status, tool names, and repository aliases—not issue text, error-log text, source content, model output, API keys, or raw tool arguments/results. If a Tool Result exceeds its transport bound, the model receives only a bounded `{truncated, result_bytes}` summary and can request a narrower read; raw oversized content is never forwarded or persisted.

## Gateway eval

`evals/gateway/case-001..010` pins Gateway commit `363a403774859dd597160b810a13bd0e13b0fab1` and covers partial write, fd reuse/stale response, eventfd wakeup, AUTH Queue overload, Response Queue overload, slow clients, Redis outage, config CAS, graceful drain, and HTTP/dependency classification.

The deterministic scorer reports success, expected-file hit, expected-evidence hit, forbidden claim, step count, latency, prompt/completion Tokens, and estimated cost. It does not use an LLM judge. CI's Fake Provider still executes one real `read_file` tool call against the pinned snapshot:

```bash
make test-agent
make smoke-agent
make smoke-agent-eval
```

`make smoke-agent` starts real PostgreSQL, HTTP/gRPC server, Worker, and Fake Provider processes. It proves 429 creates Attempt 2, disconnecting SSE leaves the Task running, explicit cancellation stops the post-tool model round and commits `CANCELED`, a Worker killed after `tool_result` is recovered by lease expiry and a replacement Attempt, DB replay spans both Attempts, and the terminal Result contains Token/cost/latency evidence.

For a configured real Provider, run `go run ./cmd/orbit-agent-eval -fake-provider=false -repository /absolute/snapshot`; `LLM_BASE_URL`, `LLM_API_KEY`, and `AGENT_MODEL` remain environment configuration.

## Reliability boundary

Orbit remains at-least-once. A crash after a read-only tool call is safe to analyze again, but Provider calls and charges may repeat. The lease reaper permits a new Attempt while fencing all late Trace and Result writes from the old Attempt. This is not exactly-once model billing and is not live remediation.

The Python compatibility lab in `labs/python-agent-baseline` mirrors the strict Pydantic input/output, async tool loop, native OpenAI SDK tool-calling shape, path boundary, pytest suite, and the same ten fixtures. It is a learning lab, not another deployed service.
