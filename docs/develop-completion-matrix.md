# `develop.md` completion matrix

Audit date: 2026-08-19 (Asia/Shanghai)

This matrix separates repository-deliverable work from personal ownership exercises. A green automated test can prove an implementation property; it cannot prove that a person independently traced a call chain, made a change without AI, debugged a fault, or answered a question closed-book.

## Repository-deliverable scope

| Requirement | Status | Implementation evidence | Verification evidence |
| --- | --- | --- | --- |
| O0 fixed reference | Present | `main` and tag `v0.2.0` point to `5f5b114bc72bc71017d3c312782ebbb739366c51`; Agent work is isolated on `feature/agent-v0.3` | `git rev-parse main v0.2.0` and `git branch --show-current` |
| O6A strict Agent contract | Implemented | `internal/executor/agent/payload.go`, `executor.go`, and `internal/executor/llm/openai_compatible.go` implement the required input/output, native tool-call loop, and a 3–6 model-step hard bound | `internal/executor/agent/*_test.go` and `make test-agent` |
| O6A exactly three read-only tools | Implemented | `internal/executor/agent/tools.go` exposes only `search_code`, `read_file`, and `read_docs`; it has no shell, write, Git, DB, arbitrary HTTP, live Gateway, or repair capability | schema/tool unit tests plus `docs/agent-v0.3.md` claim boundary |
| O6B repository containment | Implemented | server-owned aliases, canonical roots, relative-path validation, symlink containment, secret and dependency exclusions, extension/binary checks, and byte/match/result limits are in `tools.go` | traversal, `../../etc/passwd`, absolute path, symlink escape, missing file, oversized file/result, binary, secret, match-limit, malformed JSON, unknown tool/field, invalid type, and missing-field tests in `tools_test.go` |
| O6C authoritative Trace | Implemented | migration `000011_agent_steps`, domain Step types, pgstore, gRPC `RecordAgentStep`, Worker tracer, and MODEL/TOOL/FINAL/ERROR records | unit tests plus `tests/integration/agent_trace_test.go` prove contiguous steps, live-lease/Attempt fencing, stale Step and Result rejection, and authoritative Attempt 2 result |
| O6C retention and secret boundary | Implemented | Trace stores bounded metadata summaries rather than issue text, error logs, source, raw tool arguments/results, model output, or credentials; oversized Tool Results are reduced to `{truncated,result_bytes}` before the next model round | `TestExecutorTraceNeverPersistsPromptArgumentsOrToolResultContent`, bounded-result tests, and smoke log credential scan |
| O6D DB-backed SSE | Implemented | tenant-scoped `GET /api/v1/tasks/:task_id/events`, `Last-Event-ID`, PostgreSQL replay, and terminal Task result read are in `internal/api/events.go` | API tests and `make smoke-agent` cover all seven exact event names, disconnect without cancel, explicit cancel, and replay across Attempts |
| O6E ten fixed Gateway evals | Implemented | `evals/gateway/case-001` through `case-010` pin commit `363a403774859dd597160b810a13bd0e13b0fab1` and cover the ten requested fault classes | deterministic scorer emits the nine requested fields; `make smoke-agent-eval` runs a real tool round against the pinned snapshot without an LLM judge or live integration |
| O7 Agent reliability | Implemented | Agent reuses Task, Worker, retry/backoff, Lease, Attempt, Fencing, cancellation, and bounded execution semantics | `make smoke-agent` proves 429 → Attempt 2, cancel after a tool result, Worker kill after a tool result, lease expiry/recovery, replay, and result token/cost/latency; integration tests reject stale Step/Result; unit tests prove max-step failure |
| O8 Python compatibility baseline | Implemented | `labs/python-agent-baseline` contains typed Pydantic models, exceptions, async tools/loop, HTTP/OpenAI SDK shape, bounded-result summaries, pytest, the same three schemas, and no LangGraph | its 5-test pytest suite drives all ten shared fixtures through `AgentRunner` and `SafeRepositoryTools` |
| Default exclusion list | Preserved | no RAG, vector DB, MCP, planner, multi-agent, write/shell tool, live Gateway control, Kafka, Kubernetes, or LangGraph was added | source/dependency review and documented claim boundaries |

The final repository gate is:

```bash
make verify
cd labs/python-agent-baseline
pytest -q
```

`make verify` includes Go vet/race/build, PostgreSQL integration, the existing scheduler and LLM smokes, Agent unit/eval tests, the full Agent HTTP/gRPC/Worker/SSE fault smoke, the ten Gateway evals, and both MySQL gates. `report-mysql-explain` remains a separate evidence command because it is an explanatory report rather than the normal CI gate.

## Personal ownership work that remains owner-only

The following boxes intentionally remain unchecked until the repository owner performs them. Their detailed questions and fault lists are authoritative in `develop.md`.

### O0–O5

- [ ] O0: personally run the baseline commands and explain what layer each command verifies.
- [ ] O1: closed-book trace Create/Get/List/Cancel through HTTP → service → PostgreSQL; independently choose and implement one listed small change test-first; personally reproduce the five HTTP/DB faults.
- [ ] O2: draw the state machine and independently trace Fetch/Attempt/Execute/Report plus Cancel/Report/Reaper races; personally run the five faults and one small retry/classification change.
- [ ] O3: draw Task/Attempt/Lease/Worker without source; explain `SKIP LOCKED`, lease, Attempt, and fencing closed-book; personally run the seven Worker/lease/stale-result/drain faults.
- [ ] O4: personally run `make test-mysql-lab`, `make test-mysql-concurrency`, and `make report-mysql-explain`; answer the five MySQL/PostgreSQL comparison questions closed-book.
- [ ] O5: trace Payload → Provider → Result; personally run or modify all ten Fake Provider fault modes; independently complete one listed LLM boundary change; explain retry, billing, credentials, bounds, metrics, and cost.

### Seven-gate sign-off

For every A-level topic—state machine, idempotency, retry, lease, Attempt, fencing, PostgreSQL claim transaction, Worker runtime, LLM failure semantics, tool loop, and Agent cancel/retry/max steps—personally demonstrate:

- [ ] definition;
- [ ] end-to-end code path;
- [ ] invariants;
- [ ] an independent modification;
- [ ] fault injection;
- [ ] debugging from logs/DB/metrics/Trace;
- [ ] design tradeoffs and alternatives.

## Milestone rule

Do not create `owned-orbit-v0.2.0` until O1–O5 and their personal checks are complete. Do not create `owned-orbit-agent-v0.3` until the Agent repository gate is green and the Agent seven-gate exercises have been personally passed. Until then, repository release evidence may be described as implemented and tested, but personal ownership must not be claimed.
