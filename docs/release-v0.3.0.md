# Orbit Scheduler v0.3.0 Agent milestone

Release verification date: 2026-08-19 (Asia/Shanghai)

This milestone extends the verified v0.2 scheduler and Reliable LLM Executor; it does not replace or reimplement them. The delivered vertical slice is an allowlisted, read-only source diagnosis Agent with bounded native Tool Calling, authoritative fenced Trace, DB-backed SSE replay, ten fixed Gateway evals, and a minimal Python compatibility lab.

## Delivered scope

- strict `agent` task and diagnosis schemas;
- exactly `search_code`, `read_file`, and `read_docs`, with path/symlink/secret/binary/size/match/result containment;
- 3–6 model rounds, bounded tool calls, cancellation, Provider failure mapping, Token/cost/latency output, and Fake Provider coverage;
- `agent_steps` migration and gRPC recording with contiguous steps and live lease/Attempt fencing;
- authenticated DB-replay SSE with `Last-Event-ID`; disconnect does not cancel a Task;
- fixed Gateway commit and ten non-live fault fixtures with deterministic metrics;
- tests for 429 → new Attempt, crash/lease expiry after a tool step, stale Trace rejection, max-step termination, malformed calls, and cancellation during a tool loop;
- Python/Pydantic/async/OpenAI-SDK/pytest baseline over the same tool schemas and fixtures.

## Claim boundaries

No Shell/write/fix tool, live Gateway integration, RAG, vector database, MCP, planner, multi-agent system, Kafka implementation, Kubernetes deployment, LangGraph, exactly-once execution, exactly-once Provider billing, production HA, or production SLO is claimed. PostgreSQL remains the state authority.

The personal closed-book explanation, manual modification, fault injection, debugging, and tradeoff exercises in `develop.md` remain owner exercises. Automated implementation and green CI evidence cannot honestly certify that another person completed those exercises unaided.

## Local verification evidence

The final local `make verify` completed successfully on 2026-08-19:

- formatting and `go vet`, all Go unit tests under the Race Detector, and every root command build passed;
- PostgreSQL/Testcontainers integration, including Agent Trace monotonicity, tool-step crash/lease recovery, stale fencing, and 429 → Attempt 2, passed in 33.058 seconds;
- the existing two-Worker scheduler smoke and Reliable LLM 429/SIGTERM smoke passed;
- Agent/tool/eval/API/pgstore Race tests passed;
- all ten pinned Gateway Fake Provider evals passed with file/evidence hits and no forbidden claim;
- the complete MySQL Lab passed in 194.659 seconds and its three-round concurrency gate passed in 229.898 seconds;
- the Python baseline's Pydantic, async tools, native-SDK-shape loop, and shared-eval tests passed: 4 tests in 0.40 seconds.

These are local correctness and reproducibility observations, not production performance, availability, cost, or SLO claims.
