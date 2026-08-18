# LLM Security Boundaries

The LLM Executor treats task payloads as untrusted tenant input and Provider
configuration as operator-owned process configuration.

## Secrets and routing

- `LLM_API_KEY` is read only from the Worker environment.
- Payload decoding rejects `api_key`, `base_url`, `headers`, and every other
  unknown field.
- The Provider Base URL is configured at process startup and cannot be changed
  by a task.
- URLs with embedded credentials, query strings, or fragments are rejected.
- HTTPS is required outside `APP_ENV=test`; the test environment alone may use
  HTTP for a loopback Fake Provider.
- Automatic redirects are disabled, preventing credentials from being
  forwarded to another origin.
- Provider response bodies are never included in error messages.

The baseline implementation intentionally emits no Prompt or Response logs.
The API key is absent from Payload, Result, PostgreSQL state, metrics, and
structured errors. `LLM_LOG_CONTENT=false` remains the safe default; enabling
content logging is reserved for a future policy-controlled implementation.

Task messages are still part of the authoritative Task payload, and generated
content is part of the Task result, so both are persisted in PostgreSQL by
design. Clients must not submit content whose retention is incompatible with
their data policy. Orbit stores the normalized result contract, not the full
raw Provider response.

## Resource controls

- configured model allowlist;
- maximum messages and per-message bytes;
- maximum total prompt bytes;
- maximum output tokens;
- maximum Provider response bytes;
- maximum normalized Orbit Result bytes before gRPC reporting;
- Worker capacity plus Provider-specific concurrency;
- dial, TLS handshake, full request, task execution, and shutdown timeouts;
- bounded metadata fields;
- no streaming response buffer;
- no Executor-internal business retry loop.

## Metrics cardinality

Only the configured Provider name, allowlisted requested model, fixed token
type, and fixed Orbit outcome appear as labels. Prompt text, generated text,
task IDs, project IDs, trace IDs, errors, and arbitrary Provider values do not.

## Deliberately unsupported capabilities

The reliable baseline does not implement SSE streaming, RAG, MCP, Memory,
multi-agent workflows, or Tool Calling. In particular, a model cannot execute
Shell commands, arbitrary URLs, SQL, or write operations.

If controlled Tool Calling is added later, it requires a separate allowlist,
JSON Schema validation, at most three rounds, per-tool timeout and result-size
limits, and an auditable call record. It must not weaken the boundaries above.
