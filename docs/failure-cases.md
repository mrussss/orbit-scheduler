# Failure and shutdown cases

## Worker shutdown

On SIGTERM the worker announces `DRAINING`, stops fetching, keeps heartbeat and
lease renewal active, and waits for current attempts within the configured
grace period. At expiry it cancels task contexts, makes bounded result-report
attempts, stops background loops, and closes the gRPC connection. An executor
that ignores context runs in an isolated goroutine; it cannot hold the runtime's
shutdown state machine indefinitely. Its uncommitted task is recovered by the
lease reaper.

## HTTP executor boundary

The HTTP executor requires an explicit hostname allowlist. It accepts only HTTP
and HTTPS, resolves and rejects loopback/private/link-local/multicast/reserved
addresses, rechecks every redirect, and uses a custom no-proxy transport whose
dial path validates the resolved IP again. Sensitive caller-controlled headers,
oversized bodies, and excess redirects are rejected.

Application checks reduce SSRF exposure but do not replace egress firewalling,
network namespaces, DNS policy, or cloud metadata endpoint controls. A real
deployment should enforce those network-layer boundaries as well.

## At-least-once edge cases

- A Fetch response lost after commit leaves a RUNNING task whose lease is later
  reaped.
- A Report response lost after commit is safe to resend with the same attempt
  and result hash.
- A worker that finishes after lease loss must discard its result; fencing
  prevents the old attempt from overwriting a newer one.
- Kafka is outside the Phase 5 delivery. PostgreSQL outbox rows are already
  transactional, but publishing begins only in Phase 6.
