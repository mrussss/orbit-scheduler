# Python Agent Baseline

This lab mirrors Orbit v0.3's read-only repository Agent without becoming a second production service. It uses Pydantic strict models, `async` tool execution, the native OpenAI Python SDK tool-calling shape, `httpx` through that SDK, pytest, exactly three server-owned tools, and the same ten fixtures in `../../evals/gateway`.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest
```

The same boundaries apply: allowlisted snapshots only, no shell/write/git/network tools, symlink-safe path containment, bounded files/results/matches, secret and binary rejection, and 3–6 model rounds.
