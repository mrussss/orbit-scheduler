from pathlib import Path
from types import SimpleNamespace

import pytest

from orbit_agent.agent import AgentRunner
from orbit_agent.tools import SafeRepositoryTools


class Message(SimpleNamespace):
    def model_dump(self, exclude_none: bool = True):
        return {"role": "assistant", "content": self.content, "tool_calls": [{"id": call.id, "type": call.type, "function": {"name": call.function.name, "arguments": call.function.arguments}} for call in self.tool_calls or []]}


class FakeCompletions:
    def __init__(self) -> None:
        self.calls = 0

    async def create(self, **kwargs):
        self.calls += 1
        usage = SimpleNamespace(prompt_tokens=10, completion_tokens=5, total_tokens=15)
        if self.calls == 1:
            tool = SimpleNamespace(id="call-1", type="function", function=SimpleNamespace(name="read_file", arguments='{"path":"main.go"}'))
            message = Message(content="", tool_calls=[tool])
        else:
            message = Message(content='{"problem_type":"queue","likely_causes":["stall"],"code_evidence":[{"path":"main.go","line":1,"excerpt":"package main"}],"recommended_checks":["test"],"confidence":0.8,"limits":[]}', tool_calls=[])
        return SimpleNamespace(model="fake", choices=[SimpleNamespace(message=message)], usage=usage)


@pytest.mark.asyncio
async def test_native_sdk_shape_tool_loop(tmp_path: Path) -> None:
    (tmp_path / "main.go").write_text("package main\n")
    completions = FakeCompletions()
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    runner = AgentRunner(client, SafeRepositoryTools({"gateway": tmp_path}), model="fake", prompt_rate=1_000_000, completion_rate=2_000_000)
    result = await runner.run({"repository_root": "gateway", "issue": "stall"})
    assert result.diagnosis.problem_type == "queue"
    assert result.model_calls == 2 and result.tool_calls == 1 and result.estimated_cost_microunits == 40
