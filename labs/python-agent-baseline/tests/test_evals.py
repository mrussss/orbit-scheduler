import json
from pathlib import Path

from types import SimpleNamespace

import pytest

from orbit_agent.agent import AgentRunner
from orbit_agent.evals import load_cases, score
from orbit_agent.models import Diagnosis, Evidence
from orbit_agent.tools import SafeRepositoryTools


class EvalMessage(SimpleNamespace):
    def model_dump(self, exclude_none: bool = True):
        return {"role": "assistant", "content": self.content, "tool_calls": [{"id": call.id, "type": call.type, "function": {"name": call.function.name, "arguments": call.function.arguments}} for call in self.tool_calls or []]}


class EvalCompletions:
    def __init__(self, path: str, diagnosis: Diagnosis) -> None:
        self.path = path
        self.diagnosis = diagnosis
        self.calls = 0

    async def create(self, **kwargs):
        self.calls += 1
        usage = SimpleNamespace(prompt_tokens=10, completion_tokens=5, total_tokens=15)
        if self.calls == 1:
            call = SimpleNamespace(id="eval-read", type="function", function=SimpleNamespace(name="read_file", arguments=json.dumps({"path": self.path})))
            message = EvalMessage(content="", tool_calls=[call])
        else:
            message = EvalMessage(content=self.diagnosis.model_dump_json(), tool_calls=[])
        return SimpleNamespace(model="fake", choices=[SimpleNamespace(message=message)], usage=usage)


@pytest.mark.asyncio
async def test_same_ten_gateway_evals_run_through_agent(tmp_path: Path) -> None:
    fixture_root = Path(__file__).resolve().parents[3] / "evals" / "gateway"
    cases = load_cases(fixture_root)
    for case in cases:
        source = tmp_path / case.expected.expected_files[0]
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(" ".join(case.expected.expected_evidence) + "\n")
        diagnosis = Diagnosis(problem_type=case.expected.expected_category, likely_causes=["fixture"], code_evidence=[Evidence(path=case.expected.expected_files[0], line=1, excerpt=" ".join(case.expected.expected_evidence))], recommended_checks=["fixture"], confidence=0.8, limits=[])
        completions = EvalCompletions(case.expected.expected_files[0], diagnosis)
        client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
        runner = AgentRunner(client, SafeRepositoryTools({"gateway": tmp_path}), model="fake")
        result = await runner.run({"repository_root": case.input.repository_root, "issue": case.input.issue, "error_log": case.input.error_log + "\n" + case.fault_log})
        metrics = score(case, result)
        assert metrics.success and metrics.step_count == 3 and completions.calls == 2
