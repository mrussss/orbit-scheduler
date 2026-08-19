from pathlib import Path

from orbit_agent.agent import AgentRunResult
from orbit_agent.evals import load_cases, score
from orbit_agent.models import Diagnosis, Evidence


def test_same_ten_gateway_evals() -> None:
    fixture_root = Path(__file__).resolve().parents[3] / "evals" / "gateway"
    cases = load_cases(fixture_root)
    for case in cases:
        diagnosis = Diagnosis(problem_type=case.expected.expected_category, likely_causes=["fixture"], code_evidence=[Evidence(path=case.expected.expected_files[0], line=1, excerpt=" ".join(case.expected.expected_evidence))], recommended_checks=["fixture"], confidence=0.8, limits=[])
        metrics = score(case, AgentRunResult(diagnosis, "fake", 10, 5, 15, 1, 2, 1, 3))
        assert metrics.success and metrics.step_count == 3
