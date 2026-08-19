from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from .agent import AgentRunResult
from .models import EvalExpected, EvalInput


@dataclass(frozen=True)
class EvalCase:
    case_id: str
    input: EvalInput
    expected: EvalExpected
    fault_log: str


@dataclass(frozen=True)
class EvalMetrics:
    case_id: str
    success: bool
    expected_file_hit: bool
    expected_evidence_hit: bool
    forbidden_claim: bool
    step_count: int
    latency_ms: int
    prompt_tokens: int
    completion_tokens: int
    estimated_cost_microunits: int

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


def load_cases(root: Path) -> list[EvalCase]:
    cases: list[EvalCase] = []
    for directory in sorted(root.glob("case-*")):
        cases.append(EvalCase(directory.name, EvalInput.model_validate_json((directory / "input.json").read_text()), EvalExpected.model_validate_json((directory / "expected.json").read_text()), (directory / "fault.log").read_text()))
    if len(cases) != 10:
        raise ValueError(f"expected exactly 10 cases, got {len(cases)}")
    return cases


def score(case: EvalCase, result: AgentRunResult) -> EvalMetrics:
    evidence_paths = {item.path.casefold() for item in result.diagnosis.code_evidence}
    evidence_text = " ".join(f"{item.path} {item.excerpt}" for item in result.diagnosis.code_evidence).casefold()
    expected_file_hit = any(any(actual == wanted.casefold() or actual.endswith("/" + wanted.casefold()) for actual in evidence_paths) for wanted in case.expected.expected_files)
    expected_evidence_hit = all(value.casefold() in evidence_text for value in case.expected.expected_evidence)
    diagnosis_text = result.diagnosis.model_dump_json().casefold()
    forbidden_claim = any(value.casefold() in diagnosis_text for value in case.expected.forbidden_claims)
    success = result.diagnosis.problem_type.casefold() == case.expected.expected_category.casefold() and expected_file_hit and expected_evidence_hit and not forbidden_claim
    return EvalMetrics(case.case_id, success, expected_file_hit, expected_evidence_hit, forbidden_claim, result.model_calls + result.tool_calls, result.latency_ms, result.prompt_tokens, result.completion_tokens, result.estimated_cost_microunits)
