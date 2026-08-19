import pytest
from pydantic import ValidationError

from orbit_agent.models import AgentInput, Diagnosis


def test_models_reject_unknown_fields_and_invalid_confidence() -> None:
    assert AgentInput.model_validate({"repository_root": "gateway", "issue": "stall"}).issue == "stall"
    with pytest.raises(ValidationError):
        AgentInput.model_validate({"repository_root": "gateway", "issue": "stall", "shell": "id"})
    with pytest.raises(ValidationError):
        Diagnosis.model_validate({"problem_type": "x", "likely_causes": ["x"], "code_evidence": [], "recommended_checks": ["x"], "confidence": 2, "limits": []})
