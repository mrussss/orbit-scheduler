from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class AgentInput(StrictModel):
    repository_root: str = Field(min_length=1, max_length=128)
    issue: str = Field(min_length=1, max_length=65_536)
    error_log: str = Field(default="", max_length=65_536)


class Evidence(StrictModel):
    path: str = Field(min_length=1, max_length=1024)
    line: int = Field(default=0, ge=0)
    excerpt: str = Field(min_length=1, max_length=4096)


class Diagnosis(StrictModel):
    problem_type: str = Field(min_length=1, max_length=128)
    likely_causes: list[str] = Field(min_length=1, max_length=16)
    code_evidence: list[Evidence] = Field(max_length=32)
    recommended_checks: list[str] = Field(min_length=1, max_length=32)
    confidence: float = Field(ge=0, le=1)
    limits: list[str] = Field(max_length=16)

    @field_validator("likely_causes", "recommended_checks", "limits")
    @classmethod
    def nonempty_entries(cls, values: list[str]) -> list[str]:
        if any(not value.strip() for value in values):
            raise ValueError("list entries must not be empty")
        return values


class EvalInput(StrictModel):
    repository_ref: str
    repository_root: str
    issue: str
    error_log: str


class EvalExpected(StrictModel):
    expected_files: list[str]
    expected_evidence: list[str]
    expected_category: str
    forbidden_claims: list[str]
