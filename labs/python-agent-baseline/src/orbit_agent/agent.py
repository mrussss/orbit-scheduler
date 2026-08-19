from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from openai import AsyncOpenAI

from .models import AgentInput, Diagnosis
from .tools import SafeRepositoryTools, TOOL_SCHEMAS


SYSTEM_PROMPT = """Diagnose the issue using only search_code, read_file, and read_docs. Do not claim shell access, writes, live access, or evidence not returned by tools. Return exactly one JSON object matching: problem_type, likely_causes, code_evidence[{path,line,excerpt}], recommended_checks, confidence, limits."""


@dataclass(frozen=True)
class AgentRunResult:
    diagnosis: Diagnosis
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    estimated_cost_microunits: int
    model_calls: int
    tool_calls: int
    latency_ms: int


class AgentRunner:
    def __init__(self, client: AsyncOpenAI | Any, tools: SafeRepositoryTools, *, model: str, max_model_steps: int = 4, max_tool_calls: int = 12, max_output_tokens: int = 4096, prompt_rate: int = 0, completion_rate: int = 0) -> None:
        if not model or not 3 <= max_model_steps <= 6 or max_tool_calls <= 0 or max_output_tokens <= 0 or prompt_rate < 0 or completion_rate < 0:
            raise ValueError("invalid Agent configuration")
        self.client = client
        self.tools = tools
        self.model = model
        self.max_model_steps = max_model_steps
        self.max_tool_calls = max_tool_calls
        self.max_output_tokens = max_output_tokens
        self.prompt_rate = prompt_rate
        self.completion_rate = completion_rate

    async def run(self, raw: AgentInput | dict[str, Any]) -> AgentRunResult:
        payload = raw if isinstance(raw, AgentInput) else AgentInput.model_validate(raw)
        if payload.repository_root not in self.tools._roots:
            raise ValueError("repository_root is not allowlisted")
        started = time.monotonic()
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Repository alias: {payload.repository_root}\nIssue:\n{payload.issue}\nError log:\n{payload.error_log}"},
        ]
        prompt_tokens = completion_tokens = total_tokens = tool_calls = 0
        for model_call in range(1, self.max_model_steps + 1):
            response = await self.client.chat.completions.create(model=self.model, messages=messages, tools=TOOL_SCHEMAS, tool_choice="auto", max_completion_tokens=self.max_output_tokens, stream=False)
            if not response.choices or response.usage is None:
                raise RuntimeError("invalid model response")
            prompt_tokens += response.usage.prompt_tokens
            completion_tokens += response.usage.completion_tokens
            total_tokens += response.usage.total_tokens
            message = response.choices[0].message
            calls = list(message.tool_calls or [])
            if not calls:
                diagnosis = Diagnosis.model_validate_json(message.content or "")
                cost = (prompt_tokens * self.prompt_rate + completion_tokens * self.completion_rate) // 1_000_000
                return AgentRunResult(diagnosis, response.model, prompt_tokens, completion_tokens, total_tokens, cost, model_call, tool_calls, int((time.monotonic() - started) * 1000))
            messages.append(message.model_dump(exclude_none=True))
            for call in calls:
                tool_calls += 1
                if tool_calls > self.max_tool_calls or call.type != "function" or not call.id:
                    raise RuntimeError("tool call contract exhausted or invalid")
                result = await self.tools.execute(payload.repository_root, call.function.name, call.function.arguments)
                messages.append({"role": "tool", "tool_call_id": call.id, "content": result})
        raise RuntimeError("agent max model steps exhausted")
