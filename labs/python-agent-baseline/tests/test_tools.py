from pathlib import Path
import json

import pytest

from orbit_agent.tools import SafeRepositoryTools, TOOL_SCHEMAS, ToolRejected


@pytest.mark.asyncio
async def test_three_tools_and_path_security(tmp_path: Path) -> None:
    (tmp_path / "main.go").write_text("package main\nfunc retry() {}\n")
    (tmp_path / ".env").write_text("TOKEN=secret\n")
    outside = tmp_path.parent / "outside-agent-secret.txt"
    outside.write_text("secret")
    (tmp_path / "escape.txt").symlink_to(outside)
    tools = SafeRepositoryTools({"gateway": tmp_path}, max_file_bytes=1024, max_result_bytes=4096, max_matches=2)
    assert [item["function"]["name"] for item in TOOL_SCHEMAS] == ["search_code", "read_file", "read_docs"]
    assert "main.go" in await tools.execute("gateway", "search_code", '{"query":"retry"}')
    assert "func retry" in await tools.execute("gateway", "read_file", '{"path":"main.go"}')
    for arguments in ['{"path":"/etc/passwd"}', '{"path":"../outside-agent-secret.txt"}', '{"path":"escape.txt"}', '{"path":".env"}', '{"path":"main.go","command":"cat"}']:
        with pytest.raises(ToolRejected):
            await tools.execute("gateway", "read_file", arguments)


@pytest.mark.asyncio
async def test_oversized_result_is_summarized(tmp_path: Path) -> None:
    (tmp_path / "main.go").write_text("package main\n" + "x" * 300)
    tools = SafeRepositoryTools({"gateway": tmp_path}, max_file_bytes=1024, max_result_bytes=64, max_matches=10)
    result = await tools.execute("gateway", "read_file", '{"path":"main.go"}')
    summary = json.loads(result)
    assert summary["truncated"] is True and summary["result_bytes"] > 64
