from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any


TOOL_SCHEMAS: list[dict[str, Any]] = [
    {"type": "function", "function": {"name": "search_code", "description": "Search literal text in allowlisted source files.", "parameters": {"type": "object", "additionalProperties": False, "properties": {"query": {"type": "string"}, "path": {"type": "string"}}, "required": ["query"]}}},
    {"type": "function", "function": {"name": "read_file", "description": "Read a bounded range from one allowlisted text file.", "parameters": {"type": "object", "additionalProperties": False, "properties": {"path": {"type": "string"}, "start_line": {"type": "integer", "minimum": 1}, "end_line": {"type": "integer", "minimum": 1}}, "required": ["path"]}}},
    {"type": "function", "function": {"name": "read_docs", "description": "Search allowlisted documentation.", "parameters": {"type": "object", "additionalProperties": False, "properties": {"query": {"type": "string"}, "path": {"type": "string"}}, "required": ["query"]}}},
]

_TEXT_EXTENSIONS = {".go", ".py", ".rs", ".java", ".kt", ".c", ".h", ".cc", ".cpp", ".js", ".jsx", ".ts", ".tsx", ".sql", ".proto", ".yaml", ".yml", ".toml", ".json", ".md", ".txt", ".sh", ".xml"}
_EXCLUDED_DIRS = {".git", ".hg", ".svn", "node_modules", "vendor", ".venv", "venv", "__pycache__"}
_SECRET_NAMES = {".env", "credentials", "credentials.json", "id_rsa", "id_ed25519", ".npmrc", ".pypirc"}


class ToolRejected(ValueError):
    pass


class SafeRepositoryTools:
    def __init__(self, repositories: dict[str, Path], *, max_file_bytes: int = 256 << 10, max_result_bytes: int = 128 << 10, max_matches: int = 100) -> None:
        if not repositories or min(max_file_bytes, max_result_bytes, max_matches) <= 0:
            raise ValueError("invalid tool limits")
        self._roots = {alias: root.expanduser().resolve(strict=True) for alias, root in repositories.items()}
        if any(not root.is_dir() for root in self._roots.values()):
            raise ValueError("repository root must be a directory")
        self.max_file_bytes = max_file_bytes
        self.max_result_bytes = max_result_bytes
        self.max_matches = max_matches

    async def execute(self, repository: str, name: str, arguments_json: str) -> str:
        return await asyncio.to_thread(self._execute_sync, repository, name, arguments_json)

    def _execute_sync(self, repository: str, name: str, arguments_json: str) -> str:
        root = self._roots.get(repository)
        if root is None:
            raise ToolRejected("repository is not allowlisted")
        try:
            arguments = json.loads(arguments_json)
        except json.JSONDecodeError as exc:
            raise ToolRejected("malformed tool arguments") from exc
        if not isinstance(arguments, dict):
            raise ToolRejected("tool arguments must be an object")
        if name == "read_file":
            self._require_keys(arguments, {"path", "start_line", "end_line"}, {"path"})
            result = self._read(root, arguments)
        elif name in {"search_code", "read_docs"}:
            self._require_keys(arguments, {"query", "path"}, {"query"})
            result = self._search(root, arguments, docs_only=name == "read_docs")
        else:
            raise ToolRejected(f"unknown tool {name!r}")
        encoded = json.dumps(result, separators=(",", ":"), ensure_ascii=False)
        if len(encoded.encode()) > self.max_result_bytes:
            raise ToolRejected("tool result exceeds byte limit")
        return encoded

    @staticmethod
    def _require_keys(arguments: dict[str, Any], allowed: set[str], required: set[str]) -> None:
        if set(arguments) - allowed or not required.issubset(arguments):
            raise ToolRejected("unknown or missing tool argument")

    def _resolve(self, root: Path, raw: Any, *, directory_allowed: bool) -> Path:
        if raw in (None, "") and directory_allowed:
            return root
        if not isinstance(raw, str) or not raw or Path(raw).is_absolute() or "\x00" in raw:
            raise ToolRejected("path must be repository-relative")
        if ".." in Path(raw).parts:
            raise ToolRejected("path escapes repository")
        try:
            path = (root / raw).resolve(strict=True)
            path.relative_to(root)
        except (OSError, ValueError) as exc:
            raise ToolRejected("path escapes repository or does not exist") from exc
        if path.is_dir() and not directory_allowed:
            raise ToolRejected("path must be a file")
        return path

    def _allowed(self, root: Path, path: Path, *, docs_only: bool) -> bool:
        relative = path.relative_to(root)
        lower_parts = {part.lower() for part in relative.parts}
        name = path.name.lower()
        if lower_parts & _EXCLUDED_DIRS or name in _SECRET_NAMES or name.startswith(".env.") or path.suffix.lower() in {".pem", ".key", ".p12", ".pfx"}:
            return False
        if path.suffix.lower() not in _TEXT_EXTENSIONS:
            return False
        return not docs_only or path.suffix.lower() in {".md", ".txt"} or "docs" in lower_parts or name.startswith("readme")

    def _bytes(self, path: Path) -> str:
        size = path.stat().st_size
        if size > self.max_file_bytes:
            raise ToolRejected("file exceeds byte limit")
        data = path.read_bytes()
        if b"\x00" in data:
            raise ToolRejected("binary file is not allowed")
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ToolRejected("non-UTF-8 file is not allowed") from exc

    def _read(self, root: Path, arguments: dict[str, Any]) -> dict[str, Any]:
        path = self._resolve(root, arguments["path"], directory_allowed=False)
        if not self._allowed(root, path, docs_only=False):
            raise ToolRejected("file is not allowlisted text")
        start = arguments.get("start_line", 1)
        end = arguments.get("end_line", start + 199)
        if type(start) is not int or type(end) is not int or start < 1 or end < start or end - start >= 500:
            raise ToolRejected("invalid line range")
        lines = self._bytes(path).splitlines()
        if start > len(lines):
            raise ToolRejected("start line is beyond EOF")
        end = min(end, len(lines))
        return {"path": path.relative_to(root).as_posix(), "start_line": start, "end_line": end, "content": "\n".join(lines[start - 1:end]), "truncated": end < len(lines)}

    def _search(self, root: Path, arguments: dict[str, Any], *, docs_only: bool) -> dict[str, Any]:
        query = arguments["query"]
        if not isinstance(query, str) or not query or len(query.encode()) > 1024:
            raise ToolRejected("invalid query")
        start = self._resolve(root, arguments.get("path", ""), directory_allowed=True)
        candidates = [start] if start.is_file() else sorted(start.rglob("*"))
        matches: list[dict[str, Any]] = []
        for path in candidates:
            if path.is_symlink() or not path.is_file() or not self._allowed(root, path, docs_only=docs_only):
                continue
            for line_number, line in enumerate(self._bytes(path).splitlines(), 1):
                if query.casefold() in line.casefold():
                    matches.append({"path": path.relative_to(root).as_posix(), "line": line_number, "text": line[:512]})
                    if len(matches) == self.max_matches:
                        return {"matches": matches, "truncated": True}
        return {"matches": matches, "truncated": False}
