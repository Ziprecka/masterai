"""Claude Agent SDK hooks → dashboard events.

The SDK fires PreToolUse / PostToolUse / SubagentStart / SubagentStop
callbacks during the agent loop. We use them to:

  1. Stream activity to the dashboard in real time.
  2. Enforce safety (deny dangerous Bash commands, etc).
  3. Log everything for later review.

Hook signatures may evolve — check the installed claude_agent_sdk
version's docs. The shape below matches the Python SDK surface as of
early 2026.
"""
from __future__ import annotations

from typing import Any

from .bus import bus
from .events import (
    AgentMessage,
    FileChanged,
    ToolCall,
    ToolResult,
)

# Bash commands the overseer will refuse to run, regardless of the agent's intent.
BASH_DENYLIST = (
    "rm -rf /",
    "git push",        # Only the overseer pushes
    "sudo ",
    "curl ",           # Force web access through allow-listed tools instead
    "wget ",
    ":(){",            # fork bomb
)

WRITE_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}


def make_hooks(agent_id: str) -> dict[str, Any]:
    """Return a dict of hook callbacks bound to a specific agent_id."""

    async def pre_tool_use(tool_name: str, tool_input: dict[str, Any]) -> dict[str, Any]:
        # Safety gate
        if tool_name == "Bash":
            cmd = str(tool_input.get("command", ""))
            if any(bad in cmd for bad in BASH_DENYLIST):
                return {
                    "behavior": "deny",
                    "message": f"Overseer denied bash command containing forbidden pattern: {cmd!r}",
                }

        await bus.publish(ToolCall(
            agent_id=agent_id,
            tool=tool_name,
            input=_truncate_dict(tool_input),
        ))
        return {"behavior": "allow"}

    async def post_tool_use(tool_name: str, tool_input: dict[str, Any], tool_output: Any, is_error: bool) -> None:
        preview = _preview(tool_output)
        await bus.publish(ToolResult(
            agent_id=agent_id,
            tool=tool_name,
            is_error=is_error,
            output_preview=preview,
        ))
        # If the tool wrote a file, emit a file_changed event too
        if tool_name in WRITE_TOOLS and not is_error:
            path = tool_input.get("file_path") or tool_input.get("path")
            if path:
                change = "created" if tool_name == "Write" else "modified"
                await bus.publish(FileChanged(
                    agent_id=agent_id, path=str(path), change=change,
                ))

    async def on_assistant_message(text: str) -> None:
        await bus.publish(AgentMessage(agent_id=agent_id, text=text))

    return {
        "PreToolUse": pre_tool_use,
        "PostToolUse": post_tool_use,
        "OnAssistantMessage": on_assistant_message,
    }


def _truncate_dict(d: dict[str, Any], max_len: int = 500) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        s = str(v)
        out[k] = s if len(s) <= max_len else s[:max_len] + f"…(+{len(s) - max_len})"
    return out


def _preview(v: Any, max_len: int = 800) -> str:
    s = str(v)
    return s if len(s) <= max_len else s[:max_len] + f"…(+{len(s) - max_len})"
