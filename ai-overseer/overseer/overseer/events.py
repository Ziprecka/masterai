"""Event schemas streamed over WebSocket to the dashboard.

Every event has a `type` discriminator and an `agent_id`. The dashboard
uses these to update the right card without re-fetching state.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _id() -> str:
    return uuid4().hex[:12]


class AgentStatus(str, Enum):
    PENDING = "pending"      # task created, not yet started
    RUNNING = "running"      # SDK query in progress
    REVIEWING = "reviewing"  # overseer inspecting diff
    MERGED = "merged"        # work integrated to main
    FAILED = "failed"
    KILLED = "killed"


class BaseEvent(BaseModel):
    id: str = Field(default_factory=_id)
    ts: str = Field(default_factory=_now)
    agent_id: str


class AgentSpawned(BaseEvent):
    type: Literal["agent_spawned"] = "agent_spawned"
    task: str
    worktree_path: str
    branch: str


class AgentStatusChanged(BaseEvent):
    type: Literal["agent_status"] = "agent_status"
    status: AgentStatus
    detail: str | None = None


class ToolCall(BaseEvent):
    """Emitted from a PreToolUse hook."""
    type: Literal["tool_call"] = "tool_call"
    tool: str
    input: dict[str, Any]


class ToolResult(BaseEvent):
    """Emitted from a PostToolUse hook."""
    type: Literal["tool_result"] = "tool_result"
    tool: str
    is_error: bool
    output_preview: str  # truncated


class AgentMessage(BaseEvent):
    """Text the agent emitted (assistant turn)."""
    type: Literal["agent_message"] = "agent_message"
    text: str


class TokenUsage(BaseEvent):
    type: Literal["token_usage"] = "token_usage"
    input_tokens: int
    output_tokens: int
    cumulative_cost_usd: float


class FileChanged(BaseEvent):
    type: Literal["file_changed"] = "file_changed"
    path: str
    change: Literal["created", "modified", "deleted"]


class AgentFinished(BaseEvent):
    type: Literal["agent_finished"] = "agent_finished"
    final_message: str
    diff_summary: str  # `git diff --stat` of the worktree


# Discriminated union for the WS channel
Event = (
    AgentSpawned
    | AgentStatusChanged
    | ToolCall
    | ToolResult
    | AgentMessage
    | TokenUsage
    | FileChanged
    | AgentFinished
)
