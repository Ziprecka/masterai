"""Event schemas streamed over WebSocket to the dashboard.

Every event has a `type` discriminator. Most carry an `agent_id`; events
emitted from contexts where there is no concrete agent (project registry,
watchers, conversation, etc.) use the sentinel `agent_id = "system"` so the
existing bus/replay machinery keeps working unchanged.

If you add a new event type, also add a `case "<type>": break;` stub to
`dashboard/lib/store.ts` so the discriminated switch stays exhaustive.
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


SYSTEM_AGENT_ID = "system"


class AgentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    REVIEWING = "reviewing"
    MERGED = "merged"
    FAILED = "failed"
    KILLED = "killed"


class CircuitState(str, Enum):
    CLOSED = "closed"
    HALF_OPEN = "half_open"
    OPEN = "open"


class WatcherMode(str, Enum):
    ALERT_ONLY = "alert_only"
    AUTO_DISPATCH = "auto_dispatch"


class WatcherTrigger(str, Enum):
    CRON = "cron"
    INTERVAL = "interval"
    FILE_CHANGE = "file_change"
    GIT_EVENT = "git_event"


class BaseEvent(BaseModel):
    id: str = Field(default_factory=_id)
    ts: str = Field(default_factory=_now)
    agent_id: str = SYSTEM_AGENT_ID


# ──────────────────────────── agent lifecycle ────────────────────────────


class AgentSpawned(BaseEvent):
    type: Literal["agent_spawned"] = "agent_spawned"
    task: str
    worktree_path: str
    branch: str
    project_id: str | None = None


class AgentStatusChanged(BaseEvent):
    type: Literal["agent_status"] = "agent_status"
    status: AgentStatus
    detail: str | None = None


class ToolCall(BaseEvent):
    type: Literal["tool_call"] = "tool_call"
    tool: str
    input: dict[str, Any]


class ToolResult(BaseEvent):
    type: Literal["tool_result"] = "tool_result"
    tool: str
    is_error: bool
    output_preview: str


class AgentMessage(BaseEvent):
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
    diff_summary: str
    project_id: str | None = None


# ───────────────────────── projects / autonomy ────────────────────────────


class ProjectRegistered(BaseEvent):
    type: Literal["project_registered"] = "project_registered"
    project_id: str
    name: str
    repo_path: str
    base_branch: str


class CircuitStateChanged(BaseEvent):
    type: Literal["circuit_state_changed"] = "circuit_state_changed"
    project_id: str
    state: CircuitState
    consecutive_failures: int


class BudgetExceeded(BaseEvent):
    type: Literal["budget_exceeded"] = "budget_exceeded"
    project_id: str
    tokens_used: int
    daily_budget: int


class RetryScheduled(BaseEvent):
    type: Literal["retry_scheduled"] = "retry_scheduled"
    original_agent_id: str
    attempt: int  # 1-indexed; the about-to-be-spawned attempt
    reason: str


class VerificationStarted(BaseEvent):
    type: Literal["verification_started"] = "verification_started"
    command: str
    project_id: str


class VerificationFinished(BaseEvent):
    type: Literal["verification_finished"] = "verification_finished"
    project_id: str
    passed: bool
    output_preview: str


# ─────────────────────────────── watchers ─────────────────────────────────


class WatcherRegistered(BaseEvent):
    type: Literal["watcher_registered"] = "watcher_registered"
    watcher_id: str
    name: str
    project_id: str
    trigger: WatcherTrigger
    mode: WatcherMode


class WatcherFired(BaseEvent):
    type: Literal["watcher_fired"] = "watcher_fired"
    watcher_id: str
    project_id: str
    proposed_goal: str
    mode: WatcherMode


# ─────────────────────────── conversation / memory ────────────────────────


class ConversationTurn(BaseEvent):
    type: Literal["conversation_turn"] = "conversation_turn"
    role: Literal["user", "assistant"]
    text: str


class MemoryWritten(BaseEvent):
    type: Literal["memory_written"] = "memory_written"
    note_key: str
    preview: str


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
    | ProjectRegistered
    | CircuitStateChanged
    | BudgetExceeded
    | RetryScheduled
    | VerificationStarted
    | VerificationFinished
    | WatcherRegistered
    | WatcherFired
    | ConversationTurn
    | MemoryWritten
)
