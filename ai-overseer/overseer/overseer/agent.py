"""A single agent — one Claude Agent SDK `query()` running in a worktree.

The overseer creates one of these per task, awaits its result, then
moves to review/merge.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from .bus import bus
from .events import (
    AgentFinished,
    AgentSpawned,
    AgentStatus,
    AgentStatusChanged,
    TokenUsage,
)
from .hooks import make_hooks
from .worktree import Worktree, WorktreeManager

# Note: import lazily so the package can be inspected without the SDK installed
def _import_sdk() -> Any:
    from claude_agent_sdk import (  # type: ignore[import-not-found]
        ClaudeAgentOptions,
        AgentDefinition,
        query,
        ResultMessage,
        AssistantMessage,
        SystemMessage,
    )
    return {
        "ClaudeAgentOptions": ClaudeAgentOptions,
        "AgentDefinition": AgentDefinition,
        "query": query,
        "ResultMessage": ResultMessage,
        "AssistantMessage": AssistantMessage,
        "SystemMessage": SystemMessage,
    }


@dataclass
class AgentSpec:
    agent_id: str
    task: str
    task_slug: str
    role: str = "implementer"  # or "reviewer", "researcher", "test-writer"
    allowed_tools: list[str] = field(default_factory=lambda: [
        "Read", "Write", "Edit", "Glob", "Grep", "Bash", "Agent",
    ])
    max_turns: int = 60


# Sub-subagent definitions. The implementer agent can spawn these
# in-process for parallel exploration without bloating its context.
SUBAGENTS = {
    "researcher": {
        "description": "Reads and summarizes code or docs without modifying anything.",
        "prompt": (
            "You are a code researcher. You ONLY read files and report findings. "
            "Never edit. Return a tight summary with concrete file paths and line numbers."
        ),
        "tools": ["Read", "Grep", "Glob"],
    },
    "test-writer": {
        "description": "Writes or extends tests for a specified module.",
        "prompt": (
            "You write thorough tests. Mirror the project's existing test style. "
            "Run the test suite when done and report failures clearly."
        ),
        "tools": ["Read", "Write", "Edit", "Glob", "Grep", "Bash"],
    },
}


class Agent:
    def __init__(self, spec: AgentSpec, worktree: Worktree, wt_manager: WorktreeManager) -> None:
        self.spec = spec
        self.worktree = worktree
        self.wt_manager = wt_manager
        self._task: asyncio.Task[None] | None = None
        self._cancelled = False

    async def start(self) -> None:
        await bus.publish(AgentSpawned(
            agent_id=self.spec.agent_id,
            task=self.spec.task,
            worktree_path=str(self.worktree.path),
            branch=self.worktree.branch,
        ))
        self._task = asyncio.create_task(self._run())

    async def join(self) -> None:
        if self._task:
            await self._task

    async def cancel(self) -> None:
        self._cancelled = True
        if self._task:
            self._task.cancel()
        await bus.publish(AgentStatusChanged(
            agent_id=self.spec.agent_id, status=AgentStatus.KILLED,
        ))

    async def _run(self) -> None:
        sdk = _import_sdk()
        ClaudeAgentOptions = sdk["ClaudeAgentOptions"]
        AgentDefinition = sdk["AgentDefinition"]
        query = sdk["query"]
        ResultMessage = sdk["ResultMessage"]
        AssistantMessage = sdk["AssistantMessage"]

        await bus.publish(AgentStatusChanged(
            agent_id=self.spec.agent_id, status=AgentStatus.RUNNING,
        ))

        agents = {
            name: AgentDefinition(
                description=cfg["description"],
                prompt=cfg["prompt"],
                tools=cfg["tools"],
            )
            for name, cfg in SUBAGENTS.items()
        }

        options = ClaudeAgentOptions(
            cwd=str(self.worktree.path),
            allowed_tools=self.spec.allowed_tools,
            permission_mode="acceptEdits",
            max_turns=self.spec.max_turns,
            agents=agents,
            hooks=make_hooks(self.spec.agent_id),
            system_prompt=(
                f"You are agent `{self.spec.agent_id}` working on branch "
                f"`{self.worktree.branch}` inside an isolated git worktree. "
                "Make focused, minimal changes that satisfy the task. "
                "Run tests if a test command is obvious. "
                "Do NOT push, do NOT touch git history beyond local commits."
            ),
        )

        final_text = ""
        try:
            async for message in query(prompt=self.spec.task, options=options):
                if isinstance(message, AssistantMessage):
                    # Already handled by OnAssistantMessage hook; nothing to do here
                    pass
                elif isinstance(message, ResultMessage):
                    final_text = getattr(message, "result", "") or ""
                    usage = getattr(message, "usage", None)
                    if usage:
                        await bus.publish(TokenUsage(
                            agent_id=self.spec.agent_id,
                            input_tokens=getattr(usage, "input_tokens", 0),
                            output_tokens=getattr(usage, "output_tokens", 0),
                            cumulative_cost_usd=getattr(message, "total_cost_usd", 0.0),
                        ))
        except asyncio.CancelledError:
            return
        except Exception as exc:  # noqa: BLE001
            await bus.publish(AgentStatusChanged(
                agent_id=self.spec.agent_id,
                status=AgentStatus.FAILED,
                detail=f"{type(exc).__name__}: {exc}",
            ))
            return

        diff_summary = await self.wt_manager.diff_stat(self.worktree)
        await bus.publish(AgentFinished(
            agent_id=self.spec.agent_id,
            final_message=final_text,
            diff_summary=diff_summary,
        ))
        await bus.publish(AgentStatusChanged(
            agent_id=self.spec.agent_id, status=AgentStatus.REVIEWING,
        ))
