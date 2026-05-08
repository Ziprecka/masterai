"""The master overseer.

Two responsibilities:

1. **Planning** — turn a high-level user goal into a list of bounded
   tasks. The default planner uses Claude to produce the task list;
   you can swap in a deterministic planner for predictable workflows.

2. **Lifecycle** — spawn agents, track them, surface their state to
   the dashboard, and decide what to do when they finish (merge / ask
   user to review / re-spawn with adjusted instructions).
"""
from __future__ import annotations

import asyncio
import json
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .agent import Agent, AgentSpec
from .bus import bus
from .events import AgentStatus, AgentStatusChanged
from .worktree import WorktreeManager


@dataclass
class Task:
    id: str
    title: str
    description: str
    role: str = "implementer"
    depends_on: list[str] = field(default_factory=list)


@dataclass
class TrackedAgent:
    agent: Agent
    task: Task
    status: AgentStatus = AgentStatus.PENDING


class Orchestrator:
    def __init__(self, repo_root: Path, worktree_root: Path, base_branch: str = "main") -> None:
        self.wt_manager = WorktreeManager(repo_root, worktree_root, base_branch)
        self.agents: dict[str, TrackedAgent] = {}
        self._lock = asyncio.Lock()

    # ─────────────────────────── Planning ──────────────────────────────

    async def plan(self, goal: str) -> list[Task]:
        """Decompose a goal into bounded tasks using a planning agent.

        Falls back to a single-task plan if the SDK isn't available
        (handy for offline development of the dashboard).
        """
        try:
            from claude_agent_sdk import (  # type: ignore[import-not-found]
                ClaudeAgentOptions, query, ResultMessage,
            )
        except ImportError:
            return [Task(id=_short_id(), title=goal, description=goal)]

        planner_prompt = (
            "You are a planning agent. Decompose the user's goal into 2–6 bounded, "
            "parallelizable tasks. Each task should be independently mergeable. "
            "Return ONLY a JSON array of objects with keys: title, description, role. "
            "Roles must be one of: implementer, reviewer, test-writer, researcher.\n\n"
            f"GOAL:\n{goal}"
        )
        options = ClaudeAgentOptions(
            allowed_tools=[],  # planner doesn't touch the filesystem
            max_turns=4,
            system_prompt="You output only valid JSON. No prose, no markdown fences.",
        )

        text = ""
        async for msg in query(prompt=planner_prompt, options=options):
            if isinstance(msg, ResultMessage):
                text = (getattr(msg, "result", "") or "").strip()

        return _parse_plan(text, fallback_goal=goal)

    # ─────────────────────────── Lifecycle ─────────────────────────────

    async def spawn_for_task(self, task: Task) -> str:
        agent_id = _short_id()
        wt = await self.wt_manager.create(agent_id, _slugify(task.title))
        spec = AgentSpec(
            agent_id=agent_id,
            task=f"{task.title}\n\n{task.description}",
            task_slug=_slugify(task.title),
            role=task.role,
        )
        agent = Agent(spec, wt, self.wt_manager)
        async with self._lock:
            self.agents[agent_id] = TrackedAgent(agent=agent, task=task)
        await agent.start()
        return agent_id

    async def kill(self, agent_id: str) -> None:
        async with self._lock:
            tracked = self.agents.get(agent_id)
        if tracked:
            await tracked.agent.cancel()

    async def merge(self, agent_id: str) -> None:
        async with self._lock:
            tracked = self.agents.get(agent_id)
        if not tracked:
            return
        wt = tracked.agent.worktree
        await self.wt_manager.merge(wt, message=f"agent({agent_id}): {tracked.task.title}")
        await self.wt_manager.cleanup(wt, delete_branch=True)
        await bus.publish(AgentStatusChanged(
            agent_id=agent_id, status=AgentStatus.MERGED,
        ))

    async def discard(self, agent_id: str) -> None:
        async with self._lock:
            tracked = self.agents.get(agent_id)
        if not tracked:
            return
        await self.wt_manager.cleanup(tracked.agent.worktree, delete_branch=True)
        await bus.publish(AgentStatusChanged(
            agent_id=agent_id, status=AgentStatus.KILLED,
            detail="Discarded by overseer",
        ))


# ─────────────────────────── helpers ───────────────────────────────────

def _short_id() -> str:
    return uuid.uuid4().hex[:8]


def _slugify(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s.lower()).strip("-")
    return s[:40] or "task"


def _parse_plan(text: str, fallback_goal: str) -> list[Task]:
    if not text:
        return [Task(id=_short_id(), title=fallback_goal, description=fallback_goal)]
    # Strip accidental code fences
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return [Task(id=_short_id(), title=fallback_goal, description=fallback_goal)]
    if not isinstance(raw, list):
        return [Task(id=_short_id(), title=fallback_goal, description=fallback_goal)]
    out: list[Task] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        out.append(Task(
            id=_short_id(),
            title=title,
            description=str(item.get("description", title)),
            role=str(item.get("role", "implementer")),
        ))
    return out or [Task(id=_short_id(), title=fallback_goal, description=fallback_goal)]
