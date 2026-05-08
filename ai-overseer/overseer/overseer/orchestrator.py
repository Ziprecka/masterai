"""Multi-project orchestrator.

Holds a `ProjectRegistry` and lazily creates one `WorktreeManager` per
project_id. Every Task is now tagged with the project it targets so the
planner, autonomy controller, watchers, and dashboard can route work and
budgets per project.
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
from .events import (
    AgentStatus,
    AgentStatusChanged,
    BudgetExceeded,
)
from .identity import Identity
from .projects import Project, ProjectRegistry
from .worktree import WorktreeManager


@dataclass
class Task:
    id: str
    title: str
    description: str
    project_id: str
    role: str = "implementer"
    depends_on: list[str] = field(default_factory=list)


@dataclass
class TrackedAgent:
    agent: Agent
    task: Task
    project_id: str
    status: AgentStatus = AgentStatus.PENDING
    attempt: int = 1


class Orchestrator:
    def __init__(
        self,
        registry: ProjectRegistry,
        identity: Identity | None = None,
    ) -> None:
        self.registry = registry
        self.identity = identity
        self.agents: dict[str, TrackedAgent] = {}
        self._wt_managers: dict[str, WorktreeManager] = {}
        self._lock = asyncio.Lock()

    # ─────────────────────────── worktree managers ─────────────────────

    def worktree_manager_for(self, project_id: str) -> WorktreeManager:
        wm = self._wt_managers.get(project_id)
        if wm is not None:
            return wm
        project = self.registry.get(project_id)
        if project is None:
            raise KeyError(f"unknown project {project_id!r}")
        wm = WorktreeManager(
            project.repo_path, project.worktree_root, project.base_branch
        )
        self._wt_managers[project_id] = wm
        return wm

    # ─────────────────────────── Planning ──────────────────────────────

    async def plan(self, goal: str, project_ids: list[str]) -> list[Task]:
        """Decompose a goal into per-project tasks via a planning agent.

        Falls back to a one-task-per-project plan when the SDK is missing
        (offline dashboard dev) or the planner returns nothing usable.
        """
        if not project_ids:
            raise ValueError("project_ids must not be empty")

        # Validate up front so the planner only sees real projects.
        projects: list[Project] = []
        for pid in project_ids:
            p = self.registry.get(pid)
            if p is None:
                raise KeyError(f"unknown project {pid!r}")
            projects.append(p)

        try:
            from claude_agent_sdk import (  # type: ignore[import-not-found]
                ClaudeAgentOptions,
                query,
                ResultMessage,
            )
        except ImportError:
            return [
                Task(id=_short_id(), title=goal, description=goal, project_id=pid)
                for pid in project_ids
            ]

        project_brief = "\n".join(
            f"- id={p.id} name={p.name!r} repo={p.repo_path} base={p.base_branch}"
            for p in projects
        )
        planner_prompt = (
            "You are a planning agent for a multi-project orchestrator. "
            "Decompose the user's goal into 2–6 bounded, parallelizable tasks. "
            "Each task targets exactly one project from the list below; split "
            "work across projects when the goal spans them. Each task must be "
            "independently mergeable. Return ONLY a JSON array of objects with "
            "keys: title, description, role, project_id. Roles must be one of: "
            "implementer, reviewer, test-writer, researcher.\n\n"
            f"AVAILABLE PROJECTS:\n{project_brief}\n\n"
            f"GOAL:\n{goal}"
        )
        system_prompt = "You output only valid JSON. No prose, no markdown fences."
        if self.identity is not None:
            system_prompt = self.identity.system_prompt_prefix() + "\n\n" + system_prompt

        options = ClaudeAgentOptions(
            allowed_tools=[],
            max_turns=4,
            system_prompt=system_prompt,
        )

        text = ""
        async for msg in query(prompt=planner_prompt, options=options):
            if isinstance(msg, ResultMessage):
                text = (getattr(msg, "result", "") or "").strip()

        return _parse_plan(text, fallback_goal=goal, project_ids=project_ids)

    # ─────────────────────────── Lifecycle ─────────────────────────────

    async def spawn_for_task(self, task: Task) -> str | None:
        """Spawn an agent for a task. Returns the agent_id, or None if the
        project's daily token budget is exhausted (a `BudgetExceeded` event
        is emitted in that case).
        """
        project = self.registry.get(task.project_id)
        if project is None:
            raise KeyError(f"unknown project {task.project_id!r}")

        if not self.registry.is_within_budget(task.project_id):
            await bus.publish(BudgetExceeded(
                project_id=task.project_id,
                tokens_used=project.tokens_used_today,
                daily_budget=project.daily_token_budget,
            ))
            return None

        wm = self.worktree_manager_for(task.project_id)
        agent_id = _short_id()
        wt = await wm.create(agent_id, _slugify(task.title))
        spec = AgentSpec(
            agent_id=agent_id,
            task=f"{task.title}\n\n{task.description}",
            task_slug=_slugify(task.title),
            role=task.role,
            project_id=task.project_id,
            identity_prefix=(
                self.identity.system_prompt_prefix() if self.identity is not None else None
            ),
        )
        agent = Agent(spec, wt, wm)
        async with self._lock:
            self.agents[agent_id] = TrackedAgent(
                agent=agent, task=task, project_id=task.project_id,
            )
        await agent.start()
        return agent_id

    async def plan_and_spawn(self, goal: str, project_ids: list[str]) -> list[str]:
        """Convenience wrapper used by the watcher engine and conversation tools."""
        tasks = await self.plan(goal, project_ids)
        spawned: list[str] = []
        for t in tasks:
            agent_id = await self.spawn_for_task(t)
            if agent_id:
                spawned.append(agent_id)
        return spawned

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
        wm = self.worktree_manager_for(tracked.project_id)
        wt = tracked.agent.worktree
        await wm.merge(wt, message=f"agent({agent_id}): {tracked.task.title}")
        await wm.cleanup(wt, delete_branch=True)
        await bus.publish(AgentStatusChanged(
            agent_id=agent_id, status=AgentStatus.MERGED,
        ))

    async def discard(self, agent_id: str) -> None:
        async with self._lock:
            tracked = self.agents.get(agent_id)
        if not tracked:
            return
        wm = self.worktree_manager_for(tracked.project_id)
        await wm.cleanup(tracked.agent.worktree, delete_branch=True)
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


def _parse_plan(
    text: str, fallback_goal: str, project_ids: list[str]
) -> list[Task]:
    fallback = [
        Task(id=_short_id(), title=fallback_goal, description=fallback_goal, project_id=pid)
        for pid in project_ids
    ]
    if not text:
        return fallback
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return fallback
    if not isinstance(raw, list):
        return fallback

    valid_pids = set(project_ids)
    out: list[Task] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        pid = str(item.get("project_id", "")).strip()
        # Drop tasks routed to unknown projects rather than silently
        # mis-targeting work.
        if pid not in valid_pids:
            continue
        out.append(Task(
            id=_short_id(),
            title=title,
            description=str(item.get("description", title)),
            role=str(item.get("role", "implementer")),
            project_id=pid,
        ))
    return out or fallback
