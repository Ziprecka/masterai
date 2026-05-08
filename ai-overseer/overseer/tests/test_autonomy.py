"""Autonomy controller tests: verify pass/fail, retries, circuit interplay.

These tests bypass the real orchestrator entirely. We give the controller
fake ``orchestrator`` and ``registry`` objects that record what happens,
then drive ``handle_finished`` directly so the assertions don't have to
race the bus.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

import overseer.autonomy as autonomy_mod
from overseer.autonomy import MAX_RETRIES, AutonomyController
from overseer.bus import bus
from overseer.events import (
    AgentFinished,
    AgentStatus,
    AgentStatusChanged,
    CircuitStateChanged,
    Event,
    RetryScheduled,
    VerificationFinished,
    VerificationStarted,
)
from overseer.orchestrator import Task
from overseer.projects import Project, ProjectRegistry


# ─────────────────────── stand-ins for Orchestrator/Agent ───────────────────────


@dataclass
class _FakeWorktree:
    path: Path


@dataclass
class _FakeAgent:
    worktree: _FakeWorktree


@dataclass
class _FakeTracked:
    task: Task
    project_id: str
    agent: _FakeAgent


@dataclass
class _FakeOrchestrator:
    agents: dict[str, _FakeTracked] = field(default_factory=dict)
    merged: list[str] = field(default_factory=list)
    discarded: list[str] = field(default_factory=list)
    spawned: list[Task] = field(default_factory=list)
    next_agent_id: int = 1
    spawn_returns_none: bool = False

    async def merge(self, agent_id: str) -> None:
        self.merged.append(agent_id)
        # Simulate the orchestrator removing the agent on merge.
        self.agents.pop(agent_id, None)

    async def discard(self, agent_id: str) -> None:
        self.discarded.append(agent_id)
        self.agents.pop(agent_id, None)

    async def spawn_for_task(self, task: Task) -> str | None:
        if self.spawn_returns_none:
            return None
        new_id = f"retry-{self.next_agent_id}"
        self.next_agent_id += 1
        self.spawned.append(task)
        self.agents[new_id] = _FakeTracked(
            task=task,
            project_id=task.project_id,
            agent=_FakeAgent(worktree=_FakeWorktree(path=Path("/tmp/wt"))),
        )
        return new_id


# ─────────────────────── helpers ───────────────────────


async def _collect_events(coro_or_callable, *, types: tuple[type, ...]) -> list[Event]:
    """Subscribe to the bus, run the callable, return matching events."""
    events: list[Event] = []
    received = []

    async def collect():
        async for e in bus.subscribe():
            received.append(e)
            if len(received) >= 50:
                return

    import asyncio
    task = asyncio.create_task(collect())
    await asyncio.sleep(0)  # let subscribe register
    await coro_or_callable
    # Give the bus a tick to flush.
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    for e in received:
        if isinstance(e, types):
            events.append(e)
    return events


def _make_project(reg: ProjectRegistry, *, verify: str | None, auto_merge: bool = True) -> Project:
    return reg.register(Project(
        id="p",
        name="p",
        repo_path=Path("/tmp/repo"),
        worktree_root=Path("/tmp/wt"),
        verify_command=verify,
        auto_merge=auto_merge,
    ))


def _attach_agent(orch: _FakeOrchestrator, agent_id: str = "a1") -> Task:
    task = Task(
        id="t1", title="do thing", description="desc",
        project_id="p", role="implementer",
    )
    orch.agents[agent_id] = _FakeTracked(
        task=task,
        project_id="p",
        agent=_FakeAgent(worktree=_FakeWorktree(path=Path("/tmp/wt"))),
    )
    return task


# ─────────────────────── tests ───────────────────────


async def test_verify_pass_triggers_merge(
    project_registry: ProjectRegistry, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _make_project(project_registry, verify="echo ok")
    orch = _FakeOrchestrator()
    _attach_agent(orch)
    ctl = AutonomyController(orch, project_registry)

    async def fake_verify(self, command: str, cwd):
        return True, "ok\n"
    monkeypatch.setattr(AutonomyController, "_run_verify", fake_verify)

    finished = AgentFinished(
        agent_id="a1", final_message="done", diff_summary="", project_id="p",
    )

    events = await _collect_events(
        ctl.handle_finished(finished),
        types=(VerificationStarted, VerificationFinished, CircuitStateChanged),
    )

    assert orch.merged == ["a1"]
    # Verification events were emitted in order.
    starts = [e for e in events if isinstance(e, VerificationStarted)]
    finishes = [e for e in events if isinstance(e, VerificationFinished)]
    assert len(starts) == 1
    assert len(finishes) == 1
    assert finishes[0].passed is True


async def test_verify_fail_triggers_retry(
    project_registry: ProjectRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    _make_project(project_registry, verify="false")
    orch = _FakeOrchestrator()
    _attach_agent(orch)
    ctl = AutonomyController(orch, project_registry)

    async def fake_verify(self, command, cwd):
        return False, "BOOM"
    monkeypatch.setattr(AutonomyController, "_run_verify", fake_verify)

    finished = AgentFinished(
        agent_id="a1", final_message="done", diff_summary="", project_id="p",
    )

    events = await _collect_events(
        ctl.handle_finished(finished),
        types=(RetryScheduled, AgentStatusChanged),
    )

    retries = [e for e in events if isinstance(e, RetryScheduled)]
    assert len(retries) == 1
    assert retries[0].attempt == 2
    assert orch.discarded == ["a1"]
    assert len(orch.spawned) == 1
    assert "BOOM" in orch.spawned[0].description


async def test_retry_exhaustion_emits_one_failed_event(
    project_registry: ProjectRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The contract: after MAX_RETRIES, exactly one ``failed`` status event."""
    _make_project(project_registry, verify="false")
    orch = _FakeOrchestrator()
    task = _attach_agent(orch, agent_id="a1")
    ctl = AutonomyController(orch, project_registry)

    async def fake_verify(self, command, cwd):
        return False, "fail"
    monkeypatch.setattr(AutonomyController, "_run_verify", fake_verify)

    # Drive MAX_RETRIES + 1 finishes through the controller. Each retry
    # spawns a new agent_id, which we feed back as the next AgentFinished.
    current_id = "a1"
    failed_events: list[AgentStatusChanged] = []

    async def collect_failures():
        async for e in bus.subscribe():
            if isinstance(e, AgentStatusChanged) and e.status == AgentStatus.FAILED:
                failed_events.append(e)

    import asyncio
    listener = asyncio.create_task(collect_failures())
    await asyncio.sleep(0)

    for _ in range(MAX_RETRIES + 1):
        await ctl.handle_finished(
            AgentFinished(
                agent_id=current_id, final_message="", diff_summary="", project_id="p",
            )
        )
        # The controller spawned a new agent; pick that as next.
        if orch.spawned and orch.next_agent_id - 1 >= 1:
            current_id = f"retry-{orch.next_agent_id - 1}"

    await asyncio.sleep(0.05)
    listener.cancel()
    try:
        await listener
    except (asyncio.CancelledError, Exception):
        pass

    assert len(failed_events) == 1, f"expected exactly one failed event, got {failed_events}"
    # And no infinite loop: total spawn attempts capped.
    assert len(orch.spawned) == MAX_RETRIES


async def test_open_circuit_blocks_merge(
    project_registry: ProjectRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    p = _make_project(project_registry, verify=None)
    # Trip the breaker open.
    project_registry.update_circuit(p.id, success=False)
    project_registry.update_circuit(p.id, success=False)
    assert project_registry.get(p.id).circuit_state.value == "open"

    orch = _FakeOrchestrator()
    _attach_agent(orch)
    ctl = AutonomyController(orch, project_registry)

    await ctl.handle_finished(
        AgentFinished(agent_id="a1", final_message="", diff_summary="", project_id="p")
    )
    assert orch.merged == []
    assert orch.spawned == []


async def test_no_verify_command_merges_directly(
    project_registry: ProjectRegistry,
) -> None:
    _make_project(project_registry, verify=None)
    orch = _FakeOrchestrator()
    _attach_agent(orch)
    ctl = AutonomyController(orch, project_registry)

    await ctl.handle_finished(
        AgentFinished(agent_id="a1", final_message="", diff_summary="", project_id="p")
    )
    assert orch.merged == ["a1"]
