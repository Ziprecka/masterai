"""Watcher tests: cooldown, trust ramp, disabled-watcher behaviour."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from overseer.bus import bus
from overseer.events import WatcherFired, WatcherMode, WatcherTrigger
from overseer.projects import Project, ProjectRegistry
from overseer.watchers import (
    PROMOTION_THRESHOLD,
    WatcherEngine,
    WatcherRegistry,
    make_watcher,
)


@dataclass
class _RecordingOrch:
    plan_calls: list = field(default_factory=list)

    async def plan_and_spawn(self, goal: str, project_ids):
        self.plan_calls.append((goal, list(project_ids)))
        return ["aid-1"]


def _make_project(reg: ProjectRegistry) -> Project:
    return reg.register(Project(
        id="p", name="p",
        repo_path=Path("/tmp/repo"),
        worktree_root=Path("/tmp/wt"),
    ))


async def _collect_fired() -> tuple[asyncio.Task, list[WatcherFired]]:
    received: list[WatcherFired] = []

    async def collect():
        async for e in bus.subscribe():
            if isinstance(e, WatcherFired):
                received.append(e)

    task = asyncio.create_task(collect())
    await asyncio.sleep(0)
    return task, received


# ─────────────────────── tests ───────────────────────


async def test_disabled_watcher_does_not_fire(
    project_registry: ProjectRegistry, watcher_registry: WatcherRegistry
) -> None:
    _make_project(project_registry)
    w = make_watcher(
        name="w", project_id="p",
        trigger=WatcherTrigger.INTERVAL,
        trigger_config={"seconds": 1},
        goal_template="g", enabled=False,
    )
    watcher_registry.register(w)

    orch = _RecordingOrch()
    eng = WatcherEngine(watcher_registry, project_registry, orch)
    fired = await eng.fire(w)
    assert fired is False
    assert orch.plan_calls == []


async def test_cooldown_blocks_repeat_fires(
    project_registry: ProjectRegistry, watcher_registry: WatcherRegistry
) -> None:
    _make_project(project_registry)
    w = make_watcher(
        name="w", project_id="p",
        trigger=WatcherTrigger.INTERVAL,
        trigger_config={"seconds": 0.001},
        goal_template="g",
        cooldown_seconds=3600,
    )
    watcher_registry.register(w)
    orch = _RecordingOrch()
    eng = WatcherEngine(watcher_registry, project_registry, orch)

    listener, received = await _collect_fired()

    # First fire goes through.
    assert await eng.fire(w) is True
    # Second fire is blocked by cooldown.
    fresh = watcher_registry.get(w.id)
    assert await eng.fire(fresh) is False

    await asyncio.sleep(0.05)
    listener.cancel()
    try:
        await listener
    except (asyncio.CancelledError, Exception):
        pass

    assert len(received) == 1


async def test_trust_ramp_promotes_after_threshold(
    project_registry: ProjectRegistry, watcher_registry: WatcherRegistry
) -> None:
    """Watchers spend the first PROMOTION_THRESHOLD firings as ALERT_ONLY,
    then auto-promote to whatever mode the user originally requested."""
    _make_project(project_registry)
    w = make_watcher(
        name="w", project_id="p",
        trigger=WatcherTrigger.INTERVAL,
        trigger_config={"seconds": 0.001},
        goal_template="g",
        mode=WatcherMode.AUTO_DISPATCH,  # user asked for auto
        cooldown_seconds=0,
    )
    watcher_registry.register(w)
    orch = _RecordingOrch()
    eng = WatcherEngine(watcher_registry, project_registry, orch)

    # First PROMOTION_THRESHOLD firings should NOT dispatch.
    for _ in range(PROMOTION_THRESHOLD):
        current = watcher_registry.get(w.id)
        # Bypass cooldown by manually clearing last_fired_at.
        current.last_fired_at = None
        watcher_registry.register(current)
        assert await eng.fire(current) is True
    assert orch.plan_calls == []

    # The next firing — once firings_observed >= threshold — uses the
    # user's chosen mode (AUTO_DISPATCH) and actually dispatches.
    current = watcher_registry.get(w.id)
    current.last_fired_at = None
    watcher_registry.register(current)
    assert await eng.fire(current) is True
    assert len(orch.plan_calls) == 1
    assert orch.plan_calls[0][0] == "g"
    assert orch.plan_calls[0][1] == ["p"]


async def test_alert_only_never_dispatches(
    project_registry: ProjectRegistry, watcher_registry: WatcherRegistry
) -> None:
    _make_project(project_registry)
    w = make_watcher(
        name="w", project_id="p",
        trigger=WatcherTrigger.INTERVAL,
        trigger_config={"seconds": 0.001},
        goal_template="g",
        mode=WatcherMode.ALERT_ONLY,
        cooldown_seconds=0,
    )
    watcher_registry.register(w)
    orch = _RecordingOrch()
    eng = WatcherEngine(watcher_registry, project_registry, orch)

    for _ in range(PROMOTION_THRESHOLD + 2):
        current = watcher_registry.get(w.id)
        current.last_fired_at = None
        watcher_registry.register(current)
        await eng.fire(current)

    assert orch.plan_calls == []


async def test_persistence_roundtrip(tmp_path: Path) -> None:
    reg1 = WatcherRegistry(tmp_path / "watchers.json")
    w = make_watcher(
        name="w", project_id="p",
        trigger=WatcherTrigger.CRON,
        trigger_config={"expression": "0 * * * *"},
        goal_template="check things",
        mode=WatcherMode.AUTO_DISPATCH,
    )
    reg1.register(w)

    reg2 = WatcherRegistry(tmp_path / "watchers.json")
    loaded = reg2.get(w.id)
    assert loaded is not None
    assert loaded.trigger == WatcherTrigger.CRON
    assert loaded.mode == WatcherMode.AUTO_DISPATCH
    assert loaded.goal_template == "check things"
