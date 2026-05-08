"""Proactive watcher system.

A watcher binds a trigger (cron / interval / file change / git event) to a
goal template. When the trigger fires for a project that's within budget
and past cooldown, the watcher either emits a `WatcherFired` event for the
human (ALERT_ONLY) or directly dispatches the goal to the orchestrator
(AUTO_DISPATCH).

To keep humans in the loop on new automation, watchers spend their first
3 firings in ALERT_ONLY mode regardless of the user's chosen final mode.
After that, they auto-promote. This is the trust ramp.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .bus import bus
from .events import (
    BudgetExceeded,
    WatcherFired,
    WatcherMode,
    WatcherRegistered,
    WatcherTrigger,
)

if TYPE_CHECKING:
    from .orchestrator import Orchestrator
    from .projects import ProjectRegistry


PROMOTION_THRESHOLD = 3  # firings in ALERT_ONLY before auto-promotion


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _short_id() -> str:
    return uuid.uuid4().hex[:8]


@dataclass
class Watcher:
    id: str
    name: str
    project_id: str
    trigger: WatcherTrigger
    trigger_config: dict[str, Any]
    goal_template: str
    # The mode the user *requested*. While ``firings_observed`` <
    # PROMOTION_THRESHOLD we override at fire-time to ALERT_ONLY.
    mode: WatcherMode = WatcherMode.ALERT_ONLY
    cooldown_seconds: int = 300
    last_fired_at: str | None = None
    enabled: bool = True
    firings_observed: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["trigger"] = self.trigger.value
        d["mode"] = self.mode.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Watcher":
        return cls(
            id=d["id"],
            name=d["name"],
            project_id=d["project_id"],
            trigger=WatcherTrigger(d["trigger"]),
            trigger_config=d.get("trigger_config", {}),
            goal_template=d["goal_template"],
            mode=WatcherMode(d.get("mode", WatcherMode.ALERT_ONLY.value)),
            cooldown_seconds=int(d.get("cooldown_seconds", 300)),
            last_fired_at=d.get("last_fired_at"),
            enabled=bool(d.get("enabled", True)),
            firings_observed=int(d.get("firings_observed", 0)),
        )

    # Effective mode at fire-time, factoring in the trust ramp.
    def effective_mode(self) -> WatcherMode:
        if self.firings_observed < PROMOTION_THRESHOLD:
            return WatcherMode.ALERT_ONLY
        return self.mode


class WatcherRegistry:
    def __init__(self, path: Path | None = None) -> None:
        self.path = (path or Path.home() / ".overseer" / "watchers.json").expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._watchers: dict[str, Watcher] = {}
        self._lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            return
        for item in raw.get("watchers", []):
            try:
                w = Watcher.from_dict(item)
                self._watchers[w.id] = w
            except (KeyError, ValueError):
                continue

    def _save(self) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        payload = {"watchers": [w.to_dict() for w in self._watchers.values()]}
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, self.path)

    def register(self, w: Watcher) -> Watcher:
        with self._lock:
            self._watchers[w.id] = w
            self._save()
        return w

    def get(self, watcher_id: str) -> Watcher | None:
        with self._lock:
            return self._watchers.get(watcher_id)

    def list_all(self) -> list[Watcher]:
        with self._lock:
            return list(self._watchers.values())

    def delete(self, watcher_id: str) -> bool:
        with self._lock:
            if watcher_id not in self._watchers:
                return False
            del self._watchers[watcher_id]
            self._save()
            return True

    def toggle(self, watcher_id: str) -> Watcher | None:
        with self._lock:
            w = self._watchers.get(watcher_id)
            if not w:
                return None
            w.enabled = not w.enabled
            self._save()
            return w

    def record_fire(self, watcher_id: str) -> Watcher | None:
        with self._lock:
            w = self._watchers.get(watcher_id)
            if not w:
                return None
            w.firings_observed += 1
            w.last_fired_at = _now_iso()
            self._save()
            return w


class WatcherEngine:
    """Async loop that dispatches watcher firings.

    The engine holds references to the project registry (for budget checks),
    the orchestrator (for AUTO_DISPATCH), and the watcher registry (for
    state). Cron is implemented as a coarse minute-tick — for sub-minute
    precision use INTERVAL.
    """

    def __init__(
        self,
        watchers: WatcherRegistry,
        projects: "ProjectRegistry",
        orchestrator: "Orchestrator",
        *,
        tick_seconds: float = 1.0,
        git_poll_seconds: float = 60.0,
    ) -> None:
        self.watchers = watchers
        self.projects = projects
        self.orchestrator = orchestrator
        self.tick_seconds = tick_seconds
        self.git_poll_seconds = git_poll_seconds
        self._task: asyncio.Task[None] | None = None
        self._file_tasks: dict[str, asyncio.Task[None]] = {}
        self._stopped = asyncio.Event()
        # Last seen git head per project, so GIT_EVENT only fires on change.
        self._last_git_head: dict[str, str] = {}
        # Cron tick guards: avoid firing twice within the same minute.
        self._last_cron_minute: dict[str, str] = {}

    # ───────────────── public ─────────────────

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())
        # Bring up file watchers for any registered FILE_CHANGE watchers
        for w in self.watchers.list_all():
            if w.trigger == WatcherTrigger.FILE_CHANGE and w.enabled:
                self._ensure_file_task(w)

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        for t in list(self._file_tasks.values()):
            t.cancel()
        self._file_tasks.clear()

    async def fire(
        self, watcher: Watcher, context: dict[str, Any] | None = None
    ) -> bool:
        """Fire a watcher once. Returns True if it actually dispatched/alerted.

        Cooldown, project-budget, and disabled checks all gate here so unit
        tests and triggers share identical policy.
        """
        if not watcher.enabled:
            return False
        if not _cooldown_elapsed(watcher):
            return False
        # Refuse to dispatch when the project is over its daily token budget.
        # ALERT_ONLY firings still go through — they don't spend tokens.
        eff_mode = watcher.effective_mode()
        if eff_mode == WatcherMode.AUTO_DISPATCH and not self.projects.is_within_budget(
            watcher.project_id
        ):
            project = self.projects.get(watcher.project_id)
            if project is not None:
                await bus.publish(BudgetExceeded(
                    project_id=watcher.project_id,
                    tokens_used=project.tokens_used_today,
                    daily_budget=project.daily_token_budget,
                ))
            return False

        try:
            goal = watcher.goal_template.format(**(context or {}))
        except (KeyError, IndexError):
            # If the template references missing fields, fall back to raw
            # template + context dump rather than failing the firing.
            goal = f"{watcher.goal_template}\n\ncontext={context or {}}"

        self.watchers.record_fire(watcher.id)

        await bus.publish(WatcherFired(
            watcher_id=watcher.id,
            project_id=watcher.project_id,
            proposed_goal=goal,
            mode=eff_mode,
        ))

        if eff_mode == WatcherMode.AUTO_DISPATCH:
            try:
                await self.orchestrator.plan_and_spawn(goal, [watcher.project_id])
            except Exception:
                # Don't let a single bad dispatch tear down the loop.
                pass
        return True

    # ───────────────── internal loop ─────────────────

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Keep ticking even on bad watcher config.
                pass
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self.tick_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        now = datetime.now(timezone.utc)
        for w in self.watchers.list_all():
            if not w.enabled:
                continue
            if w.trigger == WatcherTrigger.INTERVAL:
                await self._maybe_fire_interval(w, now)
            elif w.trigger == WatcherTrigger.CRON:
                await self._maybe_fire_cron(w, now)
            elif w.trigger == WatcherTrigger.GIT_EVENT:
                await self._maybe_fire_git(w, now)
            elif w.trigger == WatcherTrigger.FILE_CHANGE:
                # File watchers run as their own asyncio.Tasks; just make
                # sure one exists for any newly-enabled watcher.
                self._ensure_file_task(w)

    async def _maybe_fire_interval(self, w: Watcher, now: datetime) -> None:
        every = float(w.trigger_config.get("seconds", 0) or 0)
        if every <= 0:
            return
        if w.last_fired_at is None:
            elapsed = float("inf")
        else:
            try:
                last = datetime.fromisoformat(w.last_fired_at)
            except ValueError:
                last = now
            elapsed = (now - last).total_seconds()
        if elapsed >= every:
            await self.fire(w, {"now": now.isoformat()})

    async def _maybe_fire_cron(self, w: Watcher, now: datetime) -> None:
        # Minimal 5-field cron: minute hour day month weekday. ``*`` wildcard
        # and integer literals only — enough for "every hour at :00" etc.
        expr = str(w.trigger_config.get("expression", "")).strip()
        if not expr:
            return
        if not _cron_match(expr, now):
            return
        bucket = now.strftime("%Y-%m-%dT%H:%M")
        if self._last_cron_minute.get(w.id) == bucket:
            return
        self._last_cron_minute[w.id] = bucket
        await self.fire(w, {"now": now.isoformat()})

    async def _maybe_fire_git(self, w: Watcher, now: datetime) -> None:
        every = self.git_poll_seconds
        if w.last_fired_at:
            try:
                last = datetime.fromisoformat(w.last_fired_at)
                if (now - last).total_seconds() < every:
                    return
            except ValueError:
                pass
        project = self.projects.get(w.project_id)
        if project is None:
            return
        head = await _git_head(project.repo_path, project.base_branch)
        if head is None:
            return
        prev = self._last_git_head.get(w.project_id)
        self._last_git_head[w.project_id] = head
        if prev is not None and prev != head:
            await self.fire(w, {"head": head, "previous_head": prev})

    def _ensure_file_task(self, w: Watcher) -> None:
        if w.id in self._file_tasks and not self._file_tasks[w.id].done():
            return
        project = self.projects.get(w.project_id)
        if project is None:
            return
        path = w.trigger_config.get("path") or str(project.repo_path)
        glob = w.trigger_config.get("glob")  # optional pattern filter
        self._file_tasks[w.id] = asyncio.create_task(
            self._watch_files(w.id, path, glob)
        )

    async def _watch_files(self, watcher_id: str, path: str, glob: str | None) -> None:
        try:
            from watchfiles import awatch  # type: ignore[import-not-found]
        except ImportError:
            return
        try:
            async for changes in awatch(path):
                w = self.watchers.get(watcher_id)
                if w is None or not w.enabled:
                    return
                paths = [p for _, p in changes]
                if glob:
                    import fnmatch

                    paths = [p for p in paths if fnmatch.fnmatch(p, glob)]
                    if not paths:
                        continue
                await self.fire(w, {"paths": paths, "first": paths[0]})
        except asyncio.CancelledError:
            return
        except Exception:
            return


# ──────────────────────────── helpers ────────────────────────────


def _cooldown_elapsed(w: Watcher) -> bool:
    if w.last_fired_at is None:
        return True
    try:
        last = datetime.fromisoformat(w.last_fired_at)
    except ValueError:
        return True
    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
    return elapsed >= w.cooldown_seconds


_CRON_FIELD = re.compile(r"^(\*|\d+)$")


def _cron_match(expr: str, now: datetime) -> bool:
    fields = expr.split()
    if len(fields) != 5:
        return False
    minute, hour, day, month, weekday = fields
    values = (now.minute, now.hour, now.day, now.month, now.isoweekday() % 7)
    for f, v in zip((minute, hour, day, month, weekday), values):
        if not _CRON_FIELD.match(f):
            return False
        if f == "*":
            continue
        if int(f) != v:
            return False
    return True


async def _git_head(repo: Path, branch: str) -> str | None:
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "rev-parse", branch,
            cwd=str(repo),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0:
            return None
        return out.decode().strip() or None
    except FileNotFoundError:
        return None


def emit_registered(w: Watcher) -> WatcherRegistered:
    return WatcherRegistered(
        watcher_id=w.id,
        name=w.name,
        project_id=w.project_id,
        trigger=w.trigger,
        mode=w.mode,
    )


def make_watcher(
    *,
    name: str,
    project_id: str,
    trigger: WatcherTrigger,
    trigger_config: dict[str, Any],
    goal_template: str,
    mode: WatcherMode = WatcherMode.ALERT_ONLY,
    cooldown_seconds: int = 300,
    enabled: bool = True,
    watcher_id: str | None = None,
) -> Watcher:
    return Watcher(
        id=watcher_id or _short_id(),
        name=name,
        project_id=project_id,
        trigger=trigger,
        trigger_config=trigger_config,
        goal_template=goal_template,
        mode=mode,
        cooldown_seconds=cooldown_seconds,
        enabled=enabled,
    )
