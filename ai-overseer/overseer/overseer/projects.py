"""Project registry — multi-tenant configuration for the overseer.

Each project is a target git repo the overseer can dispatch agents against.
Persisted to ``~/.overseer/projects.json`` so the registry survives restarts.

Concurrency model: methods that mutate take an internal lock and persist
synchronously on every change, so callers don't need to think about ordering.
The save path uses a tmp + rename to avoid torn writes.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path

from .events import CircuitState


CIRCUIT_OPEN_THRESHOLD = 2  # consecutive failures that flip CLOSED → OPEN
DEFAULT_DAILY_TOKEN_BUDGET = 500_000


def _today() -> str:
    return date.today().isoformat()


@dataclass
class Project:
    id: str
    name: str
    repo_path: Path
    base_branch: str = "main"
    worktree_root: Path = field(default_factory=lambda: Path.cwd() / ".worktrees")
    auto_merge: bool = True
    verify_command: str | None = None
    circuit_state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    daily_token_budget: int = DEFAULT_DAILY_TOKEN_BUDGET
    tokens_used_today: int = 0
    budget_reset_at: str = field(default_factory=_today)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["repo_path"] = str(self.repo_path)
        d["worktree_root"] = str(self.worktree_root)
        d["circuit_state"] = self.circuit_state.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Project":
        return cls(
            id=d["id"],
            name=d["name"],
            repo_path=Path(d["repo_path"]),
            base_branch=d.get("base_branch", "main"),
            worktree_root=Path(d.get("worktree_root", str(Path.cwd() / ".worktrees"))),
            auto_merge=bool(d.get("auto_merge", True)),
            verify_command=d.get("verify_command"),
            circuit_state=CircuitState(d.get("circuit_state", CircuitState.CLOSED.value)),
            consecutive_failures=int(d.get("consecutive_failures", 0)),
            daily_token_budget=int(d.get("daily_token_budget", DEFAULT_DAILY_TOKEN_BUDGET)),
            tokens_used_today=int(d.get("tokens_used_today", 0)),
            budget_reset_at=str(d.get("budget_reset_at", _today())),
        )


class ProjectRegistry:
    """Persistent map of project_id → Project.

    Used by the orchestrator (per-project worktree managers + budgets) and
    the autonomy controller (circuit breaker state).
    """

    def __init__(self, path: Path | None = None) -> None:
        self.path = (path or Path.home() / ".overseer" / "projects.json").expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._projects: dict[str, Project] = {}
        # We use a threading lock instead of an asyncio one so this is safely
        # callable from sync code (tests, lifespan setup) too.
        self._lock = threading.RLock()
        self._load()

    # ───────────────── persistence ─────────────────

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            return
        for item in raw.get("projects", []):
            try:
                p = Project.from_dict(item)
                self._projects[p.id] = p
            except (KeyError, ValueError):
                continue

    def _save(self) -> None:
        tmp = self.path.with_suffix(".json.tmp")
        payload = {"projects": [p.to_dict() for p in self._projects.values()]}
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        os.replace(tmp, self.path)

    # ───────────────── CRUD ─────────────────

    def register(self, project: Project) -> Project:
        with self._lock:
            self._projects[project.id] = project
            self._save()
        return project

    def get(self, project_id: str) -> Project | None:
        with self._lock:
            return self._projects.get(project_id)

    def list_all(self) -> list[Project]:
        with self._lock:
            return list(self._projects.values())

    # ───────────────── circuit breaker ─────────────────

    def update_circuit(self, project_id: str, success: bool) -> Project | None:
        """Record a merge outcome and update the breaker.

        - Success in any state resets the failure count and closes the breaker.
        - Failure in CLOSED increments; ≥ threshold flips to OPEN.
        - Failure in HALF_OPEN re-opens immediately.
        - Failure in OPEN keeps it open (count keeps growing for visibility).
        """
        with self._lock:
            p = self._projects.get(project_id)
            if not p:
                return None
            if success:
                p.consecutive_failures = 0
                p.circuit_state = CircuitState.CLOSED
            else:
                p.consecutive_failures += 1
                if p.circuit_state == CircuitState.HALF_OPEN:
                    p.circuit_state = CircuitState.OPEN
                elif (
                    p.circuit_state == CircuitState.CLOSED
                    and p.consecutive_failures >= CIRCUIT_OPEN_THRESHOLD
                ):
                    p.circuit_state = CircuitState.OPEN
            self._save()
            return p

    def reset_circuit(self, project_id: str) -> Project | None:
        with self._lock:
            p = self._projects.get(project_id)
            if not p:
                return None
            p.consecutive_failures = 0
            p.circuit_state = CircuitState.CLOSED
            self._save()
            return p

    # ───────────────── budgets ─────────────────

    def _maybe_reset_budget(self, p: Project) -> None:
        today = _today()
        if p.budget_reset_at != today:
            p.tokens_used_today = 0
            p.budget_reset_at = today

    def record_token_use(self, project_id: str, n: int) -> Project | None:
        with self._lock:
            p = self._projects.get(project_id)
            if not p:
                return None
            self._maybe_reset_budget(p)
            p.tokens_used_today += max(0, int(n))
            self._save()
            return p

    def is_within_budget(self, project_id: str) -> bool:
        with self._lock:
            p = self._projects.get(project_id)
            if not p:
                return False
            self._maybe_reset_budget(p)
            # Persist the rollover if it just happened
            self._save()
            return p.tokens_used_today < p.daily_token_budget


# A module-level helper that callers can use to grab a default singleton.
# Tests should construct their own ProjectRegistry(tmp_path / "projects.json").
_default: ProjectRegistry | None = None
_default_lock = asyncio.Lock()


async def get_default_registry() -> ProjectRegistry:
    global _default
    async with _default_lock:
        if _default is None:
            _default = ProjectRegistry()
        return _default
