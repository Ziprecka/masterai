"""Tests for the project registry, circuit breaker, and budget tracking."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from overseer.events import CircuitState
from overseer.projects import (
    CIRCUIT_OPEN_THRESHOLD,
    DEFAULT_DAILY_TOKEN_BUDGET,
    Project,
    ProjectRegistry,
)


def _make(reg: ProjectRegistry, repo: Path, pid: str = "p1") -> Project:
    return reg.register(Project(
        id=pid,
        name=pid,
        repo_path=repo,
        worktree_root=repo / ".worktrees",
    ))


def test_persistence_roundtrip(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    reg1 = ProjectRegistry(tmp_path / "projects.json")
    p = _make(reg1, repo)
    p.daily_token_budget = 999
    reg1.register(p)

    reg2 = ProjectRegistry(tmp_path / "projects.json")
    loaded = reg2.get("p1")
    assert loaded is not None
    assert loaded.repo_path == repo
    assert loaded.daily_token_budget == 999
    assert loaded.circuit_state == CircuitState.CLOSED


def test_circuit_opens_after_threshold(project_registry: ProjectRegistry, tmp_path: Path) -> None:
    p = _make(project_registry, tmp_path)
    # First failure does not yet trip.
    project_registry.update_circuit(p.id, success=False)
    assert project_registry.get(p.id).circuit_state == CircuitState.CLOSED

    # Second failure flips OPEN.
    project_registry.update_circuit(p.id, success=False)
    after = project_registry.get(p.id)
    assert CIRCUIT_OPEN_THRESHOLD == 2
    assert after.circuit_state == CircuitState.OPEN
    assert after.consecutive_failures == 2

    # Reset closes it again.
    project_registry.reset_circuit(p.id)
    assert project_registry.get(p.id).circuit_state == CircuitState.CLOSED
    assert project_registry.get(p.id).consecutive_failures == 0


def test_circuit_success_resets(project_registry: ProjectRegistry, tmp_path: Path) -> None:
    p = _make(project_registry, tmp_path)
    project_registry.update_circuit(p.id, success=False)
    project_registry.update_circuit(p.id, success=True)
    after = project_registry.get(p.id)
    assert after.consecutive_failures == 0
    assert after.circuit_state == CircuitState.CLOSED


def test_budget_tracks_and_blocks(project_registry: ProjectRegistry, tmp_path: Path) -> None:
    p = _make(project_registry, tmp_path)
    assert project_registry.is_within_budget(p.id) is True

    project_registry.record_token_use(p.id, DEFAULT_DAILY_TOKEN_BUDGET // 2)
    assert project_registry.is_within_budget(p.id) is True

    project_registry.record_token_use(p.id, DEFAULT_DAILY_TOKEN_BUDGET)
    assert project_registry.is_within_budget(p.id) is False


def test_budget_resets_when_date_rolls_over(
    project_registry: ProjectRegistry, tmp_path: Path
) -> None:
    p = _make(project_registry, tmp_path)
    project_registry.record_token_use(p.id, p.daily_token_budget + 1)
    assert project_registry.is_within_budget(p.id) is False

    # Pretend the recorded reset date was yesterday.
    p_stored = project_registry.get(p.id)
    p_stored.budget_reset_at = (date.today() - timedelta(days=1)).isoformat()
    project_registry.register(p_stored)

    # Touching the budget should reset it for today.
    assert project_registry.is_within_budget(p.id) is True
    assert project_registry.get(p.id).tokens_used_today == 0
    assert project_registry.get(p.id).budget_reset_at == date.today().isoformat()


def test_unknown_project_returns_none(project_registry: ProjectRegistry) -> None:
    assert project_registry.get("nope") is None
    assert project_registry.update_circuit("nope", success=True) is None
    assert project_registry.record_token_use("nope", 100) is None
    assert project_registry.is_within_budget("nope") is False
