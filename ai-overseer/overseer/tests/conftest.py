"""Shared fixtures and pytest config.

We use ``asyncio_mode = "auto"`` so tests can be plain ``async def`` without
having to decorate each one. The fresh-bus fixture is autouse so a stray
publish from one test never bleeds into another.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import overseer.bus as bus_mod


@pytest.fixture(autouse=True)
def fresh_bus():
    """Reset the in-process bus between tests.

    Other modules import ``from .bus import bus`` at module load time, so
    we mutate the singleton's internal state rather than swap the binding.
    """
    bus_mod.bus._history = []
    bus_mod.bus._subscribers = set()
    yield bus_mod.bus
    bus_mod.bus._history = []
    bus_mod.bus._subscribers = set()


@pytest.fixture
def project_registry(tmp_path: Path):
    from overseer.projects import ProjectRegistry
    return ProjectRegistry(tmp_path / "projects.json")


@pytest.fixture
def watcher_registry(tmp_path: Path):
    from overseer.watchers import WatcherRegistry
    return WatcherRegistry(tmp_path / "watchers.json")


@pytest.fixture
def identity(tmp_path: Path):
    from overseer.identity import load_identity
    return load_identity(tmp_path / "config")
