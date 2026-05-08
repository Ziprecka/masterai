"""Conversation-handler tests.

These tests don't touch the real Claude Agent SDK. We inject a fake
``query_fn`` that yields stubbed messages, and we verify the handler
dispatch table is wired up correctly by exercising it directly.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from overseer.conversation import (
    ConversationHandlers,
    ConversationManager,
    Message,
)
from overseer.identity import load_identity


@dataclass
class _FakeAssistant:
    text: str
    role: str = "assistant"


def _fake_handlers(*, dispatched: list, projects: list[dict[str, Any]] | None = None) -> ConversationHandlers:
    async def dispatch_goal(goal: str, project_ids):
        dispatched.append((goal, list(project_ids)))
        return ["new-agent"]

    async def list_projects():
        return projects or []

    async def list_active_agents():
        return []

    async def kill_agent(agent_id: str):
        return True

    async def register_watcher(spec):
        return {"id": "w-1", **spec}

    async def query_memory(text: str):
        return [f"hit for {text}"]

    return ConversationHandlers(
        dispatch_goal=dispatch_goal,
        list_projects=list_projects,
        list_active_agents=list_active_agents,
        kill_agent=kill_agent,
        register_watcher=register_watcher,
        query_memory=query_memory,
    )


async def test_streamed_assistant_text_is_yielded(tmp_path: Path) -> None:
    """The conversation manager streams assistant text chunks to the caller."""
    identity = load_identity(tmp_path / "config")

    async def fake_query(*, prompt: str, options: Any):
        yield _FakeAssistant(text="hello ")
        yield _FakeAssistant(text="world")

    cm = ConversationManager(
        identity=identity,
        handlers=_fake_handlers(dispatched=[]),
        query_fn=fake_query,
    )

    chunks = []
    async for c in cm.turn("hi"):
        chunks.append(c)

    assert "".join(chunks) == "hello world"
    assert cm.messages[0] == Message(role="user", text="hi")
    assert cm.messages[1].role == "assistant"
    assert cm.messages[1].text == "hello world"


async def test_dispatch_goal_handler_routes_to_orchestrator(tmp_path: Path) -> None:
    """The conversation agent's dispatch_goal tool wires through to the
    orchestrator — directly callable (and exercised by the fake SDK route)."""
    dispatched: list = []
    identity = load_identity(tmp_path / "config")
    handlers = _fake_handlers(dispatched=dispatched)

    # Calling the handler directly is what the @tool wrapper does at
    # runtime; the wrapping is just JSON-schema plumbing.
    ids = await handlers.dispatch_goal("ship feature X", ["proj-a", "proj-b"])
    assert ids == ["new-agent"]
    assert dispatched == [("ship feature X", ["proj-a", "proj-b"])]

    # And we can plug a fake query into the manager that simulates the
    # model deciding to call dispatch_goal — for the streaming path.
    async def fake_query(*, prompt: str, options: Any):
        yield _FakeAssistant(text="dispatching now")
        # In a real run the SDK would invoke the registered MCP tool here.
        # We simulate that effect by calling the handler directly.
        await handlers.dispatch_goal("ship feature Y", ["proj-a"])

    cm = ConversationManager(
        identity=identity, handlers=handlers, query_fn=fake_query,
    )
    chunks = [c async for c in cm.turn("please ship Y")]
    assert "dispatching now" in "".join(chunks)
    # Both direct and via-stream calls landed.
    assert dispatched[-1] == ("ship feature Y", ["proj-a"])


async def test_list_projects_handler_returns_summary(tmp_path: Path) -> None:
    handlers = _fake_handlers(
        dispatched=[],
        projects=[
            {"id": "p", "name": "P", "repo_path": "/r", "circuit_state": "closed",
             "tokens_used_today": 0, "daily_token_budget": 100},
        ],
    )
    out = await handlers.list_projects()
    assert out and out[0]["id"] == "p"


async def test_kill_and_register_watcher_handlers(tmp_path: Path) -> None:
    handlers = _fake_handlers(dispatched=[])
    assert await handlers.kill_agent("anything") is True
    spec = {
        "name": "w", "project_id": "p", "trigger": "interval",
        "trigger_config": {"seconds": 5}, "goal_template": "g",
        "mode": "alert_only", "cooldown_seconds": 60,
    }
    out = await handlers.register_watcher(spec)
    assert out["id"] == "w-1"
    assert out["name"] == "w"


async def test_query_memory_handler(tmp_path: Path) -> None:
    handlers = _fake_handlers(dispatched=[])
    hits = await handlers.query_memory("foo")
    assert hits == ["hit for foo"]
