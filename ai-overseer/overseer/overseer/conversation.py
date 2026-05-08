"""Text conversation handler.

The user types into the dashboard's chat box; that input becomes a turn
here. The conversation agent has the overseer's identity prefix as system
prompt and a small MCP toolset that lets it act on the orchestrator,
project registry, and watcher registry — but it never edits code itself.

Code edits go through ``dispatch_goal``, which hands work to the
orchestrator's planner and returns the spawned agent IDs.

A single in-process session is kept for now (single-user assumption).
Tests inject ``query_fn`` and ``handlers`` to bypass the SDK entirely.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .bus import bus
from .events import ConversationTurn

if TYPE_CHECKING:
    from .identity import Identity
    from .memory import MemoryStore
    from .orchestrator import Orchestrator
    from .projects import ProjectRegistry
    from .watchers import WatcherEngine, WatcherRegistry


@dataclass
class Message:
    role: str  # "user" | "assistant"
    text: str


@dataclass
class ConversationHandlers:
    """The set of actions the conversation agent can take.

    Kept as a separate dataclass (rather than calling the orchestrator
    directly) so tests can stub each handler in isolation, and so a future
    GraphQL/REST surface can reuse the same dispatch table.
    """
    dispatch_goal: Callable[[str, list[str]], Awaitable[list[str]]]
    list_projects: Callable[[], Awaitable[list[dict[str, Any]]]]
    list_active_agents: Callable[[], Awaitable[list[dict[str, Any]]]]
    kill_agent: Callable[[str], Awaitable[bool]]
    register_watcher: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
    query_memory: Callable[[str], Awaitable[list[str]]]


def make_handlers(
    *,
    orchestrator: "Orchestrator",
    projects: "ProjectRegistry",
    watchers: "WatcherRegistry",
    watcher_engine: "WatcherEngine | None",
    memory: "MemoryStore",
) -> ConversationHandlers:
    from .watchers import WatcherMode, WatcherTrigger, emit_registered, make_watcher

    async def dispatch_goal(goal: str, project_ids: list[str]) -> list[str]:
        return await orchestrator.plan_and_spawn(goal, project_ids)

    async def list_projects() -> list[dict[str, Any]]:
        return [
            {
                "id": p.id,
                "name": p.name,
                "repo_path": str(p.repo_path),
                "base_branch": p.base_branch,
                "auto_merge": p.auto_merge,
                "circuit_state": p.circuit_state.value,
                "tokens_used_today": p.tokens_used_today,
                "daily_token_budget": p.daily_token_budget,
            }
            for p in projects.list_all()
        ]

    async def list_active_agents() -> list[dict[str, Any]]:
        return [
            {
                "agent_id": aid,
                "task_title": tracked.task.title,
                "project_id": tracked.project_id,
                "branch": tracked.agent.worktree.branch,
            }
            for aid, tracked in orchestrator.agents.items()
        ]

    async def kill_agent(agent_id: str) -> bool:
        if agent_id not in orchestrator.agents:
            return False
        await orchestrator.kill(agent_id)
        return True

    async def register_watcher(spec: dict[str, Any]) -> dict[str, Any]:
        w = make_watcher(
            name=str(spec["name"]),
            project_id=str(spec["project_id"]),
            trigger=WatcherTrigger(spec["trigger"]),
            trigger_config=dict(spec.get("trigger_config", {})),
            goal_template=str(spec["goal_template"]),
            mode=WatcherMode(spec.get("mode", WatcherMode.ALERT_ONLY.value)),
            cooldown_seconds=int(spec.get("cooldown_seconds", 300)),
            enabled=bool(spec.get("enabled", True)),
        )
        watchers.register(w)
        await bus.publish(emit_registered(w))
        if watcher_engine is not None:
            # Bring up file watching immediately for FILE_CHANGE watchers.
            try:
                watcher_engine._ensure_file_task(w)  # noqa: SLF001
            except Exception:
                pass
        return w.to_dict()

    async def query_memory(text: str) -> list[str]:
        return await memory.recall(text)

    return ConversationHandlers(
        dispatch_goal=dispatch_goal,
        list_projects=list_projects,
        list_active_agents=list_active_agents,
        kill_agent=kill_agent,
        register_watcher=register_watcher,
        query_memory=query_memory,
    )


class ConversationManager:
    def __init__(
        self,
        *,
        identity: "Identity",
        handlers: ConversationHandlers,
        query_fn: Any | None = None,
    ) -> None:
        """Construct a conversation manager.

        ``query_fn``: optional injection point for tests. If left None we
        lazy-import ``claude_agent_sdk.query`` at first turn.
        """
        self.identity = identity
        self.handlers = handlers
        self.messages: list[Message] = []
        self._query_fn = query_fn
        self._lock = asyncio.Lock()

    # ───────────── public ─────────────

    async def turn(self, user_text: str) -> AsyncIterator[str]:
        """Run one turn. Streams the assistant's text out as it arrives."""
        await bus.publish(ConversationTurn(role="user", text=user_text))
        self.messages.append(Message(role="user", text=user_text))

        async for chunk in self._run_turn(user_text):
            yield chunk

    # ───────────── internals ─────────────

    async def _run_turn(self, user_text: str) -> AsyncIterator[str]:
        sdk = self._load_sdk()
        if sdk is None:
            # Offline fallback: synthesize a minimal assistant turn so the
            # endpoint still works for dashboard dev. We do NOT try to call
            # any handlers from here — that would conceal bugs.
            text = (
                "(offline) The Claude Agent SDK isn't installed; "
                "I can't reason over your message right now."
            )
            self.messages.append(Message(role="assistant", text=text))
            await bus.publish(ConversationTurn(role="assistant", text=text))
            yield text
            return

        async for chunk in self._run_sdk_turn(sdk, user_text):
            yield chunk

    def _load_sdk(self) -> dict[str, Any] | None:
        if self._query_fn is not None:
            return {"query": self._query_fn}
        try:
            from claude_agent_sdk import (  # type: ignore[import-not-found]
                AssistantMessage,
                ClaudeAgentOptions,
                ResultMessage,
                TextBlock,
                create_sdk_mcp_server,
                query,
                tool,
            )
        except ImportError:
            return None
        return {
            "AssistantMessage": AssistantMessage,
            "ClaudeAgentOptions": ClaudeAgentOptions,
            "ResultMessage": ResultMessage,
            "TextBlock": TextBlock,
            "create_sdk_mcp_server": create_sdk_mcp_server,
            "query": query,
            "tool": tool,
        }

    async def _run_sdk_turn(
        self, sdk: dict[str, Any], user_text: str
    ) -> AsyncIterator[str]:
        if "ClaudeAgentOptions" in sdk:
            mcp_server = self._build_mcp_server(sdk)
            options = sdk["ClaudeAgentOptions"](
                allowed_tools=self._tool_names(),
                mcp_servers={"overseer": mcp_server} if mcp_server else {},
                permission_mode="acceptEdits",
                system_prompt=self.identity.system_prompt_prefix(),
            )
            stream = sdk["query"](prompt=user_text, options=options)
            AssistantMessage = sdk.get("AssistantMessage")
            TextBlock = sdk.get("TextBlock")
            ResultMessage = sdk.get("ResultMessage")
        else:
            # Test injection path — query_fn is a plain async iterator.
            stream = sdk["query"](prompt=user_text, options=None)
            AssistantMessage = TextBlock = ResultMessage = None

        full = ""
        async for msg in stream:
            text = _extract_text(msg, AssistantMessage, TextBlock, ResultMessage)
            if not text:
                continue
            full += text
            yield text

        self.messages.append(Message(role="assistant", text=full))
        await bus.publish(ConversationTurn(role="assistant", text=full))

    def _tool_names(self) -> list[str]:
        # MCP tools surface to the model as ``mcp__<server>__<tool>``.
        prefix = "mcp__overseer__"
        return [
            f"{prefix}{n}"
            for n in (
                "dispatch_goal",
                "list_projects",
                "list_active_agents",
                "kill_agent",
                "register_watcher",
                "query_memory",
            )
        ]

    def _build_mcp_server(self, sdk: dict[str, Any]) -> Any:
        tool = sdk.get("tool")
        create = sdk.get("create_sdk_mcp_server")
        if tool is None or create is None:
            return None
        h = self.handlers

        @tool(
            "dispatch_goal",
            "Hand a goal to the planner and spawn agents on the named projects.",
            {"goal": str, "project_ids": list},
        )
        async def dispatch_goal(args: dict[str, Any]) -> dict[str, Any]:
            ids = await h.dispatch_goal(args["goal"], list(args.get("project_ids", [])))
            return _text(f"Spawned {len(ids)} agent(s): {', '.join(ids) or '(none)'}")

        @tool(
            "list_projects",
            "List registered projects with circuit and budget state.",
            {},
        )
        async def list_projects(args: dict[str, Any]) -> dict[str, Any]:
            ps = await h.list_projects()
            if not ps:
                return _text("No projects registered.")
            lines = [
                f"- {p['id']} ({p['name']}): {p['repo_path']} "
                f"[circuit={p['circuit_state']} tokens={p['tokens_used_today']}/{p['daily_token_budget']}]"
                for p in ps
            ]
            return _text("\n".join(lines))

        @tool(
            "list_active_agents",
            "List the agents currently being tracked by the orchestrator.",
            {},
        )
        async def list_active_agents(args: dict[str, Any]) -> dict[str, Any]:
            ags = await h.list_active_agents()
            if not ags:
                return _text("No active agents.")
            lines = [
                f"- {a['agent_id']} project={a['project_id']} "
                f"branch={a['branch']} task={a['task_title']!r}"
                for a in ags
            ]
            return _text("\n".join(lines))

        @tool(
            "kill_agent",
            "Cancel a running agent by id.",
            {"agent_id": str},
        )
        async def kill_agent(args: dict[str, Any]) -> dict[str, Any]:
            ok = await h.kill_agent(str(args["agent_id"]))
            return _text("killed" if ok else "no such agent")

        @tool(
            "register_watcher",
            "Register a proactive watcher. Provide a JSON spec with name, "
            "project_id, trigger, trigger_config, goal_template, mode, "
            "cooldown_seconds.",
            {"spec": dict},
        )
        async def register_watcher(args: dict[str, Any]) -> dict[str, Any]:
            w = await h.register_watcher(dict(args["spec"]))
            return _text(f"watcher registered: {w['id']}")

        @tool(
            "query_memory",
            "Search the overseer's notes and task history.",
            {"text": str},
        )
        async def query_memory(args: dict[str, Any]) -> dict[str, Any]:
            hits = await h.query_memory(str(args["text"]))
            return _text("\n".join(hits) if hits else "(no matches)")

        return create(
            "overseer",
            "1.0.0",
            tools=[
                dispatch_goal,
                list_projects,
                list_active_agents,
                kill_agent,
                register_watcher,
                query_memory,
            ],
        )


# ──────────────────────── helpers ────────────────────────


def _text(s: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": s}]}


def _extract_text(
    msg: Any,
    AssistantMessage: Any,
    TextBlock: Any,
    ResultMessage: Any,
) -> str:
    """Pull plain text out of an SDK message regardless of its type.

    Real SDK ``AssistantMessage`` carries ``content`` blocks; ``ResultMessage``
    carries a final ``result`` string. Test stubs may yield bare strings or
    plain ``Message`` dataclasses — handle those too.
    """
    if msg is None:
        return ""
    if isinstance(msg, str):
        return msg
    if AssistantMessage is not None and isinstance(msg, AssistantMessage):
        out = []
        for block in getattr(msg, "content", []) or []:
            if TextBlock is not None and isinstance(block, TextBlock):
                out.append(getattr(block, "text", ""))
            else:
                txt = getattr(block, "text", None)
                if isinstance(txt, str):
                    out.append(txt)
        return "".join(out)
    if ResultMessage is not None and isinstance(msg, ResultMessage):
        return getattr(msg, "result", "") or ""
    # Generic dataclass/dict fallback
    txt = getattr(msg, "text", None)
    if isinstance(txt, str):
        return txt
    role = getattr(msg, "role", None)
    if role == "assistant":
        c = getattr(msg, "content", "")
        return c if isinstance(c, str) else ""
    return ""
