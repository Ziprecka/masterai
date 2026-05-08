"""FastAPI entrypoint.

Exposes the multi-project / autonomy / watcher / conversation API. The
single-target ``TARGET_REPO`` env var is still honoured: if present (and the
project registry is empty) the server bootstraps a default project from it
so existing dev flows keep working.
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .autonomy import AutonomyController
from .bus import bus
from .conversation import ConversationManager, make_handlers
from .events import (
    CircuitStateChanged,
    ProjectRegistered,
    WatcherMode,
    WatcherTrigger,
)
from .identity import load_identity
from .memory import MemoryStore
from .orchestrator import Orchestrator
from .projects import DEFAULT_DAILY_TOKEN_BUDGET, Project, ProjectRegistry
from .watchers import (
    WatcherEngine,
    WatcherRegistry,
    emit_registered,
    make_watcher,
)


# Module-level singletons populated in the lifespan. Endpoints assert
# non-None rather than wrapping every access in Optional checks.
orchestrator: Orchestrator | None = None
registry: ProjectRegistry | None = None
watcher_registry: WatcherRegistry | None = None
watcher_engine: WatcherEngine | None = None
memory_store: MemoryStore | None = None
autonomy: AutonomyController | None = None
conversation_manager: ConversationManager | None = None
_memory_subscriber_task: asyncio.Task | None = None


def _bootstrap_default_project(reg: ProjectRegistry) -> None:
    """If TARGET_REPO is set and the registry is empty, register it.

    Keeps the README's quickstart path working without needing the user to
    POST a project on first run.
    """
    if reg.list_all():
        return
    repo = os.environ.get("TARGET_REPO")
    if not repo:
        return
    repo_path = Path(repo).expanduser().resolve()
    worktree_root = Path(
        os.environ.get("WORKTREE_ROOT", str(Path.cwd() / ".worktrees"))
    ).expanduser().resolve()
    base = os.environ.get("BASE_BRANCH", "main")
    p = Project(
        id="default",
        name=repo_path.name or "default",
        repo_path=repo_path,
        base_branch=base,
        worktree_root=worktree_root,
    )
    reg.register(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator, registry, watcher_registry, watcher_engine
    global memory_store, autonomy, conversation_manager, _memory_subscriber_task

    config_dir = Path(os.environ.get("OVERSEER_HOME", str(Path.home() / ".overseer")))
    config_dir.mkdir(parents=True, exist_ok=True)

    identity = load_identity(config_dir)
    registry = ProjectRegistry(config_dir / "projects.json")
    _bootstrap_default_project(registry)

    watcher_registry = WatcherRegistry(config_dir / "watchers.json")
    memory_store = MemoryStore(config_dir / "memory.db")
    orchestrator = Orchestrator(registry, identity=identity)
    watcher_engine = WatcherEngine(watcher_registry, registry, orchestrator)
    autonomy = AutonomyController(orchestrator, registry)
    conversation_manager = ConversationManager(
        identity=identity,
        handlers=make_handlers(
            orchestrator=orchestrator,
            projects=registry,
            watchers=watcher_registry,
            watcher_engine=watcher_engine,
            memory=memory_store,
        ),
    )

    _memory_subscriber_task = asyncio.create_task(memory_store.subscribe_bus())
    await autonomy.start()
    await watcher_engine.start()

    try:
        yield
    finally:
        if watcher_engine:
            await watcher_engine.stop()
        if autonomy:
            await autonomy.stop()
        if _memory_subscriber_task:
            _memory_subscriber_task.cancel()
            try:
                await _memory_subscriber_task
            except (asyncio.CancelledError, Exception):
                pass
        if memory_store:
            memory_store.close()


app = FastAPI(lifespan=lifespan, title="AI Overseer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ───────────────────────── goals & agents ─────────────────────────


class GoalRequest(BaseModel):
    goal: str
    project_ids: list[str] | None = None
    auto_spawn: bool = True


class GoalResponse(BaseModel):
    tasks: list[dict[str, Any]]
    spawned: list[str]


@app.post("/api/goals", response_model=GoalResponse)
async def submit_goal(req: GoalRequest) -> GoalResponse:
    assert orchestrator is not None and registry is not None
    pids = req.project_ids or [p.id for p in registry.list_all()]
    if not pids:
        raise HTTPException(400, "no projects registered; POST /api/projects first")
    tasks = await orchestrator.plan(req.goal, pids)
    spawned: list[str] = []
    if req.auto_spawn:
        for t in tasks:
            agent_id = await orchestrator.spawn_for_task(t)
            if agent_id:
                spawned.append(agent_id)
    return GoalResponse(
        tasks=[t.__dict__ for t in tasks],
        spawned=spawned,
    )


@app.get("/api/agents")
async def list_agents() -> list[dict[str, Any]]:
    assert orchestrator is not None
    return [
        {
            "agent_id": aid,
            "task": tracked.task.__dict__,
            "branch": tracked.agent.worktree.branch,
            "worktree_path": str(tracked.agent.worktree.path),
            "project_id": tracked.project_id,
        }
        for aid, tracked in orchestrator.agents.items()
    ]


@app.post("/api/agents/{agent_id}/kill")
async def kill_agent(agent_id: str) -> dict[str, str]:
    assert orchestrator is not None
    if agent_id not in orchestrator.agents:
        raise HTTPException(404)
    await orchestrator.kill(agent_id)
    return {"status": "killed"}


@app.post("/api/agents/{agent_id}/merge")
async def merge_agent(agent_id: str) -> dict[str, str]:
    assert orchestrator is not None
    if agent_id not in orchestrator.agents:
        raise HTTPException(404)
    await orchestrator.merge(agent_id)
    return {"status": "merged"}


@app.post("/api/agents/{agent_id}/discard")
async def discard_agent(agent_id: str) -> dict[str, str]:
    assert orchestrator is not None
    if agent_id not in orchestrator.agents:
        raise HTTPException(404)
    await orchestrator.discard(agent_id)
    return {"status": "discarded"}


# ───────────────────────── projects ─────────────────────────


class ProjectRequest(BaseModel):
    id: str
    name: str
    repo_path: str
    base_branch: str = "main"
    worktree_root: str | None = None
    auto_merge: bool = True
    verify_command: str | None = None
    daily_token_budget: int = DEFAULT_DAILY_TOKEN_BUDGET


@app.post("/api/projects")
async def register_project(req: ProjectRequest) -> dict[str, Any]:
    assert registry is not None
    repo_path = Path(req.repo_path).expanduser().resolve()
    worktree_root = (
        Path(req.worktree_root).expanduser().resolve()
        if req.worktree_root
        else Path.cwd() / ".worktrees"
    )
    p = Project(
        id=req.id,
        name=req.name,
        repo_path=repo_path,
        base_branch=req.base_branch,
        worktree_root=worktree_root,
        auto_merge=req.auto_merge,
        verify_command=req.verify_command,
        daily_token_budget=req.daily_token_budget,
    )
    registry.register(p)
    await bus.publish(ProjectRegistered(
        project_id=p.id,
        name=p.name,
        repo_path=str(p.repo_path),
        base_branch=p.base_branch,
    ))
    return p.to_dict()


@app.get("/api/projects")
async def list_projects() -> list[dict[str, Any]]:
    assert registry is not None
    return [p.to_dict() for p in registry.list_all()]


@app.post("/api/projects/{project_id}/circuit/reset")
async def reset_circuit(project_id: str) -> dict[str, Any]:
    assert registry is not None
    p = registry.reset_circuit(project_id)
    if p is None:
        raise HTTPException(404)
    await bus.publish(CircuitStateChanged(
        project_id=p.id,
        state=p.circuit_state,
        consecutive_failures=p.consecutive_failures,
    ))
    return p.to_dict()


# ───────────────────────── watchers ─────────────────────────


class WatcherRequest(BaseModel):
    name: str
    project_id: str
    trigger: WatcherTrigger
    trigger_config: dict[str, Any]
    goal_template: str
    mode: WatcherMode = WatcherMode.ALERT_ONLY
    cooldown_seconds: int = 300
    enabled: bool = True


@app.post("/api/watchers")
async def create_watcher(req: WatcherRequest) -> dict[str, Any]:
    assert watcher_registry is not None and watcher_engine is not None
    w = make_watcher(
        name=req.name,
        project_id=req.project_id,
        trigger=req.trigger,
        trigger_config=req.trigger_config,
        goal_template=req.goal_template,
        mode=req.mode,
        cooldown_seconds=req.cooldown_seconds,
        enabled=req.enabled,
    )
    watcher_registry.register(w)
    await bus.publish(emit_registered(w))
    if w.trigger == WatcherTrigger.FILE_CHANGE:
        watcher_engine._ensure_file_task(w)  # noqa: SLF001
    return w.to_dict()


@app.get("/api/watchers")
async def list_watchers() -> list[dict[str, Any]]:
    assert watcher_registry is not None
    return [w.to_dict() for w in watcher_registry.list_all()]


@app.post("/api/watchers/{watcher_id}/toggle")
async def toggle_watcher(watcher_id: str) -> dict[str, Any]:
    assert watcher_registry is not None
    w = watcher_registry.toggle(watcher_id)
    if w is None:
        raise HTTPException(404)
    return w.to_dict()


@app.delete("/api/watchers/{watcher_id}")
async def delete_watcher(watcher_id: str) -> dict[str, str]:
    assert watcher_registry is not None
    if not watcher_registry.delete(watcher_id):
        raise HTTPException(404)
    return {"status": "deleted"}


# ───────────────────────── conversation ─────────────────────────


class ConversationRequest(BaseModel):
    text: str


@app.post("/api/conversation/turn")
async def conversation_turn(req: ConversationRequest) -> StreamingResponse:
    assert conversation_manager is not None
    cm = conversation_manager

    async def stream():
        try:
            async for chunk in cm.turn(req.text):
                # SSE framing: each chunk is a single ``data:`` line. The
                # dashboard already speaks SSE for streaming logs.
                yield f"data: {json.dumps({'text': chunk})}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as exc:  # noqa: BLE001
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


# ───────────────────────── websocket ─────────────────────────


@app.websocket("/ws")
async def ws_events(ws: WebSocket) -> None:
    await ws.accept()
    try:
        async for event in bus.subscribe():
            await ws.send_json(event.model_dump())
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await ws.close()
        except Exception:
            pass


def main() -> None:
    import uvicorn
    uvicorn.run(
        "overseer.main:app",
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8000")),
        reload=bool(os.environ.get("RELOAD")),
    )


if __name__ == "__main__":
    main()
