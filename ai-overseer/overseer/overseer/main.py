"""FastAPI entrypoint.

Exposes:

  POST /api/goals          → plan + spawn agents for a goal
  GET  /api/agents         → list current agents and their state
  POST /api/agents/{id}/kill
  POST /api/agents/{id}/merge
  POST /api/agents/{id}/discard
  WS   /ws                 → live event stream for the dashboard
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .bus import bus
from .orchestrator import Orchestrator


def _env_path(key: str, *, required: bool = True) -> Path | None:
    v = os.environ.get(key)
    if not v:
        if required:
            raise RuntimeError(f"{key} env var must be set")
        return None
    return Path(v).expanduser().resolve()


orchestrator: Orchestrator | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator
    repo = _env_path("TARGET_REPO")
    worktrees = _env_path("WORKTREE_ROOT", required=False) or (Path.cwd() / ".worktrees")
    base = os.environ.get("BASE_BRANCH", "main")
    assert repo is not None
    orchestrator = Orchestrator(repo, worktrees, base_branch=base)
    yield


app = FastAPI(lifespan=lifespan, title="AI Overseer")

# Permissive CORS for local dev; tighten in production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class GoalRequest(BaseModel):
    goal: str
    auto_spawn: bool = True  # if False, just returns the plan for user approval


class GoalResponse(BaseModel):
    tasks: list[dict[str, Any]]
    spawned: list[str]


@app.post("/api/goals", response_model=GoalResponse)
async def submit_goal(req: GoalRequest) -> GoalResponse:
    assert orchestrator is not None
    tasks = await orchestrator.plan(req.goal)
    spawned: list[str] = []
    if req.auto_spawn:
        for t in tasks:
            agent_id = await orchestrator.spawn_for_task(t)
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
