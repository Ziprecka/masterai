# Repo notes for Claude Code

This is **AI Overseer** — a multi-agent orchestrator built on the Claude Agent SDK.

## How the pieces fit

- `overseer/` is a Python package. The entrypoint is `overseer.main:app` (FastAPI).
- `dashboard/` is a Next.js 15 app talking to the overseer via REST + WebSocket.
- Agents run in **git worktrees** under `$WORKTREE_ROOT` (default `./.worktrees/`).
  Each worktree is a fresh branch off `$BASE_BRANCH` (default `main`).
- Dashboard ↔ overseer event contract lives in `overseer/overseer/events.py`.
  If you add a new event type, update both `events.py` AND `dashboard/lib/store.ts`.

## Conventions

- All async I/O uses `asyncio`. No threads.
- Hooks publish events via the in-process `bus`. They must NEVER block — keep them O(1).
- The orchestrator is the only code that calls `git merge` / `git push`. Agents stay in their worktrees.
- New SDK tools an agent can use: add to `AgentSpec.allowed_tools` default in `agent.py`.
- New sub-subagent specialists: add to `SUBAGENTS` in `agent.py` (read-only or scoped tools).

## Running locally

```bash
# Backend
export TARGET_REPO=$HOME/code/some-project   # the repo agents will edit
export ANTHROPIC_API_KEY=sk-...
cd overseer && pip install -e . && cd ..
python -m overseer.main

# Frontend (separate terminal)
cd dashboard && npm install && npm run dev
```

## Don't

- Don't put long-running work inside hooks. Publish an event and return.
- Don't let agents write outside their worktree. The `cwd` option in `ClaudeAgentOptions` already enforces this; don't override it.
- Don't reuse session IDs across agents — every agent is a fresh `query()`.
