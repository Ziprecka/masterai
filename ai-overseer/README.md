# AI Overseer

A master AI orchestration system that spawns and monitors multiple Claude Code agents working in parallel on a project. Includes an interactive web dashboard for live observability and control.

## What it does

- **Plans** — Breaks user goals into bounded tasks
- **Spawns** — Launches Claude Agent SDK sessions in isolated git worktrees, one per task
- **Monitors** — Streams every tool call, file edit, and message to a live dashboard via hooks
- **Reviews** — Inspects diffs before merging, runs tests, can re-spawn agents on failure
- **Controls** — Lets you approve, pause, or kill any agent from the dashboard

## Architecture

```
Web Dashboard (Next.js)  ──WebSocket──▶  Overseer (FastAPI + asyncio)
                                              │
                                              │ claude_agent_sdk
                                              ▼
                              ┌───────┬───────┬───────┐
                              ▼       ▼       ▼       ▼
                          Agent 1  Agent 2  Agent 3  Agent N
                          (each in its own git worktree)
```

## Prerequisites

- Python 3.11+
- Node.js 20+
- Git 2.40+ (for worktrees)
- `ANTHROPIC_API_KEY` set in environment, OR an authenticated `claude` CLI session

## Quick start

```bash
# 1. Install
pip install -e ./overseer
cd dashboard && npm install && cd ..

# 2. Initialize a target repo (the project the agents will work on)
export TARGET_REPO=/path/to/your/repo

# 3. Run the overseer
python -m overseer.main

# 4. Run the dashboard (separate terminal)
cd dashboard && npm run dev
# Open http://localhost:3003
```

## Layout

```
overseer/         Python orchestration service
  main.py         FastAPI + WebSocket entrypoint
  orchestrator.py Task planning and agent lifecycle
  agent.py        Wrapper around claude_agent_sdk.query()
  worktree.py     Git worktree management
  hooks.py        SDK hooks that emit events to the dashboard
  events.py       Event bus and pydantic schemas
  store.py        In-memory state (swap for SQLite/Redis later)

dashboard/        Next.js web UI
  app/            App router pages
  components/     AgentCard, LogStream, DiffViewer, KanbanBoard

scripts/
  spawn-test.py   Smoke test that spawns a single agent
  cleanup.sh      Remove all worktrees

docs/
  ARCHITECTURE.md Deep-dive on design decisions
  SAFETY.md       Permissions, allowlists, kill switches
```

## Safety

Agents run with `permission_mode="acceptEdits"` inside worktrees only. They cannot:
- Push to remote branches
- Modify the main working tree
- Run arbitrary bash without `PreToolUse` hook approval (configurable)

The overseer is the only process that merges agent work back to main.

## License

MIT
