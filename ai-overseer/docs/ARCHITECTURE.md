# Architecture

## Why two layers of "agent"

Two distinct things in this system are both called "agent":

1. **Top-level agents** — independent `claude_agent_sdk.query()` calls the
   overseer launches concurrently, each in its own git worktree. These do the
   actual editing/expanding/improving work. The overseer is the only thing that
   sees them all.

2. **In-session subagents** — a top-level agent can use the Agent tool to spawn
   a `researcher` or `test-writer` subagent inside its own session. This keeps
   the parent agent's context clean during heavy reads.

Subagents cannot spawn subagents. Top-level agents are independent — they don't
share context.

## Why git worktrees, not branches-on-the-same-checkout

Worktrees give every agent a real, separate working directory pointed at its
own branch. That means:

- Agents can run `Bash` (tests, linters, builds) without contention.
- The overseer's main checkout stays untouched while agents work.
- You can `git diff base...branch` from the main repo without ever switching to
  the agent's branch.

## Event flow

```
Agent's SDK loop
   │
   ├─ PreToolUse hook  ──▶ bus.publish(ToolCall) ──▶ WS ──▶ dashboard
   ├─ tool runs
   ├─ PostToolUse hook ──▶ bus.publish(ToolResult, FileChanged?) ──▶ ...
   └─ assistant message ──▶ bus.publish(AgentMessage) ──▶ ...
```

The bus is a fan-out queue. Late-joining dashboard tabs replay the last 100
events so you don't lose context on refresh.

## Failure handling (what to add next)

The current scaffold marks agents `failed` and stops. A real overseer should:

1. Capture the failure reason (parse from `AgentStatusChanged.detail`).
2. Decide between three strategies: retry-as-is, retry-with-amended-prompt, or
   escalate to the human. A small policy class in `orchestrator.py` is the
   right place.
3. Optionally spawn a `reviewer` agent against the failed worktree to diagnose
   what went wrong before the human looks.

## Scaling beyond local dev

- Replace `overseer/bus.py` with Redis pub-sub.
- Replace the in-memory `agents` dict with Postgres + a row per task.
- Run the overseer behind a reverse proxy. WebSockets need sticky sessions if
  you scale horizontally.
- Per-agent token + cost budgets, enforced in `agent.py` by inspecting
  `ResultMessage.usage` after each turn.

## Security model

The overseer trusts the agents to operate within their worktrees but distrusts
their bash commands. The `BASH_DENYLIST` in `hooks.py` is the floor — extend
it for your environment. For higher-stakes deployments, run each agent in a
container or VM and pass the worktree in as a bind mount.
