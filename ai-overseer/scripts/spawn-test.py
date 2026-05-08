"""Smoke test: spawn a single agent and watch its events.

Usage:
    export TARGET_REPO=/path/to/some/repo
    export ANTHROPIC_API_KEY=sk-...
    python scripts/spawn-test.py
"""
import asyncio
import os
from pathlib import Path

from overseer.bus import bus
from overseer.orchestrator import Orchestrator, Task


async def main() -> None:
    repo = Path(os.environ["TARGET_REPO"]).expanduser().resolve()
    orch = Orchestrator(repo, repo.parent / ".overseer-worktrees")

    # Subscribe to the bus and print everything
    async def printer() -> None:
        async for ev in bus.subscribe():
            print(f"[{ev.type}] {ev.agent_id}: {ev.model_dump()}")

    asyncio.create_task(printer())

    # Spawn one agent on a tiny task
    task = Task(
        id="smoke-001",
        title="add-readme-line",
        description="Append a single line 'Hello from overseer' to README.md.",
    )
    agent_id = await orch.spawn_for_task(task)
    print(f"Spawned {agent_id}")

    # Wait for it to finish
    tracked = orch.agents[agent_id]
    await tracked.agent.join()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
