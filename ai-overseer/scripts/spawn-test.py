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
from overseer.identity import load_identity
from overseer.orchestrator import Orchestrator, Task
from overseer.projects import Project, ProjectRegistry


async def main() -> None:
    repo = Path(os.environ["TARGET_REPO"]).expanduser().resolve()
    config_dir = Path(os.environ.get("OVERSEER_HOME", str(Path.home() / ".overseer")))
    registry = ProjectRegistry(config_dir / "projects.json")
    if registry.get("smoke") is None:
        registry.register(Project(
            id="smoke",
            name=repo.name,
            repo_path=repo,
            worktree_root=repo.parent / ".overseer-worktrees",
        ))
    identity = load_identity(config_dir)
    orch = Orchestrator(registry, identity=identity)

    async def printer() -> None:
        async for ev in bus.subscribe():
            print(f"[{ev.type}] {ev.agent_id}: {ev.model_dump()}")

    asyncio.create_task(printer())

    task = Task(
        id="smoke-001",
        title="add-readme-line",
        description="Append a single line 'Hello from overseer' to README.md.",
        project_id="smoke",
    )
    agent_id = await orch.spawn_for_task(task)
    print(f"Spawned {agent_id}")

    if agent_id:
        tracked = orch.agents[agent_id]
        await tracked.agent.join()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
