"""Git worktree management.

Each agent gets a fresh worktree on a fresh branch off `main`. This is
the key isolation primitive — agents can edit freely without colliding,
and the overseer reviews diffs branch-by-branch before merging.
"""
from __future__ import annotations

import asyncio
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Worktree:
    path: Path
    branch: str
    base_branch: str


async def _run(*args: str, cwd: Path | None = None) -> str:
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"`{' '.join(args)}` failed ({proc.returncode}): {err.decode().strip()}"
        )
    return out.decode().strip()


class WorktreeManager:
    def __init__(self, repo_root: Path, worktree_root: Path, base_branch: str = "main") -> None:
        self.repo_root = repo_root
        self.worktree_root = worktree_root
        self.base_branch = base_branch
        self.worktree_root.mkdir(parents=True, exist_ok=True)

    async def create(self, agent_id: str, task_slug: str) -> Worktree:
        branch = f"agent/{agent_id}-{task_slug}"
        path = self.worktree_root / f"{agent_id}-{task_slug}"
        # Ensure base is up to date before branching off it
        await _run("git", "fetch", "origin", self.base_branch, cwd=self.repo_root)
        await _run(
            "git", "worktree", "add", "-b", branch, str(path), f"origin/{self.base_branch}",
            cwd=self.repo_root,
        )
        return Worktree(path=path, branch=branch, base_branch=self.base_branch)

    async def diff_stat(self, wt: Worktree) -> str:
        return await _run(
            "git", "diff", "--stat", f"{wt.base_branch}...{wt.branch}",
            cwd=self.repo_root,
        )

    async def diff_full(self, wt: Worktree) -> str:
        return await _run(
            "git", "diff", f"{wt.base_branch}...{wt.branch}",
            cwd=self.repo_root,
        )

    async def merge(self, wt: Worktree, message: str) -> None:
        # Squash-merge agent branch into base
        await _run("git", "checkout", wt.base_branch, cwd=self.repo_root)
        await _run("git", "merge", "--squash", wt.branch, cwd=self.repo_root)
        await _run("git", "commit", "-m", message, cwd=self.repo_root)

    async def cleanup(self, wt: Worktree, *, delete_branch: bool = False) -> None:
        try:
            await _run("git", "worktree", "remove", "--force", str(wt.path), cwd=self.repo_root)
        except RuntimeError:
            # Fallback: nuke the directory and prune
            shutil.rmtree(wt.path, ignore_errors=True)
            await _run("git", "worktree", "prune", cwd=self.repo_root)
        if delete_branch:
            try:
                await _run("git", "branch", "-D", wt.branch, cwd=self.repo_root)
            except RuntimeError:
                pass
