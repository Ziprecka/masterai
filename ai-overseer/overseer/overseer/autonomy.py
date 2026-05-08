"""Autonomy controller — turn `agent_finished` into `merged` / retry / fail.

Subscribes to the bus. On each `AgentFinished`:

1. Look up the project. Bail out if the breaker is OPEN, auto_merge is off,
   or the project is over budget — work just sits in `reviewing` waiting
   for a human.
2. Run the project's `verify_command` if set, emitting
   `VerificationStarted` / `VerificationFinished` around it.
3. On green: merge via the orchestrator, mark the breaker as healthy.
4. On red: retry up to `MAX_RETRIES` with the test failure appended to the
   prompt. After exhaustion, mark failed and increment the breaker.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .bus import bus
from .events import (
    AgentFinished,
    AgentStatus,
    AgentStatusChanged,
    CircuitState,
    CircuitStateChanged,
    Event,
    RetryScheduled,
    VerificationFinished,
    VerificationStarted,
)
from .orchestrator import Task

if TYPE_CHECKING:
    from .orchestrator import Orchestrator
    from .projects import ProjectRegistry


MAX_RETRIES = 2  # so each task makes at most 3 total attempts


@dataclass
class _RetryState:
    attempts: int = 1  # 1 = original spawn


class AutonomyController:
    def __init__(
        self,
        orchestrator: "Orchestrator",
        registry: "ProjectRegistry",
        *,
        bus_subscribe: AsyncIterator[Event] | None = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.registry = registry
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()
        # Retry state keyed by task_id (so a retried agent inherits the count).
        self._retries: dict[str, _RetryState] = {}
        self._bus_iter = bus_subscribe

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run(self) -> None:
        iterator = self._bus_iter or bus.subscribe()
        async for event in iterator:
            if self._stopped.is_set():
                return
            if isinstance(event, AgentFinished):
                try:
                    await self.handle_finished(event)
                except Exception:
                    # An exception here would dead-end the loop; swallow and
                    # keep listening so the next agent can still be reviewed.
                    continue

    # Exposed for tests.
    async def handle_finished(self, event: AgentFinished) -> None:
        tracked = self.orchestrator.agents.get(event.agent_id)
        if tracked is None:
            return
        project = self.registry.get(tracked.project_id)
        if project is None:
            return

        # Gate: auto-merge disabled, breaker open, or budget blown means we
        # leave the agent in `reviewing` for the human.
        if not project.auto_merge:
            return
        if project.circuit_state == CircuitState.OPEN:
            return
        if not self.registry.is_within_budget(project.id):
            return

        # Verification (if configured)
        passed = True
        verify_output = ""
        if project.verify_command:
            await bus.publish(VerificationStarted(
                agent_id=event.agent_id,
                command=project.verify_command,
                project_id=project.id,
            ))
            passed, verify_output = await self._run_verify(
                project.verify_command, tracked.agent.worktree.path
            )
            await bus.publish(VerificationFinished(
                agent_id=event.agent_id,
                project_id=project.id,
                passed=passed,
                output_preview=verify_output[-2000:],
            ))

        if passed:
            await self._merge(event.agent_id)
            return

        await self._retry_or_fail(event.agent_id, verify_output)

    # ───────────────────────── helpers ─────────────────────────

    async def _run_verify(self, command: str, cwd) -> tuple[bool, str]:
        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await proc.communicate()
            return proc.returncode == 0, out.decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001
            return False, f"verify_command failed to start: {exc}"

    async def _merge(self, agent_id: str) -> None:
        tracked = self.orchestrator.agents.get(agent_id)
        if tracked is None:
            return
        try:
            await self.orchestrator.merge(agent_id)
        except Exception as exc:  # noqa: BLE001
            # Treat a merge failure as a circuit failure so two in a row
            # trips the breaker open.
            await self._on_circuit_failure(tracked.project_id, str(exc))
            return
        # Successful merge clears the breaker.
        before = self.registry.get(tracked.project_id)
        prev_state = before.circuit_state if before else CircuitState.CLOSED
        prev_failures = before.consecutive_failures if before else 0
        updated = self.registry.update_circuit(tracked.project_id, success=True)
        if updated and (
            updated.circuit_state != prev_state
            or updated.consecutive_failures != prev_failures
        ):
            await bus.publish(CircuitStateChanged(
                project_id=tracked.project_id,
                state=updated.circuit_state,
                consecutive_failures=updated.consecutive_failures,
            ))

    async def _retry_or_fail(self, agent_id: str, verify_output: str) -> None:
        tracked = self.orchestrator.agents.get(agent_id)
        if tracked is None:
            return
        task_id = tracked.task.id
        state = self._retries.setdefault(task_id, _RetryState())

        if state.attempts > MAX_RETRIES:
            # Already exhausted (defensive — shouldn't normally hit).
            await self._fail(agent_id, "retries exhausted")
            return

        if state.attempts >= 1 + MAX_RETRIES:
            await self._fail(agent_id, "retries exhausted")
            return

        # We have at least one retry left.
        state.attempts += 1
        attempt = state.attempts
        await bus.publish(RetryScheduled(
            agent_id=agent_id,
            original_agent_id=agent_id,
            attempt=attempt,
            reason="verify failed",
        ))

        amended = Task(
            id=task_id,
            title=tracked.task.title,
            description=(
                f"{tracked.task.description}\n\n"
                f"Previous attempt {attempt - 1} failed verification. "
                f"Test output (last 4000 chars):\n{verify_output[-4000:]}"
            ),
            project_id=tracked.project_id,
            role=tracked.task.role,
            depends_on=list(tracked.task.depends_on),
        )
        # Discard the old worktree so the retry starts from base.
        try:
            await self.orchestrator.discard(agent_id)
        except Exception:
            pass
        new_id = await self.orchestrator.spawn_for_task(amended)
        if new_id is None:
            # Budget blocked the retry; treat as failure.
            await self._fail(agent_id, "budget exhausted before retry")

    async def _fail(self, agent_id: str, detail: str) -> None:
        tracked = self.orchestrator.agents.get(agent_id)
        if tracked is None:
            return
        await bus.publish(AgentStatusChanged(
            agent_id=agent_id,
            status=AgentStatus.FAILED,
            detail=detail,
        ))
        await self._on_circuit_failure(tracked.project_id, detail)

    async def _on_circuit_failure(self, project_id: str, detail: str) -> None:
        before = self.registry.get(project_id)
        prev_state = before.circuit_state if before else CircuitState.CLOSED
        prev_failures = before.consecutive_failures if before else 0
        updated = self.registry.update_circuit(project_id, success=False)
        if updated and (
            updated.circuit_state != prev_state
            or updated.consecutive_failures != prev_failures
        ):
            await bus.publish(CircuitStateChanged(
                project_id=project_id,
                state=updated.circuit_state,
                consecutive_failures=updated.consecutive_failures,
            ))
